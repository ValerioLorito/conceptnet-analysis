#!/usr/bin/env python3
"""
prepare_graph_inputs.py — single source of truth for the typed subgraph.

input  : conceptnet_science_2hop.csv   relation, subject, object, weight (URIs)
output : typed_nodes.csv               uri, name, pos, node_type, label_source
         typed_edges.csv               relation, subject, object, weight,
                                          edge_class, permitted
         prepare_report.json           funnel statistics

Typing policy
    1. URI carries a POS segment (n|v|a|r|s) -> use it            label_source='uri'
    2. otherwise: label_concepts.py cascade on the term
       dump > api > spaCy > nltk > wordnet > heuristics

Everything untypable is DROPPED here, together with every edge touching it,
so the MySQL and Memgraph loaders ingest identical data by construction.

Requires the same dependencies as label_concepts.py. Reuses its cascade
and its static relation schema by import.
"""

import argparse
import csv
import json
import os
import time
from collections import Counter, defaultdict

from label_concepts import (
    CONCEPTNET_POS_TO_NODE_TYPE, FUNCTION_POS, RELATION_TO_EDGE_CLASSES,
    STOPWORD_CONCEPTS, build_pos_index_from_dump, normalize, resolve_pos,
)

E, A, P = "EntityNode", "ActionEventNode", "PropertyNode"
TYPE_LETTER = {E: "E", A: "A", P: "P"}


def parse_uri(uri):
    """'/c/en/term/pos/sense' -> (lang, term, pos); None if not a concept URI."""
    parts = uri.strip().split("/")
    if len(parts) < 4 or parts[1] != "c" or not parts[3]:
        return None
    return parts[2], parts[3], parts[4] if len(parts) > 4 else ""


def read_edges(path):
    edges = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            rel = (row.get("relation") or "").strip()
            subj = (row.get("subject") or "").strip()
            obj = (row.get("object") or "").strip()
            if not (rel and subj and obj):
                continue
            try:
                w = float(row.get("weight", 1.0))
            except (TypeError, ValueError):
                w = 1.0
            edges.append((rel, subj, obj, w))
    return edges


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input",
                    default="data/preprocessed/conceptnet_science_2hop.csv")
    ap.add_argument("--dump",
                    default="data/original/conceptnet-assertions-5.7.0.csv.gz")
    ap.add_argument("--out-dir", default="data/preprocessed")
    ap.add_argument("--no-spacy", action="store_true")
    ap.add_argument("--no-nltk", action="store_true")
    ap.add_argument("--no-wordnet", action="store_true")
    ap.add_argument("--no-heuristics", action="store_true")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    nodes_path = os.path.join(args.out_dir, "typed_nodes.csv")
    edges_path = os.path.join(args.out_dir, "typed_edges.csv")
    report_path = os.path.join(args.out_dir, "prepare_report.json")

    print(f"Reading {args.input} ...")
    edges = read_edges(args.input)
    print(f"  {len(edges):,} edges")

    # --- pass 1: type every endpoint; URI POS wins when present ----------
    funnel = Counter()
    info = {}        # uri -> dict(name, pos, node_type, label_source) | None
    untagged = {}    # uri -> term
    for _r, s, o, _w in edges:
        for uri in (s, o):
            if uri in info:
                continue
            parsed = parse_uri(uri)
            if parsed is None:
                funnel["uri_malformed"] += 1
                info[uri] = None
                continue
            lang, term, pos = parsed
            if lang != "en":
                funnel["non_english"] += 1
                info[uri] = None
            elif normalize(term) in STOPWORD_CONCEPTS:
                funnel["stopword"] += 1
                info[uri] = None
            elif pos in CONCEPTNET_POS_TO_NODE_TYPE:
                info[uri] = {"name": term, "pos": pos,
                             "node_type": CONCEPTNET_POS_TO_NODE_TYPE[pos],
                             "label_source": "uri"}
            elif pos and pos in FUNCTION_POS:
                funnel["function_pos"] += 1
                info[uri] = None
            else:
                info[uri] = None
                untagged[uri] = term
                funnel["untagged"] += 1

    print(f"  {len(info):,} unique endpoints "
          f"({funnel['untagged']:,} without a POS in the URI)")

    # --- pass 2: cascade only for the untagged ones ------------------------
    if untagged:
        dump_index = None
        dump_path = os.path.abspath(os.path.expanduser(args.dump))
        if os.path.isfile(dump_path):
            terms = {normalize(t) for t in untagged.values()}
            print(f"Scanning ConceptNet dump for {len(terms):,} terms ...")
            dump_index, diag = build_pos_index_from_dump(dump_path, terms)
            funnel["dump_lines"] = diag["lines"]
        else:
            print("  [WARN] dump not found; spaCy/NLTK/WordNet/heuristics only.")

        t0 = time.time()
        for uri, term in untagged.items():
            pos, source = resolve_pos(
                term, dump_index=dump_index,
                use_spacy=not args.no_spacy, use_nltk=not args.no_nltk,
                use_wordnet=not args.no_wordnet,
                use_heuristics=not args.no_heuristics)
            if pos in CONCEPTNET_POS_TO_NODE_TYPE:
                info[uri] = {"name": term, "pos": pos,
                             "node_type": CONCEPTNET_POS_TO_NODE_TYPE[pos],
                             "label_source": source}
            else:
                funnel[f"unresolved:{source}"] += 1
        print(f"  cascade finished in {time.time() - t0:.1f}s")

    # --- write nodes ---------------------------------------------------------
    nodes = [{"uri": uri, **d} for uri, d in info.items() if d]
    funnel["nodes_kept"] = len(nodes)
    with open(nodes_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["uri", "name", "pos",
                                          "node_type", "label_source"])
        w.writeheader()
        w.writerows(nodes)
    print(f"Wrote {len(nodes):,} typed nodes -> {nodes_path}")

    # --- write edges (classify + flag, do not filter on the contract) -------
    kept, dropped_endpoint, unknown_rel = 0, 0, Counter()
    violations = Counter()
    out_rows = []
    for rel, s, o, w in edges:
        si, oi = info.get(s), info.get(o)
        if not (si and oi):
            dropped_endpoint += 1
            continue
        classes = RELATION_TO_EDGE_CLASSES.get(rel)
        if not classes:
            unknown_rel[rel] += 1
            continue
        ec = f"{TYPE_LETTER[si['node_type']]}2{TYPE_LETTER[oi['node_type']]}"
        permitted = "ALL" in classes or ec in classes
        if not permitted:
            violations[f"{rel}:{ec}"] += 1
        out_rows.append({"relation": rel, "subject": s, "object": o,
                         "weight": w, "edge_class": ec,
                         "permitted": int(permitted)})
        kept += 1

    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["relation", "subject", "object",
                                          "weight", "edge_class", "permitted"])
        w.writeheader()
        w.writerows(out_rows)
    print(f"Wrote {kept:,} edges -> {edges_path}")
    print(f"  dropped: {dropped_endpoint:,} (endpoint), "
          f"{sum(unknown_rel.values()):,} (unknown relation)")
    print(f"  contract violations kept + flagged: {sum(violations.values()):,}")

    report = dict(funnel)
    report.update({
        "edges_raw": len(edges),
        "edges_kept": kept,
        "edges_dropped_endpoint": dropped_endpoint,
        "edges_dropped_unknown_relation": dict(unknown_rel),
        "contract_violations": dict(violations)
    })


    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(funnel, f, indent=2)
    print(f"Wrote funnel report -> {report_path}")


if __name__ == "__main__":
    main()