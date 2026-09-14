#!/usr/bin/env python3
"""
Label ConceptNet concepts with POS tags, node types and possible edge classes.

Pipeline
--------
1.  Read input ConceptNet CSV (relation, subject, object, weight).
2.  Filter function words / stopwords.
3.  For each remaining concept:
        a. query the ConceptNet API;
        b. extract POS votes from the concept URI and its edges;
        c. if ConceptNet has no usable POS, fall back to NLTK WordNet;
        d. if the concept URI POS is a function tag (c/p/d/x/t), reject
           the concept without falling back to WordNet.
4.  Refine the seed labels with a graph-based collective classifier that
    uses:
        - hard-coded relation signatures (allowed source/target types);
        - same-type propagation for Antonym / SimilarTo / etc.
5.  Write:
        - concept_labels.csv      (final labels, node type, link classes, relations)
        - unresolved_concepts.csv (concepts without a trustworthy label)
        - pipeline_stats.json     (optional; counts at every stage)

The script also prints a full "pipeline funnel" so that every input
concept is accounted for.
"""

import argparse
import csv
import json
import os
import sys
import time
from collections import Counter, defaultdict
from urllib.parse import quote

import requests

try:
    import nltk
    from nltk.corpus import wordnet as wn
except ImportError:
    print("Please install nltk:  pip install nltk", file=sys.stderr)
    sys.exit(1)

try:
    wn.synsets("dog")
except LookupError:
    nltk.download("wordnet", quiet=True)
    nltk.download("omw-1.4", quiet=True)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

E = "EntityNode"
A = "ActionEventNode"
P = "PropertyNode"
UNKNOWN = "UNKNOWN"

# Strict POS mapping: only content words. Function tags are intentionally
# absent so that they are rejected during voting.
CONCEPTNET_POS_TO_NODE_TYPE = {
    "n": E,
    "v": A,
    "a": P,
    "r": P,
    "s": P,
}

# POS codes that identify function words in ConceptNet URIs.
FUNCTION_POS = {"c", "p", "d", "x", "t"}

# For each primary node type, the link classes it can participate in
# (union of outgoing and incoming classes).
NODE_TYPE_LINK_CLASSES = {
    E: ["E2E", "E2A", "E2P", "A2E", "P2E"],
    A: ["A2E", "A2A", "A2P", "E2A", "P2A"],
    P: ["P2E", "P2A", "P2P", "E2P", "A2P"],
}

# Relation signatures: which node types are allowed as subject / object
# and how strongly the relation votes for each.
RELATION_SIGNATURES = {
    # -- Entity -> Entity --------------------------------------------------
    "IsA":               {"subject": {E: 3.0}, "object": {E: 3.0}},
    "InstanceOf":        {"subject": {E: 3.0}, "object": {E: 3.0}},
    "PartOf":            {"subject": {E: 3.0}, "object": {E: 3.0}},
    "HasA":              {"subject": {E: 3.0}, "object": {E: 3.0}},
    "MadeOf":            {"subject": {E: 3.0}, "object": {E: 3.0}},
    "AtLocation":        {"subject": {E: 3.0}, "object": {E: 3.0}},
    "LocatedNear":       {"subject": {E: 3.0}, "object": {E: 3.0}},
    "CreatedBy":         {"subject": {E: 3.0}, "object": {E: 3.0}},

    # -- Entity / Action -> Property --------------------------------------
    "HasProperty":       {"subject": {E: 2.0, A: 2.0}, "object": {P: 3.0}},
    "NotHasProperty":    {"subject": {E: 2.0, A: 2.0}, "object": {P: 3.0}},
    "PropertyOf":        {"subject": {P: 3.0},        "object": {E: 2.0, A: 2.0}},

    # -- Entity -> Action --------------------------------------------------
    "CapableOf":         {"subject": {E: 3.0}, "object": {A: 3.0}},
    "NotCapableOf":      {"subject": {E: 3.0}, "object": {A: 3.0}},
    "UsedFor":           {"subject": {E: 3.0}, "object": {A: 3.0}},
    "ReceivesAction":    {"subject": {E: 3.0}, "object": {A: 3.0}},

    # -- Action -> Action --------------------------------------------------
    "HasSubevent":       {"subject": {A: 3.0}, "object": {A: 3.0}},
    "HasFirstSubevent":  {"subject": {A: 3.0}, "object": {A: 3.0}},
    "HasLastSubevent":   {"subject": {A: 3.0}, "object": {A: 3.0}},
    "HasPrerequisite":   {"subject": {A: 3.0}, "object": {A: 3.0}},
    "Entails":           {"subject": {A: 3.0}, "object": {A: 3.0}},
    "MannerOf":          {"subject": {A: 3.0}, "object": {A: 3.0}},
    "MotivatedByGoal":   {"subject": {A: 3.0}, "object": {A: 3.0}},

    # -- Causal (broad) ----------------------------------------------------
    "Causes":            {"subject": {E: 2.0, A: 2.0},
                          "object":  {E: 1.5, A: 1.5, P: 1.5}},
    "CausesDesire":      {"subject": {E: 2.0, A: 2.0},
                          "object":  {A: 2.5}},

    # -- Desire ------------------------------------------------------------
    "Desires":           {"subject": {E: 3.0}, "object": {E: 2.0, A: 2.0}},
    "NotDesires":        {"subject": {E: 3.0}, "object": {E: 2.0, A: 2.0}},

    # -- Definition --------------------------------------------------------
    "DefinedAs":         {"subject": {E: 2.0}, "object": {E: 2.0, P: 2.0}},

    # -- Permissive --------------------------------------------------------
    "RelatedTo":         {"subject": {E: 1.0, A: 1.0, P: 1.0},
                          "object":  {E: 1.0, A: 1.0, P: 1.0}},
    "HasContext":        {"subject": {E: 1.0, A: 1.0, P: 1.0},
                          "object":  {E: 1.0, A: 1.0, P: 1.0}},
}

# Relations that propagate one endpoint's label to the other.
SAME_TYPE_RELATIONS = {
    "Antonym",
    "SimilarTo",
    "DistinctFrom",
    "FormOf",
    "DerivedFrom",
    "EtymologicallyDerivedFrom",
    "EtymologicallyRelatedTo",
}

# Function words and other non-content concepts to drop up front.
STOPWORD_CONCEPTS = {
    "a", "an", "the", "and", "or", "not",
    "be", "is", "are", "was", "were", "am",
    "do", "does", "did", "done",
    "have", "has", "had", "having",
    "of", "in", "on", "at", "to", "for", "with", "by", "from",
    "if", "but", "so", "as", "that", "this", "these", "those",
    "it", "its", "he", "she", "they", "them", "his", "her",
    "i", "you", "we", "us", "me", "my", "your", "our",
    "thing", "things", "someone", "something", "anything",
    "anyone", "everyone", "everything", "somewhere", "anywhere",
    "nothing", "nobody", "none", "no",
}

# Classifier hyperparameters
SEED_WEIGHT = 1.0
SAME_TYPE_WEIGHT = 1.5
MAX_ITER = 15
MARGIN_THRESHOLD = 1.5   # keep graph-only labels only if top / second >= threshold


# ---------------------------------------------------------------------------
# Pipeline accounting
# ---------------------------------------------------------------------------

class PipelineStats:
    """Tracks counts at every stage of the labeling pipeline."""

    def __init__(self):
        # Raw input
        self.raw_concepts = 0
        self.raw_edges = 0

        # Filtering
        self.removed_stopwords = 0
        self.removed_function_pos = 0
        self.concepts_after_filter = 0

        # Seeding
        self.seed_conceptnet = 0
        self.seed_wordnet = 0
        self.seed_none = 0

        # Graph refinement
        self.graph_corrected = 0
        self.graph_only_accepted = 0
        self.graph_only_rejected = 0

        # Final outputs
        self.labeled_total = 0
        self.unresolved_total = 0

        # Diagnostics
        self.unresolved_reasons = Counter()

    def as_dict(self):
        return {
            "raw_concepts":          self.raw_concepts,
            "raw_edges":             self.raw_edges,
            "removed_stopwords":     self.removed_stopwords,
            "removed_function_pos":  self.removed_function_pos,
            "concepts_after_filter": self.concepts_after_filter,
            "seed_conceptnet":       self.seed_conceptnet,
            "seed_wordnet":          self.seed_wordnet,
            "seed_none":             self.seed_none,
            "graph_corrected":       self.graph_corrected,
            "graph_only_accepted":   self.graph_only_accepted,
            "graph_only_rejected":   self.graph_only_rejected,
            "labeled_total":         self.labeled_total,
            "unresolved_total":      self.unresolved_total,
            "unresolved_reasons":    dict(self.unresolved_reasons),
        }


def report_stats(stats: "PipelineStats", output_stream=sys.stdout):
    """Print a human-readable summary of the pipeline funnel."""
    d = stats.as_dict()

    def pct(n, total):
        return f"({100.0 * n / total:5.2f}%)" if total else "(  0.00%)"

    out = output_stream
    print("\nPipeline funnel", file=out)
    print("===============", file=out)
    print(f"  Raw input concepts                 : {d['raw_concepts']:>8}", file=out)
    print(f"  Raw input edges                    : {d['raw_edges']:>8}", file=out)
    print(file=out)

    print("Stage 1 - filtering", file=out)
    print(f"  removed (stopword)                 : {d['removed_stopwords']:>8}  "
          f"{pct(d['removed_stopwords'], d['raw_concepts'])}", file=out)
    print(f"  removed (ConceptNet function POS)  : {d['removed_function_pos']:>8}  "
          f"{pct(d['removed_function_pos'], d['raw_concepts'])}", file=out)
    print(f"  kept for labeling                  : {d['concepts_after_filter']:>8}  "
          f"{pct(d['concepts_after_filter'], d['raw_concepts'])}", file=out)
    print(file=out)

    print("Stage 2 - seeding", file=out)
    print(f"  seeded from ConceptNet             : {d['seed_conceptnet']:>8}  "
          f"{pct(d['seed_conceptnet'], d['concepts_after_filter'])}", file=out)
    print(f"  seeded from WordNet                : {d['seed_wordnet']:>8}  "
          f"{pct(d['seed_wordnet'], d['concepts_after_filter'])}", file=out)
    print(f"  no seed (graph-only candidates)    : {d['seed_none']:>8}  "
          f"{pct(d['seed_none'], d['concepts_after_filter'])}", file=out)
    print(file=out)

    print("Stage 3 - graph refinement", file=out)
    print(f"  graph corrected a seed             : {d['graph_corrected']:>8}", file=out)
    print(f"  graph-only labels accepted         : {d['graph_only_accepted']:>8}", file=out)
    print(f"  graph-only labels rejected         : {d['graph_only_rejected']:>8}", file=out)
    print(file=out)

    print("Final outputs", file=out)
    print(f"  labeled concepts                   : {d['labeled_total']:>8}  "
          f"{pct(d['labeled_total'], d['concepts_after_filter'])}", file=out)
    print(f"  unresolved concepts                : {d['unresolved_total']:>8}  "
          f"{pct(d['unresolved_total'], d['concepts_after_filter'])}", file=out)
    if d["unresolved_reasons"]:
        print("  unresolved reasons:", file=out)
        for reason, count in sorted(d["unresolved_reasons"].items(),
                                    key=lambda kv: -kv[1]):
            print(f"      {reason:<28}: {count:>6}", file=out)
    print(file=out)

    # Sanity check: everything that entered the funnel must be accounted for.
    accounted = (d["labeled_total"] + d["unresolved_total"]
                 + d["removed_stopwords"] + d["removed_function_pos"])
    if accounted != d["raw_concepts"]:
        print(f"  [WARN] accounting mismatch: "
              f"{accounted} != {d['raw_concepts']}", file=out)
    else:
        print(f"  accounting check: all {d['raw_concepts']} concepts accounted for.",
              file=out)


def save_stats(stats: "PipelineStats", path: str):
    """Write the stats dictionary as JSON."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(stats.as_dict(), f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize(term: str) -> str:
    return term.strip().lower().replace(" ", "_")


def load_cache(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(path: str, cache: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)


# ---------------------------------------------------------------------------
# ConceptNet API
# ---------------------------------------------------------------------------

def query_conceptnet(concept, session, cache, api_base, sleep_seconds, max_attempts=3):
    if concept in cache:
        return cache[concept]

    encoded = quote(concept, safe="")
    url = f"{api_base}/c/en/{encoded}"
    result = None

    for attempt in range(max_attempts):
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 200:
                data = r.json()
                result = {
                    "uri": data.get("@id"),
                    "edges": [
                        {
                            "start":  e.get("start", {}).get("@id"),
                            "end":    e.get("end",   {}).get("@id"),
                            "rel":    e.get("rel",   {}).get("@id"),
                            "weight": float(e.get("weight", 1.0)),
                        }
                        for e in data.get("edges", [])
                    ],
                }
                break
            if r.status_code == 404:
                break
            time.sleep(1.0 * (attempt + 1))
        except requests.RequestException:
            time.sleep(1.0 * (attempt + 1))

    cache[concept] = result
    time.sleep(sleep_seconds)
    return result


def extract_pos_from_conceptnet(concept, data):
    """
    Return (winning_pos, uri_pos_code).

    winning_pos: dominant POS code among valid votes, or None.
    uri_pos_code: POS code of the concept URI itself, or None.
    """
    if not data:
        return None, None

    target = normalize(concept)
    votes = {}

    def add(pos, weight=1.0):
        if pos in CONCEPTNET_POS_TO_NODE_TYPE:
            votes[pos] = votes.get(pos, 0.0) + weight

    # URI POS
    uri = data.get("uri") or ""
    parts = uri.split("/")
    uri_pos = None
    if len(parts) > 4 and parts[1] == "c" and parts[2] == "en":
        uri_pos = parts[4]
        add(uri_pos, weight=2.0)

    # Edge votes
    for edge in data.get("edges", []):
        for key in ("start", "end"):
            ep = edge.get(key) or ""
            parts = ep.split("/")
            if len(parts) > 4 and parts[1] == "c" and parts[2] == "en":
                if normalize(parts[3]) == target:
                    add(parts[4], weight=edge.get("weight", 1.0))

    if not votes:
        return None, uri_pos
    return max(votes.items(), key=lambda kv: kv[1])[0], uri_pos


# ---------------------------------------------------------------------------
# WordNet fallback
# ---------------------------------------------------------------------------

def wordnet_pos(concept: str):
    term = concept.replace("_", " ").lower()
    for wn_pos, cn_pos in (("n", "n"), ("v", "v"), ("a", "a"), ("r", "r")):
        try:
            if wn.synsets(term, pos=wn_pos):
                return cn_pos
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------

def count_raw_concepts_and_edges(path: str):
    """Read the input once just to count raw unique concepts and edges."""
    concepts = set()
    edges = 0
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rel = (row.get("relation") or "").strip()
            subj = (row.get("subject") or "").strip()
            obj = (row.get("object") or "").strip()
            if not (rel and subj and obj):
                continue
            edges += 1
            concepts.add(subj)
            concepts.add(obj)
    return concepts, edges


def read_input(path):
    concepts = set()
    relations_by_concept = defaultdict(set)
    edges = []

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
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
            concepts.add(subj)
            concepts.add(obj)
            relations_by_concept[subj].add(rel)
            relations_by_concept[obj].add(rel)

    return concepts, relations_by_concept, edges


# ---------------------------------------------------------------------------
# Graph-based collective classification
# ---------------------------------------------------------------------------

def classify_concepts(concepts, edges, seed_labels, max_iter=MAX_ITER):
    """
    Iterative collective classification.

    Returns
    -------
    labels  : dict concept -> node_type or UNKNOWN
    margins : dict concept -> top / second ratio (inf if only one class)
    """
    labels = {c: seed_labels.get(c, UNKNOWN) for c in concepts}

    for _ in range(max_iter):
        votes = defaultdict(lambda: defaultdict(float))

        # Seed
        for c, t in labels.items():
            if t != UNKNOWN:
                votes[c][t] += SEED_WEIGHT

        # Edge votes
        for rel, s, o, w in edges:
            sig = RELATION_SIGNATURES.get(rel)
            if sig:
                for t, v in sig["subject"].items():
                    votes[s][t] += v * w
                for t, v in sig["object"].items():
                    votes[o][t] += v * w

            if rel in SAME_TYPE_RELATIONS:
                ts = labels.get(s, UNKNOWN)
                to = labels.get(o, UNKNOWN)
                if ts != UNKNOWN:
                    votes[o][ts] += SAME_TYPE_WEIGHT * w
                if to != UNKNOWN:
                    votes[s][to] += SAME_TYPE_WEIGHT * w

        new_labels = {}
        for c in concepts:
            dist = votes.get(c, {})
            if dist:
                new_labels[c] = max(dist.items(), key=lambda kv: kv[1])[0]
            else:
                new_labels[c] = labels.get(c, UNKNOWN)

        if new_labels == labels:
            break
        labels = new_labels

    # Recompute margins at convergence
    final_votes = defaultdict(lambda: defaultdict(float))
    for c, t in labels.items():
        if t != UNKNOWN:
            final_votes[c][t] += SEED_WEIGHT
    for rel, s, o, w in edges:
        sig = RELATION_SIGNATURES.get(rel)
        if sig:
            for t, v in sig["subject"].items():
                final_votes[s][t] += v * w
            for t, v in sig["object"].items():
                final_votes[o][t] += v * w
        if rel in SAME_TYPE_RELATIONS:
            ts = labels.get(s, UNKNOWN)
            to = labels.get(o, UNKNOWN)
            if ts != UNKNOWN:
                final_votes[o][ts] += SAME_TYPE_WEIGHT * w
            if to != UNKNOWN:
                final_votes[s][to] += SAME_TYPE_WEIGHT * w

    margins = {}
    for c in concepts:
        dist = final_votes.get(c, {})
        if not dist:
            margins[c] = 0.0
            continue
        ranked = sorted(dist.values(), reverse=True)
        top = ranked[0]
        second = ranked[1] if len(ranked) > 1 else 0.0
        margins[c] = float("inf") if second == 0 else top / second

    return labels, margins


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("input_csv")
    parser.add_argument("-o", "--output-csv", default="concept_labels.csv")
    parser.add_argument("-u", "--unresolved-csv", default="unresolved_concepts.csv")
    parser.add_argument("-c", "--cache-file", default="conceptnet_cache.json")
    parser.add_argument("--api-base", default="https://api.conceptnet.io")
    parser.add_argument("--sleep", type=float, default=0.2,
                        help="Seconds between ConceptNet requests")
    parser.add_argument("--margin", type=float, default=MARGIN_THRESHOLD,
                        help="Minimum top/second ratio to trust a graph-only label")
    parser.add_argument("--no-api", action="store_true",
                        help="Skip ConceptNet API; use WordNet + graph only")
    parser.add_argument("--stats-file", default=None,
                        help="Optional path to write pipeline stats as JSON")
    args = parser.parse_args()

    stats = PipelineStats()

    # --- 1. Read input -----------------------------------------------------
    print(f"Reading {args.input_csv} ...")
    raw_concepts, raw_edges = count_raw_concepts_and_edges(args.input_csv)
    stats.raw_concepts = len(raw_concepts)
    stats.raw_edges = raw_edges
    print(f"  {stats.raw_concepts} unique concepts, {stats.raw_edges} edges.")

    concepts, relations_by_concept, edges = read_input(args.input_csv)

    # Drop function words up front
    kept = set()
    removed_stopwords = 0
    for c in concepts:
        if normalize(c) in STOPWORD_CONCEPTS:
            removed_stopwords += 1
        else:
            kept.add(c)
    concepts = kept
    stats.removed_stopwords = removed_stopwords
    stats.concepts_after_filter = len(concepts)
    print(f"  {stats.removed_stopwords} stopwords removed.")
    print(f"  {stats.concepts_after_filter} concepts after stopword filter.")

    # --- 2. Seed labels (ConceptNet URI -> WordNet) ------------------------
    cache = load_cache(args.cache_file)
    session = requests.Session()

    seed_labels = {}
    pos_raw = {}
    pos_source = {}

    for i, concept in enumerate(sorted(concepts), 1):
        pos = None
        src = None
        uri_pos = None

        if not args.no_api:
            data = query_conceptnet(concept, session, cache,
                                    args.api_base, args.sleep)
            if data is not None:
                pos, uri_pos = extract_pos_from_conceptnet(concept, data)
                if pos:
                    src = "conceptnet"

        # If ConceptNet knows this is a function word, reject outright.
        if uri_pos in FUNCTION_POS:
            stats.removed_function_pos += 1
            pos = None
            src = None

        # Fall back to WordNet only if ConceptNet had nothing usable
        # and did not explicitly flag a function POS.
        if not pos and uri_pos not in FUNCTION_POS:
            wn_pos = wordnet_pos(concept)
            if wn_pos:
                pos = wn_pos
                src = "wordnet"

        if pos and src:
            pos_raw[concept] = pos
            pos_source[concept] = src
            seed_labels[concept] = CONCEPTNET_POS_TO_NODE_TYPE.get(pos, UNKNOWN)
        else:
            pos_raw[concept] = ""
            pos_source[concept] = "none"

        if i % 100 == 0 or i == len(concepts):
            print(f"  [{i}/{len(concepts)}] seeded")

    save_cache(args.cache_file, cache)

    # Aggregate seeding stats
    stats.seed_conceptnet = sum(1 for c in concepts if pos_source.get(c) == "conceptnet")
    stats.seed_wordnet = sum(1 for c in concepts if pos_source.get(c) == "wordnet")
    stats.seed_none = sum(1 for c in concepts if pos_source.get(c) == "none")

    # --- 3. Graph-based collective classification --------------------------
    print("Running graph-based collective classification ...")
    final_labels, margins = classify_concepts(concepts, edges, seed_labels)

    # --- 4. Write outputs --------------------------------------------------
    rows = []
    unresolved = []

    for concept in sorted(concepts):
        seed = seed_labels.get(concept, UNKNOWN)
        final = final_labels.get(concept, UNKNOWN)
        margin = margins.get(concept, 0.0)

        if seed == UNKNOWN:
            # No trustworthy seed: trust the graph only if the margin is high
            if final == UNKNOWN or margin < args.margin:
                stats.graph_only_rejected += 1
                reason = "no_seed_low_margin" if final != UNKNOWN else "no_label"
                stats.unresolved_reasons[reason] += 1
                unresolved.append({
                    "concept": concept,
                    "reason": reason,
                    "margin": round(margin, 3) if margin != float("inf") else "inf",
                    "connected_relations": "|".join(
                        sorted(relations_by_concept[concept])),
                })
                continue
            stats.graph_only_accepted += 1
            label_source = "graph_only"
        else:
            if final == UNKNOWN:
                final = seed
            if final == seed:
                label_source = pos_source[concept]
            else:
                stats.graph_corrected += 1
                label_source = "graph_corrected"

        link_classes = NODE_TYPE_LINK_CLASSES.get(final, [])
        rows.append({
            "concept": concept,
            "pos": pos_raw.get(concept, ""),
            "node_type": final,
            "label_source": label_source,
            "possible_edge_classes": "|".join(link_classes),
            "connected_relations": "|".join(
                sorted(relations_by_concept[concept])),
        })

    with open(args.output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "concept", "pos", "node_type", "label_source",
            "possible_edge_classes", "connected_relations",
        ])
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} labeled concepts to {args.output_csv}")

    if unresolved:
        with open(args.unresolved_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "concept", "reason", "margin", "connected_relations",
            ])
            writer.writeheader()
            writer.writerows(unresolved)
        print(f"Wrote {len(unresolved)} unresolved concepts to {args.unresolved_csv}")

    stats.labeled_total = len(rows)
    stats.unresolved_total = len(unresolved)

    # --- 5. Summary --------------------------------------------------------
    report_stats(stats)

    if args.stats_file:
        save_stats(stats, args.stats_file)
        print(f"Wrote pipeline statistics to {args.stats_file}")


if __name__ == "__main__":
    main()