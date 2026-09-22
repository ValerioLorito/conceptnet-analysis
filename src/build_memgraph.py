#!/usr/bin/env python3
"""
build_memgraph.py — load the typed ConceptNet subgraph into Memgraph.

Twin loader of build_mysql.py; consumes the SAME two files produced by
label_concepts.py:

    typed_nodes.csv : uri, name, pos, node_type, label_source
    typed_edges.csv : relation, subject, object, weight, edge_class,
                      permitted

Graph schema established by this loader (Memgraph is schema-optional):
    (:Concept:EntityNode      {uri, name, pos, label_source})
    (:Concept:ActionEventNode {uri, name, pos, label_source})
    (:Concept:PropertyNode    {uri, name, pos, label_source})
    (:Concept)-[:<RelationName> {weight}]->(:Concept)   one type per relation

Every node carries exactly two labels: :Concept (the supertype — it scopes
the global uri-uniqueness constraint, the shared name/pos/label_source
indexes and the all-concepts queries) plus exactly one type label (the
graph image of nodes.node_type and of edge_class).

Identity rules — mirrored exactly by build_mysql.py:
    node = uri
    edge = (subject, relation, object); a duplicate triple keeps the max
    weight (MERGE ... ON MATCH SET), the Cypher image of MySQL's
    INSERT ... ON DUPLICATE KEY UPDATE.

A priori contract (STRICT_CONTRACT = True):
    only edges whose realized (subject_type, object_type) class is
    permitted for their relation — per RELATION_TO_EDGE_CLASSES in
    label_concepts.py, precomputed as the CSV `permitted` column — are
    loaded. Violating rows remain recorded in typed_edges.csv and
    pipeline_stats.json; they simply do not enter the graph. Set False in
    BOTH loaders for "faithful" mode (everything loaded, violations
    queryable in-DB).

Declared schema as data (LOAD_SCHEMA_META = True):
    property graphs cannot declare edge endpoint-type constraints, so the
    contract itself is stored as a small meta-graph under :Schema (kept
    out of every :Concept scope):
        (:Schema:NodeType)
        (:Schema:EdgeClass)-[:FROM|:TO]->(:Schema:NodeType)
        (:Schema:RelationType {wildcard})-[:PERMITS]->(:Schema:EdgeClass)
    ALL grants are expanded to the 9 concrete classes, mirroring
    relation_class_perms in conceptnet_schema.sql. The post-load report
    then verifies the loaded data against this declared schema with one
    query — the Cypher counterpart of
    SELECT COUNT(*) FROM v_edges WHERE NOT permitted.

Other notes
    * edge_class / permitted are NOT stored on edges: the class is implied
      by the endpoint labels, the permission by the relation type — the
      graph answer to what SQL stores in tables and columns.
    * set CREATE_EDGE_TYPE_INDEXES = True if your Memgraph version
      supports edge-type indexes; guarded below.
    * persistence: Memgraph is in-memory; enable snapshots/WAL
      (--storage-snapshot-interval-sec, --storage-wal-enabled) if you want
      the graph to survive restarts.

Run order
    1. python label_concepts.py       (produces the two CSVs)
    2. python build_memgraph.py       (this file)
    3. mysql -u <user> -p < conceptnet_schema.sql
    4. python build_mysql.py          (same CSVs, same rules)

Paired files: label_concepts.py (upstream), build_mysql.py (twin loader),
conceptnet_schema.sql (MySQL DDL).
"""

import csv
import json
import os
import re
import sys
import time
from collections import defaultdict

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

# --- Configuration (mirror of build_mysql.py's constants block) ------------

NODES_FILE = "data/preprocessed/typed_nodes.csv"
EDGES_FILE = "data/preprocessed/typed_edges.csv"

MEMGRAPH_URI  = "bolt://localhost:7687"
MEMGRAPH_AUTH = ("", "")

BATCH_SIZE               = 5_000
RESET_FIRST              = True
CREATE_EDGE_TYPE_INDEXES = False
STRICT_CONTRACT          = True
LOAD_SCHEMA_META         = True

NODE_TYPES  = {"EntityNode", "ActionEventNode", "PropertyNode"}
RELATION_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")   # guards the f-strings

LETTER_TO_TYPE = {"E": "EntityNode", "A": "ActionEventNode",
                  "P": "PropertyNode"}
ALL_CLASSES = [f"{a}2{b}" for a in "EAP" for b in "EAP"]

# Post-load verification queries ------------------------------------------------

LABEL_INVARIANT_QUERY = """
    MATCH (n:Concept)
    RETURN count(n) AS total,
           sum(CASE WHEN n:EntityNode      THEN 1 ELSE 0 END) AS entities,
           sum(CASE WHEN n:ActionEventNode THEN 1 ELSE 0 END) AS actions,
           sum(CASE WHEN n:PropertyNode    THEN 1 ELSE 0 END) AS properties,
           sum(CASE WHEN (n:EntityNode AND n:ActionEventNode)
                     OR (n:EntityNode AND n:PropertyNode)
                     OR (n:ActionEventNode AND n:PropertyNode)
                     OR NOT (n:EntityNode OR n:ActionEventNode
                             OR n:PropertyNode)
                    THEN 1 ELSE 0 END) AS malformed
"""

# Every loaded relation exists in the meta-graph (label_concepts.py drops
# unknown relations from the CSV), so the inner MATCH always binds.
VIOLATION_QUERY = """
    MATCH (s:Concept)-[r]->(o:Concept)
    MATCH (rt:Schema:RelationType) WHERE rt.name = type(r)
    OPTIONAL MATCH (rt)-[:PERMITS]->(c:Schema:EdgeClass)
        WHERE c.name = (CASE WHEN s:EntityNode THEN 'E'
                             WHEN s:ActionEventNode THEN 'A' ELSE 'P' END)
                    + '2' +
                    (CASE WHEN o:EntityNode THEN 'E'
                          WHEN o:ActionEventNode THEN 'A' ELSE 'P' END)
    WITH r, c WHERE c IS NULL
    RETURN count(r) AS violations
"""


# --- Small helpers -----------------------------------------------------------

def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run_ignore_errors(session, stmt):
    try:
        session.run(stmt)
        print(f"  ok: {stmt}")
    except Neo4jError as e:
        print(f"  skip ({e.code}): {stmt}")


def load_pipeline_stats():
    path = os.path.join(os.path.dirname(os.path.abspath(NODES_FILE)),
                        "pipeline_stats.json")
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


# --- Schema setup --------------------------------------------------------------

def setup_schema(session, relations):
    # Wipes EVERYTHING (data + :Schema meta-graph of a previous run); the
    # meta-graph is rebuilt afterwards by load_schema_meta().
    if RESET_FIRST:
        session.run("MATCH (n) DETACH DELETE n")
    run_ignore_errors(session,
        "CREATE CONSTRAINT ON (c:Concept) ASSERT c.uri IS UNIQUE")
    run_ignore_errors(session, "CREATE INDEX ON :Concept(name)")
    run_ignore_errors(session, "CREATE INDEX ON :Concept(pos)")
    run_ignore_errors(session, "CREATE INDEX ON :Concept(label_source)")
    if CREATE_EDGE_TYPE_INDEXES:
        for rel in relations:
            run_ignore_errors(session, f"CREATE EDGE INDEX ON :{rel}")


# --- Loading --------------------------------------------------------------------

def load_nodes(session, nodes):
    by_type = defaultdict(list)
    for n in nodes:
        if n["node_type"] not in NODE_TYPES:
            raise SystemExit(f"typed_nodes.csv: unknown node_type "
                             f"{n['node_type']!r} (URI {n['uri']})")
        by_type[n["node_type"]].append(n)

    for node_type, rows in sorted(by_type.items()):
        cypher = f"""
            UNWIND $rows AS row
            MERGE (c:Concept:{node_type} {{uri: row.uri}})
            SET c.name = row.name,
                c.pos = row.pos,
                c.label_source = row.label_source
        """
        t0, count = time.time(), 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            session.run(cypher, rows=batch)
            count += len(batch)
        print(f"  :{node_type:<15} {count:>9,} nodes   "
              f"({time.time() - t0:.1f}s)")


def load_edges(session, edges):
    """
    Load typed_edges.csv as one edge type per relation, applying the same
    filters build_mysql.py applies (so the two stores stay isomorphic):
      * relation names must be Cypher-safe (RELATION_RE);
      * STRICT_CONTRACT: rows with permitted=0 are not loaded.
    MERGE gives idempotent re-runs and the edge-identity rule: a duplicate
    (subject, relation, object) triple keeps the max weight.
    Returns a stats dict consumed by report().
    """
    by_rel = defaultdict(list)
    skipped_name = skipped_contract = csv_permitted0 = 0
    seen, unique_expected = set(), 0

    for e in edges:
        rel = e["relation"].strip()
        if not RELATION_RE.match(rel):
            skipped_name += 1
            continue
        if e.get("permitted") == "0":
            csv_permitted0 += 1
            if STRICT_CONTRACT:
                skipped_contract += 1
                continue
        key = (e["subject"], rel, e["object"])
        if key not in seen:
            seen.add(key)
            unique_expected += 1
        e["weight"] = float(e["weight"])
        by_rel[rel].append(e)

    if skipped_name:
        print(f"  [WARN] {skipped_name:,} edges with non-Cypher-safe "
              f"relation names skipped")
    if skipped_contract:
        print(f"  strict contract: {skipped_contract:,} violating edges "
              f"NOT loaded (recorded in pipeline_stats.json / "
              f"typed_edges.csv)")

    for rel in sorted(by_rel):
        rows = by_rel[rel]
        cypher = f"""
            UNWIND $rows AS row
            MATCH (s:Concept {{uri: row.subject}})
            MATCH (o:Concept {{uri: row.object}})
            MERGE (s)-[r:{rel}]->(o)
            ON CREATE SET r.weight = row.weight
            ON MATCH  SET r.weight = CASE WHEN row.weight > r.weight
                                          THEN row.weight ELSE r.weight END
        """
        t0 = time.time()
        for i in range(0, len(rows), BATCH_SIZE):
            session.run(cypher, rows=rows[i:i + BATCH_SIZE])
        print(f"  {rel:<28} {len(rows):>9,} edges  ({time.time() - t0:.1f}s)")

    return {
        "csv_total": len(edges),
        "eligible": sum(len(v) for v in by_rel.values()),
        "unique_expected": unique_expected,
        "skipped_name": skipped_name,
        "skipped_contract": skipped_contract,
        "csv_permitted0": csv_permitted0,
    }


def load_schema_meta(session):
    """
    Store the a priori contract as data — the graph's answer to a
    'declared schema', which its engine cannot enforce:
        (:Schema:NodeType)             EntityNode / ActionEventNode / PropertyNode
        (:Schema:EdgeClass)-[:FROM|:TO]->(:Schema:NodeType)
        (:Schema:RelationType {wildcard})-[:PERMITS]->(:Schema:EdgeClass)
    ALL grants are expanded to the 9 concrete classes, mirroring
    relation_class_perms in conceptnet_schema.sql.
    """
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        # NB: importing label_concepts pulls in nltk/requests; acceptable
        # for a one-shot meta load, skipped gracefully if unavailable.
        from label_concepts import RELATION_TO_EDGE_CLASSES
    except Exception as exc:
        print(f"  [WARN] schema meta-graph skipped — label_concepts.py "
              f"not importable ({exc!r})")
        return

    session.run("MATCH (n:Schema) DETACH DELETE n")
    run_ignore_errors(session, "CREATE INDEX ON :Schema:RelationType(name)")
    run_ignore_errors(session, "CREATE INDEX ON :Schema:EdgeClass(name)")

    session.run("UNWIND $names AS n MERGE (:Schema:NodeType {name: n})",
                names=sorted(set(LETTER_TO_TYPE.values())))
    for ec in ALL_CLASSES:
        subj, obj = ec.split("2")
        session.run("""
            MERGE (c:Schema:EdgeClass {name: $ec})
            MERGE (s:Schema:NodeType {name: $s})
            MERGE (o:Schema:NodeType {name: $o})
            MERGE (c)-[:FROM]->(s)
            MERGE (c)-[:TO]->(o)
        """, ec=ec, s=LETTER_TO_TYPE[subj], o=LETTER_TO_TYPE[obj])

    for rel, classes in sorted(RELATION_TO_EDGE_CLASSES.items()):
        wildcard = "ALL" in classes
        expanded = ALL_CLASSES if wildcard else sorted(classes)
        session.run(
            "MERGE (:Schema:RelationType {name: $rel, wildcard: $wildcard})",
            rel=rel, wildcard=wildcard)
        session.run("""
            MATCH (r:Schema:RelationType {name: $rel})
            MATCH (c:Schema:EdgeClass) WHERE c.name IN $classes
            MERGE (r)-[:PERMITS]->(c)
        """, rel=rel, classes=expanded)

    grants = session.run("""
        MATCH (:Schema:RelationType)-[:PERMITS]->(:Schema:EdgeClass)
        RETURN count(*) AS c
    """).single()["c"]
    print(f"  meta-graph: {len(RELATION_TO_EDGE_CLASSES)} relation types, "
          f"{grants} grants (ALL expanded) — the declared schema, as data")


# --- Report (mirrors build_mysql.py's report, plus graph-specific checks) ----

def report(session, es, stats):
    print("\nPost-load summary")
    for label in ("EntityNode", "ActionEventNode", "PropertyNode"):
        c = session.run(
            f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
        print(f"  :{label:<15} {c:>9,} nodes")
    n = session.run("MATCH (n:Concept) RETURN count(n) AS c").single()["c"]
    # Concept-scoped, so the :Schema meta-graph never contaminates counts.
    e = session.run(
        "MATCH (:Concept)-[r]->(:Concept) RETURN count(r) AS c").single()["c"]
    print(f"  {'nodes (total)':<18} {n:>9,}")
    print(f"  {'edges (total)':<18} {e:>9,}")
    merged = es["eligible"] - e
    if merged:
        print(f"  [INFO] {es['eligible']:,} eligible rows -> {e:,} edges "
              f"({merged:,} duplicate triples merged, max-weight rule)")
    if e != es["unique_expected"]:
        print(f"  [WARN] edge count != unique triples "
              f"({es['unique_expected']:,}) — investigate")

    print("\nLabel provenance")
    for rec in session.run(
            "MATCH (n:Concept) "
            "RETURN n.label_source AS src, count(n) AS c ORDER BY c DESC"):
        print(f"  {rec['src']:<16} {rec['c']:>9,}")

    print("\nLabel invariant (exactly one type label per :Concept — "
          "loader-maintained; MySQL enforces this in-engine)")
    try:
        rec = session.run(LABEL_INVARIANT_QUERY).single()
        ok = (rec["malformed"] == 0 and rec["total"] ==
              rec["entities"] + rec["actions"] + rec["properties"])
        print(f"  total={rec['total']:,}  E={rec['entities']:,}  "
              f"A={rec['actions']:,}  P={rec['properties']:,}  "
              f"malformed={rec['malformed']:,}  "
              f"[{'ok' if ok else 'VIOLATION'}]")
    except Neo4jError as exc:
        print(f"  (unavailable: {exc.code})")

    print("\nContract (a priori relation->class permission)")
    print(f"  mode                 : "
          f"{'STRICT — violating edges not loaded' if STRICT_CONTRACT else 'FAITHFUL — violations loaded and queryable'}")
    print(f"  CSV rows permitted=0 : {es['csv_permitted0']:>9,}   "
          f"(audit record: pipeline_stats.json / typed_edges.csv)")
    if STRICT_CONTRACT:
        print(f"  filtered at load     : {es['skipped_contract']:>9,}")
    try:
        viol = session.run(VIOLATION_QUERY).single()["violations"]
        print(f"  violations in graph  : {viol:>9,}   "
              f"(vs the :Schema meta-graph; expected "
              f"{0 if STRICT_CONTRACT else es['csv_permitted0']:,})")
    except Neo4jError as exc:
        print(f"  violations in graph  : (unavailable: {exc.code})")

    if stats is not None:
        print("\nIsomorphism cross-check vs pipeline_stats.json")
        exp_nodes = stats.get("nodes_kept")
        stats_viol = sum(stats.get("contract_violations", {}).values())
        dups = stats.get("edges_duplicate_triples", 0)
        exp_edges = (stats.get("edges_kept", 0) - dups
                     - (stats_viol if STRICT_CONTRACT else 0))
        print(f"  nodes: DB {n:>9,}  expected {exp_nodes:>9,}  "
              f"[{'ok' if exp_nodes == n else 'MISMATCH'}]")
        flag = "ok" if exp_edges == e else "CAUTION"
        print(f"  edges: DB {e:>9,}  expected {exp_edges:>9,}  [{flag}]")
        if flag != "ok" and dups and stats_viol:
            print("    (the stats formula assumes duplicate rows and "
                  "violating rows are disjoint; the loader-side "
                  "unique-triple check above is the authoritative one)")

    report_storage(session)


def report_storage(session):
    print("\nStorage (SHOW STORAGE INFO — the Memgraph side of the "
          "memory metric)")
    try:
        recs = [dict(r) for r in session.run("SHOW STORAGE INFO")]
    except Neo4jError as exc:
        print(f"  (unavailable: {exc.code})")
        return
    for rec in recs:
        # Handle both possible shapes: one map field per row, or flat
        # name/value fields.
        if len(rec) == 1 and isinstance(next(iter(rec.values())), dict):
            info = next(iter(rec.values()))
            print(f"  {str(info.get('name', '?')):<32} {info.get('value', '?')}")
        else:
            print("  " + "  ".join(f"{k}={v}" for k, v in rec.items()))


# --- Main -----------------------------------------------------------------------

def main():
    t_start = time.time()

    print(f"Reading {NODES_FILE} ...")
    nodes = read_csv(NODES_FILE)
    print(f"  {len(nodes):,} nodes")
    print(f"Reading {EDGES_FILE} ...")
    edges = read_csv(EDGES_FILE)
    print(f"  {len(edges):,} edges")

    if nodes and "node_type" not in nodes[0]:
        raise SystemExit("typed_nodes.csv lacks the 'node_type' column — "
                         "regenerate it with label_concepts.py")
    if edges and "permitted" not in edges[0]:
        raise SystemExit("typed_edges.csv lacks the 'permitted' column — "
                         "regenerate it with label_concepts.py")

    relations = sorted({e["relation"].strip() for e in edges})
    driver = GraphDatabase.driver(MEMGRAPH_URI, auth=MEMGRAPH_AUTH)
    try:
        with driver.session() as session:
            print("\nSchema setup")
            setup_schema(session, relations)
            print("\nLoading nodes")
            load_nodes(session, nodes)
            print("\nLoading edges")
            es = load_edges(session, edges)
            if LOAD_SCHEMA_META:
                print("\nSchema meta-graph (declared contract as data)")
                load_schema_meta(session)
            report(session, es, load_pipeline_stats())
    finally:
        driver.close()
    print(f"\nDone. ({time.time() - t_start:.1f}s total)")


if __name__ == "__main__":
    main()