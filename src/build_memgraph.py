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
    weight. The rule is now enforced in PYTHON, before creation (the SQL
    image is INSERT ... ON DUPLICATE KEY UPDATE ... GREATEST(...)).

PLANNER NOTE (why this loader avoids property matching in bulk):
    On several Memgraph versions the planner does NOT use the
    :Concept(uri) index when the matched value comes from an UNWIND row
    variable — `MATCH (s:Concept {uri: row.subject})` compiles to ScanAll.
    With 482k edges x 2 endpoint lookups x 291k nodes that is ~10^11
    comparisons (~20 hours). This version therefore:
      * creates nodes with CREATE (no existence check, no matching);
      * resolves URIs to internal ids with ONE full scan;
      * binds edge endpoints either by internal id (fast path) or by
        per-edge PARAMETERS (safe path — parameters are the documented
        indexed lookup form);
      * chooses between the two with a behavioral canary that measures
        which one the installed Memgraph actually accelerates.
    Memgraph has no index hints, so being planner-agnostic is the robust
    choice.

A priori contract (STRICT_CONTRACT = True):
    only edges whose realized (subject_type, object_type) class is
    permitted for their relation — per RELATION_TO_EDGE_CLASSES in
    label_concepts.py, precomputed as the CSV `permitted` column — are
    loaded. Violating rows remain recorded in typed_edges.csv and
    pipeline_stats.json. Set False in BOTH loaders for "faithful" mode.

Declared schema as data (LOAD_SCHEMA_META = True):
    (:Schema:NodeType), (:Schema:EdgeClass)-[:FROM|:TO]->(:Schema:NodeType),
    (:Schema:RelationType {wildcard})-[:PERMITS]->(:Schema:EdgeClass)
    — the contract stored as data, checked by the D1 query (the Cypher
    counterpart of SELECT COUNT(*) FROM v_edges WHERE NOT permitted).

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
RESET_FIRST              = True      # must stay True: this loader CREATES,
                                     # so it always wipes and rebuilds
CREATE_EDGE_TYPE_INDEXES = False
STRICT_CONTRACT          = True
LOAD_SCHEMA_META         = True

# "auto"  : run the plan canary and pick automatically (default)
# "id"    : force the fast path (UNWIND + WHERE id(n) = row.id)
# "param" : force the safe path (one parameterized query per edge)
EDGE_LOAD_STRATEGY = "auto"
CANARY_EDGES       = 200    # throwaway edges used to measure the planner
CANARY_THRESHOLD_S = 1.0    # seek answers in ms; a scan of 291k nodes per
                            # lookup takes seconds — 1s separates them

NODE_TYPES  = {"EntityNode", "ActionEventNode", "PropertyNode"}
RELATION_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")   # guards the f-strings

LETTER_TO_TYPE = {"E": "EntityNode", "A": "ActionEventNode",
                  "P": "PropertyNode"}
ALL_CLASSES = [f"{a}2{b}" for a in "EAP" for b in "EAP"]

# Post-load verification queries ---------------------------------------------

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
    # NOTE: creation failures are swallowed on purpose (version-tolerant),
    # so ALWAYS check that the "ok:" lines below actually printed "ok" —
    # a silently missing uri index is what turned a previous load into a
    # 20-hour scan fest.
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
    """
    CREATE, not MERGE: typed_nodes.csv is unique by uri (validated here;
    the :Concept(uri) uniqueness constraint is the engine-side safety
    net) and the graph was wiped in setup_schema, so no existence check
    is needed. Crucially, this means NO property matching at all during
    node loading — immune to the UNWIND/ScanAll planner issue.
    """
    seen = set()
    by_type = defaultdict(list)
    for n in nodes:
        if n["node_type"] not in NODE_TYPES:
            raise SystemExit(f"typed_nodes.csv: unknown node_type "
                             f"{n['node_type']!r} (URI {n['uri']})")
        if n["uri"] in seen:
            raise SystemExit(f"typed_nodes.csv: duplicate uri {n['uri']!r}")
        seen.add(n["uri"])
        by_type[n["node_type"]].append(n)

    for node_type, rows in sorted(by_type.items()):
        cypher = f"""
            UNWIND $rows AS row
            CREATE (c:Concept:{node_type} {{
                uri:   row.uri,
                name:  row.name,
                pos:   row.pos,
                label_source: row.label_source
            }})
        """
        t0, count = time.time(), 0
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i + BATCH_SIZE]
            session.run(cypher, rows=batch).consume()
            count += len(batch)
        print(f"  :{node_type:<15} {count:>9,} nodes   "
              f"({time.time() - t0:.1f}s)")


def build_uri_id_map(session):
    """
    ONE full scan of the already-loaded nodes: uri -> internal id.
    Internal ids are only used within this same load run (no restarts, no
    deletions in between — the canary below runs BEFORE this map is
    built), which is exactly the lifetime we need.
    """
    t0 = time.time()
    uri2id = {}
    result = session.run("MATCH (n:Concept) RETURN n.uri AS uri, id(n) AS nid")
    for rec in result:
        uri2id[rec["uri"]] = rec["nid"]
    print(f"  uri->id map: {len(uri2id):,} entries  "
          f"({time.time() - t0:.1f}s)")
    return uri2id


def choose_edge_strategy(session):
    """
    Decide how edges bind their endpoints — by MEASURING the planner.

    The canary runs after the nodes are loaded and times a small batch of
    id-bound edges between two throwaway nodes, using exactly the query
    form of the fast path:
        UNWIND $rows AS row
        MATCH (s) WHERE id(s) = row.sid
        MATCH (o) WHERE id(o) = row.oid
        CREATE (s)-[:RelatedTo {weight: row.w}]->(o)
    If `WHERE id(n) = row.id` compiles to a by-id seek, 200 edges answer
    in milliseconds; if it compiles to a scan, each of the 400 lookups
    visits all 291k nodes and the batch takes seconds. The measured time
    therefore picks the strategy — no reliance on operator names.
    """
    if EDGE_LOAD_STRATEGY != "auto":
        print(f"  edge strategy: forced '{EDGE_LOAD_STRATEGY}'")
        return EDGE_LOAD_STRATEGY

    session.run("CREATE (:_LoadCanary {k: 1}), (:_LoadCanary {k: 2})")
    try:
        ids = [rec["nid"] for rec in session.run(
            "MATCH (c:_LoadCanary) RETURN id(c) AS nid ORDER BY c.k")]
        if len(ids) != 2:
            raise SystemExit("canary: could not create probe nodes")
        rows = [{"sid": ids[0], "oid": ids[1], "w": 1.0}] * CANARY_EDGES
        query = ("UNWIND $rows AS row "
                 "MATCH (s) WHERE id(s) = row.sid "
                 "MATCH (o) WHERE id(o) = row.oid "
                 "CREATE (s)-[:RelatedTo {weight: row.w}]->(o)")
        t0 = time.time()
        session.run(query, rows=rows).consume()
        elapsed = time.time() - t0
    finally:
        session.run("MATCH (c:_LoadCanary) DETACH DELETE c")

    strategy = "id" if elapsed < CANARY_THRESHOLD_S else "param"
    verdict = ("by-id seek works — fast path" if strategy == "id" else
               "scan detected — safe path (parameterized lookups)")
    print(f"  plan canary: {CANARY_EDGES} id-bound edges in "
          f"{elapsed:.3f}s -> {verdict}")
    return strategy


def load_edges(session, edges, strategy):
    """
    Load typed_edges.csv as one edge type per relation, applying the same
    filters build_mysql.py applies (so the two stores stay isomorphic):
      * relation names must be Cypher-safe (RELATION_RE);
      * STRICT_CONTRACT: rows with permitted=0 are not loaded.
    Duplicate (subject, relation, object) triples keep the max weight —
    enforced in Python (the image of MySQL's ON DUPLICATE KEY UPDATE).
    Endpoint binding uses the chosen strategy; neither path performs a
    property match driven by an UNWIND row variable.
    """
    by_rel = defaultdict(list)
    skipped_name = skipped_contract = csv_permitted0 = 0
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
        by_rel[rel].append((e["subject"], e["object"], float(e["weight"])))

    if skipped_name:
        print(f"  [WARN] {skipped_name:,} edges with non-Cypher-safe "
              f"relation names skipped")
    if skipped_contract:
        print(f"  strict contract: {skipped_contract:,} violating edges "
              f"NOT loaded (recorded in pipeline_stats.json / "
              f"typed_edges.csv)")

    uri2id = None
    if strategy == "id":
        uri2id = build_uri_id_map(session)
        referenced = {u for rows in by_rel.values()
                        for (s, o, _w) in rows for u in (s, o)}
        missing = referenced - uri2id.keys()
        if missing:
            raise SystemExit(
                f"typed_edges.csv references {len(missing):,} uris absent "
                f"from typed_nodes.csv (e.g. {sorted(missing)[:3]})")

    eligible = 0
    unique_expected = 0
    for rel in sorted(by_rel):
        rows_in = by_rel[rel]
        eligible += len(rows_in)

        # duplicate triples: keep the max weight
        best = {}
        for s, o, w in rows_in:
            if (s, o) not in best or w > best[(s, o)]:
                best[(s, o)] = w
        pairs = [(s, o, w) for (s, o), w in best.items()]
        unique_expected += len(pairs)

        t0 = time.time()
        if strategy == "id":
            batch = [{"sid": uri2id[s], "oid": uri2id[o], "w": w}
                     for s, o, w in pairs]
            cypher = (f"UNWIND $rows AS row "
                      f"MATCH (s) WHERE id(s) = row.sid "
                      f"MATCH (o) WHERE id(o) = row.oid "
                      f"CREATE (s)-[:{rel} {{weight: row.w}}]->(o)")
            for i in range(0, len(batch), BATCH_SIZE):
                session.run(cypher, rows=batch[i:i + BATCH_SIZE]).consume()
        else:
            # parameters are the documented indexed-lookup form:
            # the planner picks the :Concept(uri) index for $su / $ou
            cypher = (f"MATCH (s:Concept {{uri: $su}}) "
                      f"MATCH (o:Concept {{uri: $ou}}) "
                      f"CREATE (s)-[:{rel} {{weight: $w}}]->(o)")
            for s, o, w in pairs:
                session.run(cypher, su=s, ou=o, w=w).consume()
        print(f"  {rel:<28} {len(pairs):>9,} edges  "
              f"({time.time() - t0:.1f}s)")

    return {
        "csv_total": len(edges),
        "eligible": eligible,
        "unique_expected": unique_expected,
        "skipped_name": skipped_name,
        "skipped_contract": skipped_contract,
        "csv_permitted0": csv_permitted0,
        "strategy": strategy,
    }


def load_schema_meta(session):
    """
    Store the a priori contract as data — the graph's answer to a
    'declared schema', which its engine cannot enforce:
        (:Schema:NodeType)             EntityNode / ActionEventNode / PropertyNode
        (:Schema:EdgeClass)-[:FROM|:TO]->(:Schema:NodeType)
        (:Schema:RelationType {wildcard})-[:PERMITS]->(:Schema:EdgeClass)
    ALL grants are expanded to the 9 concrete classes, mirroring
    relation_class_perms in conceptnet_schema.sql. (~50 nodes: matching
    here is trivially cheap even without indexes.)
    """
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
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
    e = session.run(
        "MATCH (:Concept)-[r]->(:Concept) RETURN count(r) AS c").single()["c"]
    print(f"  {'nodes (total)':<18} {n:>9,}")
    print(f"  {'edges (total)':<18} {e:>9,}")
    print(f"  {'edge load strategy':<18} {es.get('strategy', '?')}")
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
        if len(rec) == 1 and isinstance(next(iter(rec.values())), dict):
            info = next(iter(rec.values()))
            print(f"  {str(info.get('name', '?')):<32} {info.get('value', '?')}")
        else:
            print("  " + "  ".join(f"{k}={v}" for k, v in rec.items()))


# --- Main -----------------------------------------------------------------------

def main():
    t_start = time.time()

    if not RESET_FIRST:
        raise SystemExit(
            "This loader uses CREATE (no MERGE), so it must wipe and "
            "rebuild: RESET_FIRST has to stay True.")

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

    relations = sorted({e["relation"].strip() for e in edges
                        if RELATION_RE.match(e["relation"].strip())})
    driver = GraphDatabase.driver(MEMGRAPH_URI, auth=MEMGRAPH_AUTH)
    try:
        with driver.session() as session:
            print("\nSchema setup")
            setup_schema(session, relations)
            print("\nLoading nodes")
            load_nodes(session, nodes)
            print("\nEdge strategy (plan canary)")
            strategy = choose_edge_strategy(session)
            print("\nLoading edges")
            es = load_edges(session, edges, strategy)
            if LOAD_SCHEMA_META:
                print("\nSchema meta-graph (declared contract as data)")
                load_schema_meta(session)
            report(session, es, load_pipeline_stats())
    finally:
        driver.close()
    print(f"\nDone. ({time.time() - t_start:.1f}s total)")


if __name__ == "__main__":
    main()