#!/usr/bin/env python3
"""
benchmark.py — execute the FINAL query battery (Queries.pdf) on MySQL and
Memgraph and emit the comparison analytics for the report.

Battery — exactly the 18 queries of the final list, same numbering:

FAMILY A (Local Retrieval)
    A1  Single Node Search by URI          (equality on the unique key — baseline)
    A2  Full Relation Table Search         (1-hop fan-out + tuple reassembly)
    A3  Relation 1-hop Search              (discriminated access)
    A4a Directed (reverse) 1-hop Search    (second index vs. free adjacency)
    A4b Undirected 1-hop Search            (OR predicate vs. one pattern)
    A5  Relation Pair Intersection         (join topology in the pattern)
    A6  Range Selection                    (sorted-index range vs. filter)
    A7  Neighborhoods Set Difference       (anti-join vs. negated pattern)
FAMILY B (Traversal Paths)
    B1  K-Hop Neighborhood, k=1..4         (the latency-vs-depth headline)
    B2  Transitive Closure                 (recursive CTE vs. *1..d, depth 5)
    B3  Concepts Pair Shortest Chain       (depth-bounded — see SP note)
    B4  Weighted Path Scoring              (Cypher-only, expressiveness boundary)
    B5  Anchored Triangle                  (cyclic pattern)
    B6  Neighborhood Degree Profile       (traversal feeding aggregation)
FAMILY C (Global Retrieval)
    C1  Concept Hubs Ranking               (full scan + one-pass grouping)
    C2  Relation-ClassType Matrix          (stored vs. derived grouping key)
FAMILY D (Schema and Integrity Findings)
    D1  Violation Check                    (view vs. meta-graph anti-join)
    D2  Invalid Insert                     — MANUAL write demo (read-only harness)
    D3  Concept Delete                     — MANUAL write demo (read-only harness)

DELIBERATE DEVIATIONS from the PDF texts (each forced by correctness):
    * ORDER BY added before LIMIT in B1/B5/B6 — LIMIT without ORDER BY is
      nondeterministic and could return different rows per system, which
      would break cross-system verification.
    * A1 drops the id(n)/node_id column: engine ids are non-isomorphic by
      design (MySQL = load order, Memgraph = storage-assigned); the URI
      is the identity, so it is the comparable key.
    * B3 returns the hop count, not the path: a path is a sequence of
      engine-internal ids (non-comparable); its length is the isomorphic
      projection.
    * B3's Cypher uses Memgraph's BFS expansion (*BFS..d); if this build
      rejects the syntax, replace with *..d (same semantics, more paths
      enumerated).

SHORTEST-PATH DEPTH (SP_MAX_DEPTH, the '3-hop constant'): undirected,
all-relation variable-length traversal enumerates walks exponentially in
depth — ConceptNet's hubs multiply the frontier, the outer LIMIT cannot
prune MySQL's recursive CTE (it is materialized fully first), and
Memgraph's plain *..d enumerates all paths too. Depth 5 had to be killed
after ~30 minutes and the memory blow-up froze the Docker VM. Hence ONE
constant for both B3 and B4, default 3, overridable with --sp-depth.
WHATEVER DEPTH YOU RUN: (a) verify the anchor pair is connected at that
depth (inspect_query.py B3 must return >= 1 row, else the query
benchmarks nothing), and (b) update Queries.pdf to match — the proposal
must describe the executed experiment.

Protocol: per query, WARMUP discarded runs + REPS measured runs
(per-query overrides); median / min / max / mean / stdev reported.
Results are verified across systems by canonicalized row-set hash.
Plans (EXPLAIN ANALYZE / PROFILE) are captured for PLAN_QUERIES.

TEMPLATE RULE: query texts are {{token}} templates substituted by
QuerySpec.resolve(); they must be built as PLAIN strings, never
f-strings (inside an f-string, {{token}} collapses to {token} and
resolve() silently substitutes nothing). resolve() aborts on any
leftover {identifier} as a permanent guard.

Usage
    python benchmark.py                     # full battery, both systems
    python benchmark.py --quick             # smoke test (3 reps, 1 warm-up)
    python benchmark.py --queries A2,B1k4   # subset
    python benchmark.py --sp-depth 4        # B3/B4 depth override (careful!)
    python benchmark.py --mysql-only | --memgraph-only
    python benchmark.py --stratify-b1       # B1 over low/med/high-degree anchors
    python benchmark.py --no-plans --reps 20 --out-dir results/bench1

Outputs (in --out-dir, default results/):
    benchmark_results.json   raw per-rep timings + summary (incremental)
    benchmark_results.csv    one row per (query, system)
    benchmark_report.md      the report table, ready to paste
    plans/<qid>_<system>.txt physical plans for the key queries

Requires: mysql-connector-python, neo4j (same drivers as the loaders).
"""

import argparse
import csv as csvmod
import datetime
import decimal
import hashlib
import json
import os
import platform
import re
import statistics
import sys
import time
from dataclasses import dataclass, field

try:
    import mysql.connector
except ImportError:
    print("pip install mysql-connector-python", file=sys.stderr)
    sys.exit(1)

try:
    from neo4j import GraphDatabase
    from neo4j.exceptions import Neo4jError
except ImportError:
    print("pip install neo4j", file=sys.stderr)
    sys.exit(1)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MYSQL_CONFIG = {
    "host":     os.environ.get("MYSQL_HOST", "localhost"),
    "port":     int(os.environ.get("MYSQL_PORT", "3306")),
    "user":     os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", "rootpassword"),
    "database": "conceptnet",
    "charset":  "utf8mb4",
}

MEMGRAPH_URI  = os.environ.get("MEMGRAPH_URI", "bolt://localhost:7687")
MEMGRAPH_AUTH = ("", "")

REPS   = 10      # measured repetitions (median reported)
WARMUP = 2       # discarded repetitions

OUT_DIR = "results"

# Anchors — curated; existence is verified at startup (URIs against
# nodes.uri, names against nodes.name), and queries whose anchors are
# missing are skipped with a recorded reason.
# !! A query where BOTH systems return 0 rows verifies OK but benchmarks
# !! NOTHING — the harness warns. From the first quick run, A3/A4b/A5/A7
# !! anchors were dead in this slice: run the anchor-finding queries in
# !! the project notes and substitute before the final run.
ANCHORS = {
    # A1 — URI anchor (identity-level lookup, per the final query list)
    "a1_uri": "/c/en/photosynthesis/n",
    # name anchors
    "a2":  "photosynthesis",
    "a3":  "cell",
    "a4a": "laboratory",
    "a4b": "abundant",
    "a5a": "microscope", "a5b": "beaker",
    "a6lo": "cell", "a6hi": "gene",
    "a7a": "enzyme", "a7b": "cell",
    "b1":  "enzyme",
    "b2":  "mitochondrion",
    "b3a": "chlorophyll", "b3b": "sunlight",
    "b4a": "chlorophyll", "b4b": "sunlight",
    "b5":  "enzyme",
    "b6":  "enzyme",
    # relation names (for the Cypher edge types)
    "a3_rel": "CapableOf",
    "a4a_rel": "AtLocation", "a4b_rel": "Antonym",
    "a5_rel": "AtLocation",
    "a7_rel": "AtLocation",
    "b2_rel": "IsA",
}

# Relation ids (must match conceptnet_schema.sql)
REL_IDS = {"Antonym": 1, "AtLocation": 2, "CapableOf": 3, "DefinedAs": 7,
           "HasProperty": 20, "IsA": 23, "PartOf": 31, "RelatedTo": 33,
           "UsedFor": 37}

MIN_W = 0.5        # weight filter for B1

B2_MAX_DEPTH = 5   # transitive closure: single relation -> bounded fan-out
SP_MAX_DEPTH = 3   # shortest path family (B3/B4): all relations ->
                   # EXPONENTIAL fan-out; see the docstring incident note.
                   # Override with --sp-depth; update Queries.pdf to match.

# Queries whose physical plans are captured into plans/
PLAN_QUERIES = {"A2", "A4b", "A6", "A7", "B1k2", "B1k4", "B5", "B6",
                "C1", "C2"}

# Degree pool used when --stratify-b1 is given
B1_STRAT_POOL = ["enzyme", "cell", "gene", "protein", "animal",
                 "plant", "water", "organism", "blood", "dna", "food"]

# Leftover single-brace token in a resolved query — the f-string bug
# detector. Legitimate Cypher property maps ('{name: ...}') contain a
# colon and therefore never match this pattern.
LEFTOVER_RE = re.compile(r"\{[a-z_][a-z0-9_]*\}")


# ---------------------------------------------------------------------------
# Query specifications
# ---------------------------------------------------------------------------

@dataclass
class QuerySpec:
    qid: str
    family: str
    mechanism: str
    expected: str
    sql: str = None
    cypher: str = None
    params: dict = field(default_factory=dict)
    reps: int = 0      # 0 = use the global protocol value (resolved in
    warmup: int = 0    # build_specs AFTER --quick/--reps are applied; a
                       # literal default here would freeze at import time)
    verify: bool = True
    note: str = ""
    plan: bool = False

    def resolve(self, text):
        out = text
        for key, val in self.params.items():
            out = out.replace("{{" + key + "}}", str(val))
        if "{{" in out:
            raise SystemExit(f"{self.qid}: unresolved double-brace token "
                             f"in query text")
        m = LEFTOVER_RE.search(out)
        if m:
            raise SystemExit(
                f"{self.qid}: leftover token {m.group(0)!r} after "
                f"resolution — the template was probably built with an "
                f"f-string, whose {{x}} escapes collapse to single braces "
                f"that resolve() cannot substitute")
        return out


def b1_pair(k):
    """
    Generate the k-hop SQL and Cypher TEMPLATES for a given k.

    Built with plain concatenation on purpose: the outputs must contain
    the double-brace tokens ({{anchor}}, {{minw}}) that
    QuerySpec.resolve() substitutes. Inside an f-string, {{minw}}
    renders to {minw}, resolve() silently replaces nothing, and the
    engine receives literal brace junk.
    ORDER BY name is added before LIMIT so the 100-row window is
    deterministic and therefore comparable across systems.
    """
    joins = ["JOIN edges e1 ON e1.subject_id = n.node_id"]
    wfilt, cw = [], []
    for i in range(1, k + 1):
        if i > 1:
            joins.append("JOIN edges e" + str(i) + " ON e" + str(i)
                         + ".subject_id = e" + str(i - 1) + ".object_id")
        wfilt.append("e" + str(i) + ".weight >= {{minw}}")
        cw.append("r" + str(i) + ".weight >= {{minw}}")
    chain = "".join("-[r" + str(i) + "]->()" for i in range(1, k)) \
        + "-[r" + str(k) + "]->(x:Concept)"
    sql = ("SELECT DISTINCT x.name AS name\n"
           "FROM nodes n\n"
           + "\n".join(joins) + "\n"
           + "JOIN nodes x ON x.node_id = e" + str(k) + ".object_id\n"
           + "WHERE n.name = '{{anchor}}' AND " + " AND ".join(wfilt) + "\n"
           + "ORDER BY name LIMIT 100\n")
    cypher = ("MATCH (n:Concept {name: '{{anchor}}'})" + chain + "\n"
              "WHERE " + " AND ".join(cw) + "\n"
              "RETURN DISTINCT x.name AS name\n"
              "ORDER BY name LIMIT 100")
    return sql, cypher


NODE_TYPE_CASE = ("CASE WHEN n:EntityNode THEN 'EntityNode' "
                  "WHEN n:ActionEventNode THEN 'ActionEventNode' "
                  "ELSE 'PropertyNode' END")


def build_specs(stratify_b1=False, b1_anchors=None):
    p = ANCHORS
    specs = [
        # ---------------- FAMILY A — local retrieval ----------------------
        QuerySpec(
            "A1", "A", "equality selection on the unique key (baseline)", "tie",
            sql="""SELECT uri, name, node_type, label_source
FROM nodes WHERE uri = '{{anchor_uri}}'""",
            cypher="""MATCH (n:Concept {uri: '{{anchor_uri}}'})
RETURN n.uri AS uri, n.name AS name,
       {{node_type_case}} AS node_type, n.label_source AS label_source""",
            params={"anchor_uri": p["a1_uri"],
                    "node_type_case": NODE_TYPE_CASE},
            note="PDF also selects id(n)/node_id; dropped because engine "
                 "ids are non-isomorphic by design (MySQL = load order, "
                 "Memgraph = storage-assigned) — the URI is the identity"),

        QuerySpec(
            "A2", "A", "1-hop fan-out + tuple reassembly (join-backs)", "slight graph",
            sql="""SELECT s.name AS subject, r.relation_name AS relation,
       o.name AS object, e.weight AS weight
FROM nodes s
JOIN edges e     ON e.subject_id = s.node_id
JOIN nodes o     ON o.node_id = e.object_id
JOIN relations r ON r.relation_id = e.relation_id
WHERE s.name = '{{anchor}}'
ORDER BY weight DESC, relation, object LIMIT 25""",
            cypher="""MATCH (n:Concept {name: '{{anchor}}'})-[r]->(m:Concept)
RETURN n.name AS subject, type(r) AS relation, m.name AS object, r.weight AS weight
ORDER BY weight DESC, relation, object LIMIT 25""",
            params={"anchor": p["a2"]}, plan=True),

        QuerySpec(
            "A3", "A", "discriminated access (relation id vs. edge type)", "tie",
            sql="""SELECT o.name AS action
FROM nodes s
JOIN edges e ON e.subject_id = s.node_id
JOIN nodes o ON o.node_id = e.object_id
WHERE s.name = '{{anchor}}' AND e.relation_id = {{rel}}
ORDER BY action""",
            cypher="""MATCH (:Concept {name: '{{anchor}}'})-[:{{rel_name}}]->(a:ActionEventNode)
RETURN a.name AS action
ORDER BY action""",
            params={"anchor": p["a3"], "rel": REL_IDS["CapableOf"],
                    "rel_name": p["a3_rel"]}),

        QuerySpec(
            "A4a", "A", "directed (reverse) 1-hop: second index vs. free adjacency", "tie",
            sql="""SELECT s.name AS thing
FROM nodes o
JOIN edges e ON e.object_id = o.node_id
JOIN nodes s ON s.node_id = e.subject_id
WHERE o.name = '{{anchor}}' AND e.relation_id = {{rel}}
ORDER BY thing""",
            cypher="""MATCH (thing:Concept)-[:{{rel_name}}]->(:Concept {name: '{{anchor}}'})
RETURN thing.name AS thing
ORDER BY thing""",
            params={"anchor": p["a4a"], "rel": REL_IDS["AtLocation"],
                    "rel_name": p["a4a_rel"]}),

        QuerySpec(
            "A4b", "A", "undirected 1-hop: OR predicate vs. one pattern", "graph",
            sql="""SELECT CASE WHEN e.subject_id = n.node_id THEN o.name ELSE s.name END AS neighbor
FROM nodes n
JOIN edges e ON (e.subject_id = n.node_id OR e.object_id = n.node_id)
JOIN nodes s ON s.node_id = e.subject_id
JOIN nodes o ON o.node_id = e.object_id
WHERE n.name = '{{anchor}}' AND e.relation_id = {{rel}}
ORDER BY neighbor""",
            cypher="""MATCH (:Concept {name: '{{anchor}}'})-[:{{rel_name}}]-(y:Concept)
RETURN y.name AS neighbor
ORDER BY neighbor""",
            params={"anchor": p["a4b"], "rel": REL_IDS["Antonym"],
                    "rel_name": p["a4b_rel"]},
            note="ConceptNet stores symmetric relations reciprocally", plan=True),

        QuerySpec(
            "A5", "A", "two-anchor intersection (join topology in the pattern)", "tie (readability: graph)",
            sql="""SELECT loc.name AS shared
FROM nodes a
JOIN edges e1 ON e1.subject_id = a.node_id AND e1.relation_id = {{rel}}
JOIN edges e2 ON e2.object_id = e1.object_id AND e2.relation_id = {{rel}}
JOIN nodes b  ON b.node_id = e2.subject_id AND b.name = '{{anchor_b}}'
JOIN nodes loc ON loc.node_id = e1.object_id
WHERE a.name = '{{anchor_a}}'
ORDER BY shared""",
            cypher="""MATCH (a:Concept {name: '{{anchor_a}}'})-[:{{rel_name}}]->(loc:Concept)
      <-[:{{rel_name}}]-(b:Concept {name: '{{anchor_b}}'})
RETURN loc.name AS shared
ORDER BY shared""",
            params={"anchor_a": p["a5a"], "anchor_b": p["a5b"],
                    "rel": REL_IDS["AtLocation"], "rel_name": p["a5_rel"]}),

        QuerySpec(
            "A6", "A", "range selection on a sorted index", "MySQL",
            sql="""SELECT name, node_type FROM nodes
WHERE name BETWEEN '{{lo}}' AND '{{hi}}'
ORDER BY name""",
            cypher="""MATCH (n:Concept)
WHERE n.name >= '{{lo}}' AND n.name <= '{{hi}}'
RETURN n.name AS name, {{node_type_case}} AS node_type
ORDER BY name""",
            params={"lo": p["a6lo"], "hi": p["a6hi"],
                    "node_type_case": NODE_TYPE_CASE},
            note="B+ tree: O(log B + fs*B); check PROFILE for index-vs-scan",
            plan=True),

        QuerySpec(
            "A7", "A", "set difference (anti-join vs. negated pattern)", "near tie (readability: graph)",
            sql="""SELECT DISTINCT s.name AS name
FROM edges e
JOIN nodes s ON s.node_id = e.subject_id
JOIN nodes a ON a.node_id = e.object_id
WHERE a.name = '{{anchor_a}}' AND e.relation_id = {{rel}}
  AND NOT EXISTS (
      SELECT 1 FROM edges e2
      JOIN nodes b ON b.node_id = e2.object_id
      WHERE e2.subject_id = e.subject_id AND e2.relation_id = {{rel}}
        AND b.name = '{{anchor_b}}')
ORDER BY name""",
            cypher="""MATCH (x:Concept)-[:{{rel_name}}]->(:Concept {name: '{{anchor_a}}'})
WHERE NOT (x)-[:{{rel_name}}]->(:Concept {name: '{{anchor_b}}'})
RETURN DISTINCT x.name AS name
ORDER BY name""",
            params={"anchor_a": p["a7a"], "anchor_b": p["a7b"],
                    "rel": REL_IDS["AtLocation"], "rel_name": p["a7_rel"]},
            note="PDF form 2 (anti-join) — better for plan inspection; "
                 "form 1 (EXCEPT) is equivalent", plan=True),
    ]

    # ---------------- FAMILY B — traversal paths --------------------------
    b1_list = b1_anchors if (stratify_b1 and b1_anchors) else [p["b1"]]
    for anchor in b1_list:
        base = "B1" if len(b1_list) == 1 else f"B1[{anchor}]"
        for k in (1, 2, 3, 4):
            sql, cypher = b1_pair(k)
            specs.append(QuerySpec(
                f"{base}k{k}", "B",
                f"{k}-hop expansion (index-join chain vs. traversal)",
                "graph, gap grows with k" if k > 1 else "near tie",
                sql=sql, cypher=cypher,
                params={"anchor": anchor, "minw": MIN_W},
                plan=(k in (2, 4))))

    specs += [
        QuerySpec(
            "B2", "B", "transitive closure (recursive CTE vs. *1..d)", "graph, modest",
            sql="""WITH RECURSIVE anc(node_id, depth) AS (
    SELECT node_id, 0 FROM nodes WHERE name = '{{anchor}}'
    UNION ALL
    SELECT e.object_id, a.depth + 1
    FROM anc a JOIN edges e ON e.subject_id = a.node_id
    WHERE e.relation_id = {{rel}} AND a.depth < {{maxd}}
)
SELECT DISTINCT n.name AS superclass
FROM anc a JOIN nodes n ON n.node_id = a.node_id
WHERE a.depth > 0
ORDER BY superclass""",
            cypher="""MATCH (:Concept {name: '{{anchor}}'})-[:{{rel_name}}*1..{{maxd}}]->(sup:Concept)
RETURN DISTINCT sup.name AS superclass
ORDER BY superclass""",
            params={"anchor": p["b2"], "rel": REL_IDS["IsA"],
                    "rel_name": p["b2_rel"], "maxd": B2_MAX_DEPTH},
            note="CTE needs the manual depth bound as cycle guard"),

        QuerySpec(
            "B3", "B", "shortest chain, undirected, any relation (depth-bounded)", "graph (expressiveness)",
            sql="""WITH RECURSIVE walk(node_id, depth, path) AS (
    SELECT node_id, 0, CAST(node_id AS CHAR(2000))
    FROM nodes WHERE name = '{{anchor_a}}'
    UNION ALL
    SELECT e.other_id, w.depth + 1, CONCAT(w.path, ',', e.other_id)
    FROM walk w
    JOIN ( SELECT subject_id AS from_id, object_id AS other_id FROM edges
           UNION ALL
           SELECT object_id AS from_id, subject_id AS other_id FROM edges ) e
      ON e.from_id = w.node_id
    WHERE w.depth < {{maxd}} AND FIND_IN_SET(e.other_id, w.path) = 0
)
SELECT depth AS hops FROM walk
WHERE node_id IN (SELECT node_id FROM nodes WHERE name = '{{anchor_b}}')
ORDER BY depth, path LIMIT 1""",
            cypher="""MATCH p = (a:Concept {name: '{{anchor_a}}'})-[r*BFS..{{maxd}}]-(b:Concept {name: '{{anchor_b}}'})
RETURN length(p) AS hops
ORDER BY hops LIMIT 1""",
            params={"anchor_a": p["b3a"], "anchor_b": p["b3b"],
                    "maxd": SP_MAX_DEPTH},
            reps=3, warmup=1,
            note="returns hops, not the path (paths contain engine ids); "
                 "*BFS explores level by level (if this build rejects "
                 "*BFS, use *..{{maxd}}); depth is SP_MAX_DEPTH — keep "
                 "Queries.pdf in sync with whatever you run"),

        QuerySpec(
            "B4", "B", "weighted path scoring — expressiveness boundary", "no SQL counterpart",
            sql=None,
            cypher="""MATCH p = (a:Concept {name: '{{anchor_a}}'})-[r*..{{maxd}}]->(b:Concept {name: '{{anchor_b}}'})
RETURN p, reduce(acc = 1.0, rel IN relationships(p) | acc * rel.weight) AS score
ORDER BY score DESC LIMIT 5""",
            params={"anchor_a": p["b4a"], "anchor_b": p["b4b"],
                    "maxd": SP_MAX_DEPTH},
            reps=3, warmup=1, verify=False,
            note="reduce() over native path objects; depth-bounded for "
                 "the same exponential reason as B3"),

        QuerySpec(
            "B5", "B", "anchored triangle (cyclic pattern)", "graph, clearly",
            sql="""SELECT DISTINCT y.name AS hop1, z.name AS hop2
FROM nodes n
JOIN edges e1 ON e1.subject_id = n.node_id
JOIN edges e2 ON e2.subject_id = e1.object_id
JOIN edges e3 ON e3.subject_id = e2.object_id
WHERE n.name = '{{anchor}}' AND e3.object_id = n.node_id
ORDER BY hop1, hop2 LIMIT 200""",
            cypher="""MATCH (a:Concept {name: '{{anchor}}'})-[]->(y:Concept)-[]->(z:Concept)-[]->(a)
RETURN DISTINCT y.name AS hop1, z.name AS hop2
ORDER BY hop1, hop2 LIMIT 200""",
            params={"anchor": p["b5"]}, reps=5,
            note="hub-sensitive: mid-degree anchor; LIMIT 200 + ORDER BY "
                 "added to the PDF text for determinism and safety",
            plan=True),

        QuerySpec(
            "B6", "B", "traversal feeding aggregation (bags vs sets!)", "graph, moderate",
            sql="""SELECT nbr.name AS name, COUNT(DISTINCT e2.edge_id) AS out_degree
FROM nodes n
JOIN edges e1 ON e1.subject_id = n.node_id
JOIN edges e2 ON e2.subject_id = e1.object_id
JOIN nodes nbr ON nbr.node_id = e1.object_id
WHERE n.name = '{{anchor}}'
GROUP BY nbr.node_id, nbr.name
ORDER BY out_degree DESC, name LIMIT 50""",
            cypher="""MATCH (:Concept {name: '{{anchor}}'})-[]->(nbr:Concept)-[r2]->()
RETURN nbr.name AS name, count(DISTINCT r2) AS out_degree
ORDER BY out_degree DESC, name LIMIT 50""",
            params={"anchor": p["b6"]},
            note="COUNT(DISTINCT) on both sides: the anchor->neighbor "
                 "join is a bag; LIMIT 50 added for safety",
            plan=True),

        # ---------------- FAMILY C — global retrieval ----------------------
        QuerySpec(
            "C1", "C", "hub ranking (full scan + one-pass grouping)", "MySQL",
            sql="""SELECT n.uri, n.name, COUNT(*) AS out_degree
FROM edges e JOIN nodes n ON n.node_id = e.subject_id
GROUP BY n.uri, n.name
ORDER BY out_degree DESC LIMIT 10;""",
            cypher="""MATCH (n:Concept)-[r]->(:Concept)
RETURN n.uri AS uri, n.name AS name, count(r) AS out_degree
ORDER BY out_degree DESC, uri LIMIT 10;""",
            plan=True, note="the honest pro-SQL row"),

        QuerySpec(
            "C2", "C", "stored vs. derived grouping key (relation x class)", "either — trade-off is the finding",
            sql="""SELECT r.relation_name AS relation, e.edge_class AS edge_class,
       COUNT(*) AS n_edges, ROUND(AVG(e.weight), 4) AS avg_w
FROM edges e JOIN relations r ON r.relation_id = e.relation_id
GROUP BY r.relation_name, e.edge_class
ORDER BY n_edges DESC, relation, edge_class""",
            cypher="""MATCH (s:Concept)-[r]->(o:Concept)
RETURN type(r) AS relation,
       (CASE WHEN s:EntityNode THEN 'E' WHEN s:ActionEventNode THEN 'A' ELSE 'P' END) + '2' +
       (CASE WHEN o:EntityNode THEN 'E' WHEN o:ActionEventNode THEN 'A' ELSE 'P' END) AS edge_class,
       count(r) AS n_edges, round(avg(r.weight), 4) AS avg_w
ORDER BY n_edges DESC, relation, edge_class""",
            plan=True, note="output = the table behind Lab's schema view"),

        # ---------------- FAMILY D — schema and integrity -----------------
        QuerySpec(
            "D1", "D", "violation check (view vs. meta-graph anti-join)", "qualitative",
            sql="""SELECT COUNT(*) AS violations FROM v_edges WHERE NOT permitted""",
            cypher="""MATCH (s:Concept)-[r]->(o:Concept)
MATCH (rt:Schema:RelationType {name: type(r)})
OPTIONAL MATCH (rt)-[:PERMITS]->(c:Schema:EdgeClass)
    WHERE c.name = (CASE WHEN s:EntityNode THEN 'E' WHEN s:ActionEventNode THEN 'A' ELSE 'P' END) + '2' +
               (CASE WHEN o:EntityNode THEN 'E' WHEN o:ActionEventNode THEN 'A' ELSE 'P' END)
WITH r, c WHERE c IS NULL
RETURN count(r) AS violations""",
            note="expect 0 on both sides under STRICT_CONTRACT; D2/D3 are "
                 "manual write demos by design", plan=False),
    ]

    # resolve the protocol sentinels AFTER --quick/--reps have been applied
    for s in specs:
        if not s.reps:
            s.reps = REPS
        if not s.warmup:
            s.warmup = WARMUP
    return specs


# ---------------------------------------------------------------------------
# Runners
# ---------------------------------------------------------------------------

def run_mysql(conn, sql):
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    return [tuple(r) for r in rows]


def run_memgraph(session, cypher):
    return [tuple(rec.values()) for rec in session.run(cypher)]


def bench(run_fn, reps, warmup):
    for _ in range(warmup):
        run_fn()
    times, rows = [], None
    for _ in range(reps):
        t0 = time.perf_counter()
        rows = run_fn()
        times.append((time.perf_counter() - t0) * 1000.0)  # ms
    return times, rows


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def canon_value(v):
    if isinstance(v, decimal.Decimal):
        v = float(v)
    if v is None:
        return ""
    if isinstance(v, bool):
        return str(v)
    if isinstance(v, float):
        return f"{round(v, 4):g}"
    return str(v)


def canon_rows(rows):
    return sorted(tuple(canon_value(v) for v in row) for row in rows)


def rows_hash(rows):
    payload = json.dumps(canon_rows(rows), ensure_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Plan capture
# ---------------------------------------------------------------------------

def mysql_plan(conn, sql):
    try:
        cur = conn.cursor()
        cur.execute("EXPLAIN ANALYZE " + sql)
        text = "\n".join(str(r[0]) for r in cur.fetchall())
        cur.close()
        return text
    except mysql.connector.Error as exc:
        return f"(EXPLAIN ANALYZE failed: {exc})"


def render_cypher_plan(node, depth=0):
    op = None
    args = None
    children = []
    if isinstance(node, dict):
        op = node.get("operator_type") or node.get("operator") or str(node)
        args = node.get("arguments")
        children = node.get("children", [])
    else:
        op = getattr(node, "operator_type", None) or getattr(node, "operator_type_", None)
        args = getattr(node, "arguments", None)
        children = getattr(node, "children", None) or []
    lines = ["  " * depth + f"* {op}" + (f"  {dict(args)}" if args else "")]
    for ch in children:
        lines.extend(render_cypher_plan(ch, depth + 1))
    return lines


def memgraph_plan(session, cypher):
    try:
        result = session.run("PROFILE " + cypher)
        summary = result.consume()
        plan = getattr(summary, "profile", None) or getattr(summary, "plan", None)
        if plan is None:
            return "(driver returned no plan object)"
        return "\n".join(render_cypher_plan(plan))
    except Neo4jError as exc:
        return f"(PROFILE failed: {exc})"


def plan_note(text):
    """Heuristic flags — the 'check the physical plan' discipline."""
    if not text or text.startswith("("):
        return ""
    t = text.lower()
    notes = []
    if "scanall" in t:
        notes.append("ScanAll(!)")
    if re.search(r"\bindex", t):
        notes.append("index")
    for token in ("eq_ref", "range", "ref", "full scan", "hash join"):
        if token in t:
            notes.append(token)
    return ",".join(dict.fromkeys(notes))[:60]


# ---------------------------------------------------------------------------
# Environment / anchors / storage
# ---------------------------------------------------------------------------

def json_default(o):
    """JSON fallback: MySQL DECIMAL -> float, everything exotic -> str."""
    if isinstance(o, decimal.Decimal):
        return float(o)
    return str(o)


def collect_env(conn, session):
    env = {
        "date": datetime.datetime.now().isoformat(timespec="seconds"),
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpus": os.cpu_count(),
        "reps": REPS, "warmup": WARMUP,
        "sp_depth": SP_MAX_DEPTH,
    }
    try:
        cur = conn.cursor()
        cur.execute("SELECT VERSION(), @@innodb_buffer_pool_size")
        v, bp = cur.fetchone()
        env["mysql_version"] = v
        env["innodb_buffer_pool_mb"] = round(bp / 1024 / 1024)
        cur.close()
    except Exception as exc:
        env["mysql_version"] = f"(unavailable: {exc})"
    try:
        info = {r["name"]: r["value"] for r in
                session.run("CALL mg.info() YIELD name, value "
                            "RETURN name, value")}
        env["memgraph_version"] = info.get("version", "?")
    except Exception as exc:
        env["memgraph_version"] = f"(unavailable: {exc})"
    # the index that caused the 21h incident — logged on every benchmark run
    try:
        env["memgraph_indexes"] = [
            dict(r) for r in session.run("SHOW INDEX INFO")]
    except Exception:
        env["memgraph_indexes"] = "(unavailable)"
    return env


def anchor_prep(conn):
    """
    Existence + out-degree of every anchor used by the battery.
    URI anchors (values starting with '/c/') are checked against
    nodes.uri; name anchors against nodes.name. Out-degrees are computed
    for names only (they feed --stratify-b1 and the report's degree table).
    """
    values = sorted({v for k, v in ANCHORS.items()
                     if not k.endswith("_rel")})
    uris = [v for v in values if v.startswith("/c/")]
    names = [v for v in values if not v.startswith("/c/")]
    cur = conn.cursor()
    present = set()
    if uris:
        fmt = ",".join(["%s"] * len(uris))
        cur.execute(f"SELECT uri FROM nodes WHERE uri IN ({fmt})", uris)
        present |= {r[0] for r in cur.fetchall()}
    degrees = {}
    if names:
        fmt = ",".join(["%s"] * len(names))
        cur.execute(f"SELECT name FROM nodes WHERE name IN ({fmt})", names)
        present |= {r[0] for r in cur.fetchall()}
        cur.execute(f"""SELECT n.name, COUNT(*) FROM edges e
                        JOIN nodes n ON n.node_id = e.subject_id
                        WHERE n.name IN ({fmt}) GROUP BY n.name""", names)
        degrees = dict(cur.fetchall())
    cur.close()
    missing = [v for v in values if v not in present]
    return present, degrees, missing


def stratified_anchors(degrees):
    pool = [a for a in B1_STRAT_POOL if a in degrees]
    pool.sort(key=lambda a: degrees[a])
    if len(pool) < 3:
        return None
    return [pool[0], pool[len(pool) // 2], pool[-1]]


def collect_storage(conn, session):
    out = {}
    try:
        cur = conn.cursor()
        cur.execute("""SELECT table_name, ROUND(data_length/1024/1024,1),
                              ROUND(index_length/1024/1024,1)
                       FROM information_schema.tables
                       WHERE table_schema = %s
                         AND table_name IN ('nodes','edges')""", ("conceptnet",))
        out["mysql_mb"] = {
            r[0]: {"data": float(r[1]) if r[1] is not None else None,
                   "index": float(r[2]) if r[2] is not None else None}
            for r in cur.fetchall()}
        cur.close()
    except Exception as exc:
        out["mysql_mb"] = f"(unavailable: {exc})"
    try:
        out["memgraph_storage_info"] = [
            dict(r) for r in session.run("SHOW STORAGE INFO")]
    except Exception as exc:
        out["memgraph_storage_info"] = f"(unavailable: {exc})"
    return out


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def summary_stats(times):
    return {
        "median_ms": round(statistics.median(times), 3),
        "min_ms": round(min(times), 3),
        "max_ms": round(max(times), 3),
        "mean_ms": round(statistics.fmean(times), 3),
        "stdev_ms": round(statistics.stdev(times), 3) if len(times) > 1 else 0.0,
        "reps": len(times),
    }


def loc_of(text):
    return len([l for l in text.strip().splitlines() if l.strip()])


def fmt_ms(v):
    return f"{v:.2f}" if v is not None else "—"


def winner_of(sql_ms, cy_ms, sql_err=False, cy_err=False):
    if sql_err and cy_err:
        return "ERROR (both)"
    if sql_err:
        return "graph (SQL failed)"
    if cy_err:
        return "sql (Cypher failed)"
    if sql_ms is None:
        return "graph (no SQL counterpart)"
    if cy_ms is None:
        return "sql"
    ratio = cy_ms / sql_ms if sql_ms > 0 else float("inf")
    if 1 / 1.15 < ratio < 1.15:
        return "tie"
    return "graph" if ratio < 1 else "sql"


def write_outputs(out_dir, results, specs, env, storage, anchors_info):
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(os.path.join(out_dir, "plans"), exist_ok=True)

    # ---- raw + summary JSON (also used for incremental saves) ----
    with open(os.path.join(out_dir, "benchmark_results.json"), "w",
              encoding="utf-8") as f:
        json.dump({"environment": env, "anchors": anchors_info,
                   "storage": storage, "results": results}, f,
                  indent=2, ensure_ascii=False, default=json_default)

    # ---- CSV ----
    cols = ["query", "family", "mechanism", "expected", "system", "anchor",
            "median_ms", "min_ms", "max_ms", "mean_ms", "stdev_ms", "reps",
            "rows", "loc", "chars", "verify", "plan_note", "error"]
    with open(os.path.join(out_dir, "benchmark_results.csv"), "w",
              newline="", encoding="utf-8") as f:
        w = csvmod.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in results:
            w.writerow({k: r.get(k) for k in cols})

    # ---- Markdown report ----
    by_q = {}
    for r in results:
        by_q.setdefault(r["query"], {})[r["system"]] = r

    lines = []
    lines.append("# Benchmark results — MySQL vs Memgraph (ConceptNet typed subgraph)\n")
    lines.append(f"* generated: {env.get('date')}")
    lines.append(f"* protocol: {env.get('warmup')} warm-up runs discarded, "
                 f"{env.get('reps')} measured, median reported; "
                 f"shortest-path depth (B3/B4) = {env.get('sp_depth')}")
    lines.append(f"* MySQL {env.get('mysql_version')} "
                 f"(buffer pool {env.get('innodb_buffer_pool_mb', '?')} MB, warm) · "
                 f"Memgraph {env.get('memgraph_version')} (in-RAM)")
    lines.append(f"* platform: {env.get('platform')}, {env.get('cpus')} CPUs, "
                 f"Python {env.get('python')}\n")

    deg = anchors_info.get("degrees", {})
    if deg:
        lines.append("## Anchor degrees (out-degree in the loaded slice)\n")
        lines.append("| anchor | out-degree |")
        lines.append("|---|---|")
        for a in sorted(deg, key=lambda x: -deg[x]):
            lines.append(f"| {a} | {deg[a]:,} |")
        lines.append("")

    lines.append("## Results\n")
    lines.append("| query | mechanism (course concept) | expected | SQL med (ms) | "
                 "Cypher med (ms) | ratio | measured winner | rows | verify | "
                 "LOC S/C | plan |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|")
    for spec in specs:
        rs = by_q.get(spec.qid, {})
        sq, cy = rs.get("mysql"), rs.get("memgraph")
        if not sq and not cy:
            continue
        sql_ms = sq.get("median_ms") if sq else None
        cy_ms = cy.get("median_ms") if cy else None
        sql_err = bool(sq and "error" in sq)
        cy_err = bool(cy and "error" in cy)
        ratio = (f"{cy_ms / sql_ms:.2f}x" if (sql_ms and cy_ms) else "—")
        rows = (sq.get("rows") if sq else None)
        if rows is None:
            rows = cy.get("rows") if cy else "—"
        if sql_err or cy_err:
            errs = []
            if sql_err:
                errs.append("SQL: " + str(sq.get("error"))[:60])
            if cy_err:
                errs.append("CYPHER: " + str(cy.get("error"))[:60])
            verify = " ; ".join(errs)
        elif sq and cy and sq.get("verify") is not None:
            verify = "OK" if sq["verify"] == cy["verify"] else "MISMATCH(!)"
        else:
            verify = "n/a"
        locs = sq.get("loc", "—") if sq else "—"
        locc = cy.get("loc", "—") if cy else "—"
        plan = (sq or {}).get("plan_note") or (cy or {}).get("plan_note") or ""
        lines.append(
            f"| {spec.qid} | {spec.mechanism} | {spec.expected} | "
            f"{fmt_ms(sql_ms)} | {fmt_ms(cy_ms)} | {ratio} | "
            f"{winner_of(sql_ms, cy_ms, sql_err, cy_err)} | {rows} | "
            f"{verify} | {locs}/{locc} | {plan} |")
    lines.append("")

    mism = [q for q, d in by_q.items()
            if d.get("mysql", {}).get("verify") is not None
            and d.get("memgraph", {}).get("verify") is not None
            and d["mysql"]["verify"] != d["memgraph"]["verify"]]
    lines.append("## Verification\n")
    lines.append("* result sets are canonicalized (sorted, floats rounded to 4) "
                 "and compared by hash across systems."
                 + ("** All matched.**" if not mism else
                    f"** MISMATCH on: {', '.join(mism)} — investigate before "
                    f"trusting any timing.**") + "\n")

    lines.append("## Storage\n")
    lines.append("```json")
    lines.append(json.dumps(storage, indent=2, ensure_ascii=False,
                            default=json_default))
    lines.append("```\n")

    lines.append("## Reading guide (one line per family)\n")
    lines.append("* A1 is the noise floor: read every other number relative to it.")
    lines.append("* A2 minus A1 = tuple reassembly (join-backs); the B1 slope = "
                 "index-join chain vs. pointer traversal — the headline figure.")
    lines.append("* A4a tie means SQL *prepaid* for it (ix_obj); A6/B1 plans: "
                 "look for 'range'/'ref' vs 'ScanAll'.")
    lines.append("* C1 is expected to favor SQL — it keeps the table honest.")
    lines.append("* D1 is qualitative: both 0 under STRICT_CONTRACT; D2/D3 "
                 "(write demos) are manual by design.\n")

    with open(os.path.join(out_dir, "benchmark_report.md"), "w",
              encoding="utf-8") as f:
        f.write("\n".join(lines))

    # ---- console summary ----
    print("\n" + "=" * 100)
    print(f"{'query':<10}{'expected':<28}{'SQL med':>10}{'Cypher med':>12}"
          f"{'winner':>18}{'rows':>9}{'verify':>10}")
    print("-" * 100)
    for spec in specs:
        rs = by_q.get(spec.qid, {})
        if not rs:
            continue
        sq, cy = rs.get("mysql"), rs.get("memgraph")
        sql_ms = sq.get("median_ms") if sq else None
        cy_ms = cy.get("median_ms") if cy else None
        sql_err = bool(sq and "error" in sq)
        cy_err = bool(cy and "error" in cy)
        rows = (sq.get("rows") if sq else None)
        if rows is None:
            rows = cy.get("rows") if cy else "—"
        if sql_err or cy_err:
            verify = "ERROR"
        elif sq and cy and sq.get("verify") is not None:
            verify = "OK" if sq["verify"] == cy["verify"] else "MISMATCH"
        else:
            verify = "n/a"
        print(f"{spec.qid:<10}{spec.expected[:27]:<28}"
              f"{fmt_ms(sql_ms):>10}{fmt_ms(cy_ms):>12}"
              f"{winner_of(sql_ms, cy_ms, sql_err, cy_err):>18}"
              f"{str(rows):>9}{verify:>10}")
    print("=" * 100)
    print(f"artifacts: {out_dir}/benchmark_report.md, benchmark_results.csv, "
          f"benchmark_results.json, plans/")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global REPS, WARMUP, SP_MAX_DEPTH
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reps", type=int, default=REPS)
    ap.add_argument("--warmup", type=int, default=WARMUP)
    ap.add_argument("--quick", action="store_true", help="3 reps, 1 warm-up")
    ap.add_argument("--sp-depth", type=int, default=SP_MAX_DEPTH,
                    help="depth bound for B3/B4 (shortest-path family); "
                         "default 3 — raising it re-enters the exponential "
                         "regime that froze Docker at depth 5")
    ap.add_argument("--queries", default=None, help="comma-separated qids")
    ap.add_argument("--out-dir", default=OUT_DIR)
    ap.add_argument("--mysql-only", action="store_true")
    ap.add_argument("--memgraph-only", action="store_true")
    ap.add_argument("--no-plans", action="store_true")
    ap.add_argument("--stratify-b1", action="store_true",
                    help="run B1 on low/medium/high-degree anchors")
    args = ap.parse_args()

    if args.quick:
        REPS, WARMUP = 3, 1
    else:
        REPS, WARMUP = args.reps, args.warmup
    SP_MAX_DEPTH = args.sp_depth

    conn = session = driver = None
    want_mysql = not args.memgraph_only
    want_memgraph = not args.mysql_only

    if want_mysql:
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
        except mysql.connector.Error as exc:
            raise SystemExit(f"MySQL connection failed: {exc}")
    if want_memgraph:
        driver = GraphDatabase.driver(MEMGRAPH_URI, auth=MEMGRAPH_AUTH)
        driver.verify_connectivity()
        session = driver.session()

    # --- environment, anchors, specs ---------------------------------------
    env = collect_env(conn if want_mysql else _NullConn(),
                      session if want_memgraph else _NullSession())
    present, degrees, missing = (anchor_prep(conn) if want_mysql
                                 else (set(), {}, []))
    if missing:
        print(f"[WARN] anchors not present in the loaded slice: {missing}")
        print("       queries using them will be skipped — edit ANCHORS at "
              "the top of this file to substitute.")

    b1_anchors = None
    if args.stratify_b1:
        b1_anchors = stratified_anchors(degrees)
        if b1_anchors:
            print(f"[INFO] B1 stratified anchors: {b1_anchors}")

    specs = build_specs(stratify_b1=args.stratify_b1, b1_anchors=b1_anchors)
    if args.queries:
        keep = {q.strip() for q in args.queries.split(",")}
        specs = [s for s in specs
                 if s.qid in keep or any(s.qid.startswith(k) for k in keep)]

    # skip queries whose anchors are missing
    def anchors_of(spec):
        return {v for k, v in spec.params.items()
                if k.startswith("anchor") or k in ("lo", "hi")}
    runnable = []
    for s in specs:
        miss = anchors_of(s) - present
        if want_mysql and miss:
            print(f"[SKIP] {s.qid}: missing anchor(s) {sorted(miss)}")
            continue
        runnable.append(s)

    os.makedirs(args.out_dir, exist_ok=True)
    plans_dir = os.path.join(args.out_dir, "plans")
    os.makedirs(plans_dir, exist_ok=True)

    results = []
    print(f"\nRunning {len(runnable)} queries "
          f"(warmup={WARMUP}, reps={REPS}, sp_depth={SP_MAX_DEPTH}, "
          f"plans={'on' if not args.no_plans else 'off'})\n")

    for spec in runnable:
        print(f"[{spec.qid}] {spec.mechanism} ...", flush=True)
        spec_rows = []
        for system in ("mysql", "memgraph"):
            if system == "mysql" and not (want_mysql and spec.sql):
                continue
            if system == "memgraph" and not (want_memgraph and spec.cypher):
                continue
            text = spec.resolve(spec.sql if system == "mysql" else spec.cypher)
            run_fn = (lambda t=text: run_mysql(conn, t)) if system == "mysql" \
                     else (lambda t=text: run_memgraph(session, t))
            try:
                times, rows = bench(run_fn, spec.reps, spec.warmup)
            except Exception as exc:
                print(f"    {system}: FAILED — {exc}")
                results.append({"query": spec.qid, "system": system,
                                "error": str(exc), "family": spec.family,
                                "mechanism": spec.mechanism,
                                "expected": spec.expected})
                continue

            entry = {
                "query": spec.qid, "family": spec.family,
                "mechanism": spec.mechanism, "expected": spec.expected,
                "system": system,
                "anchor": next(iter(anchors_of(spec)), ""),
                **summary_stats(times),
                "rows": len(rows),
                "loc": loc_of(text), "chars": len(text.strip()),
                "verify": rows_hash(rows) if spec.verify else None,
                "raw_ms": [round(t, 3) for t in times],
            }
            spec_rows.append(len(rows))

            if not args.no_plans and (spec.plan or spec.qid in PLAN_QUERIES):
                try:
                    ptext = (mysql_plan(conn, text) if system == "mysql"
                             else memgraph_plan(session, text))
                    fname = os.path.join(
                        plans_dir, f"{spec.qid}_{system}.txt")
                    with open(fname, "w", encoding="utf-8") as f:
                        f.write(f"-- {spec.qid} ({system}) "
                                f"{spec.mechanism}\n\n{text}\n\n"
                                f"---- PLAN ----\n{ptext}\n")
                    entry["plan_file"] = os.path.relpath(fname, args.out_dir)
                    entry["plan_note"] = plan_note(ptext)
                except Exception as exc:
                    entry["plan_note"] = f"(plan capture failed: {exc})"

            results.append(entry)
            print(f"    {system:<9} median {entry['median_ms']:>10.2f} ms   "
                  f"rows {entry['rows']:>7,}   "
                  f"loc {entry['loc']:>3}   "
                  f"{('verify ' + entry['verify']) if entry['verify'] else ''}")

        if spec_rows and all(r == 0 for r in spec_rows):
            print(f"    [WARN] both systems returned 0 rows — this query "
                  f"benchmarks nothing; consider different anchors "
                  f"(edit ANCHORS at the top of this file)")

        # incremental save: a Ctrl-C keeps everything measured so far
        write_outputs(args.out_dir, results, runnable, env,
                      collect_storage(conn, session)
                      if (want_mysql and want_memgraph) else {},
                      {"present": sorted(present), "degrees": degrees,
                       "missing": missing})

    write_outputs(args.out_dir, results, runnable, env,
                  collect_storage(conn, session)
                  if (want_mysql and want_memgraph) else {},
                  {"present": sorted(present), "degrees": degrees,
                   "missing": missing})

    if conn:
        conn.close()
    if driver:
        driver.close()
    print("\nDone.")


class _NullConn:
    """No-op stand-in when a system is skipped (env collection only)."""

    def cursor(self):
        raise Exception("mysql disabled")


class _NullSession:
    def run(self, *_a, **_k):
        raise Exception("memgraph disabled")


if __name__ == "__main__":
    main()