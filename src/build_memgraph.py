#!/usr/bin/env python3
"""
Load the typed ConceptNet subgraph into Memgraph.

Inputs (both produced by prepare_graph_inputs.py):
    typed_nodes.csv : uri, name, pos, node_type, label_source
    typed_edges.csv : relation, subject, object, weight, edge_class, permitted

Graph schema established by this loader (Memgraph is schema-optional):
    (:Concept:EntityNode      {uri, name, pos, label_source})
    (:Concept:ActionEventNode {uri, name, pos, label_source})
    (:Concept:PropertyNode    {uri, name, pos, label_source})
    (:Concept)-[:<RelationName> {weight}]->(:Concept)     one type per relation

Identity rules (mirrored exactly by the MySQL schema):
    node = uri
    edge = (subject, relation, object); duplicate triples keep the max weight

Notes
    * edge_class / permitted are NOT stored on edges: the class is implied by
      the endpoint labels, the permission by the relation type -- the graph
      answer to what SQL stores in tables and columns.
    * set CREATE_EDGE_TYPE_INDEXES = True if your Memgraph version supports
      edge-type indexes (see the Indexes page of the docs); guarded below.
    * persistence: Memgraph is in-memory; enable snapshots/WAL
      (--storage-snapshot-interval-sec, --storage-wal-enabled) if you want
      the graph to survive restarts.
"""

import csv
import re
import time
from collections import defaultdict

from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

NODES_FILE = "data/preprocessed/conceptnet_science_2hop_concepts.csv"
EDGES_FILE = "data/preprocessed/conceptnet_science_2hop.csv"

MEMGRAPH_URI  = "bolt://localhost:7687"
MEMGRAPH_AUTH = ("", "")

BATCH_SIZE              = 5_000
RESET_FIRST             = True
CREATE_EDGE_TYPE_INDEXES = False

NODE_TYPES  = {"EntityNode", "ActionEventNode", "PropertyNode"}
RELATION_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")   # guards the f-strings


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run_ignore_errors(session, stmt):
    try:
        session.run(stmt)
        print(f"  ok: {stmt}")
    except Neo4jError as e:
        print(f"  skip ({e.code}): {stmt}")


def setup_schema(session, relations):
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


def load_nodes(session, nodes):
    by_type = defaultdict(list)
    for n in nodes:
        if n["node_type"] not in NODE_TYPES:
            raise ValueError(f"unknown node_type {n['node_type']!r} "
                             f"(URI {n['uri']})")
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
            session.run(cypher, rows=rows[i:i + BATCH_SIZE])
            count += len(batch := rows[i:i + BATCH_SIZE])
        print(f"  :{node_type:<15} {count:>9,} nodes   ({time.time() - t0:.1f}s)")


def load_edges(session, edges):
    by_rel, skipped = defaultdict(list), 0
    for e in edges:
        rel = e["relation"].strip()
        if RELATION_RE.match(rel):
            e["weight"] = float(e["weight"])
            by_rel[rel].append(e)
        else:
            skipped += 1
    if skipped:
        print(f"  [WARN] {skipped} edges with non-Cypher-safe relation names skipped")

    for rel in sorted(by_rel):
        rows = by_rel[rel]
        # MERGE = idempotent re-runs + the edge-identity rule: a duplicate
        # (subject, relation, object) triple keeps the max weight.
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
    return sum(len(v) for v in by_rel.values())


def report(session, csv_edge_count):
    print("\nPost-load summary")
    for label in ("EntityNode", "ActionEventNode", "PropertyNode"):
        c = session.run(f"MATCH (n:{label}) RETURN count(n) AS c").single()["c"]
        print(f"  :{label:<15} {c:>9,} nodes")
    n = session.run("MATCH (n:Concept) RETURN count(n) AS c").single()["c"]
    e = session.run("MATCH ()-[r]->() RETURN count(r) AS c").single()["c"]
    print(f"  {'nodes (total)':<18} {n:>9,}")
    print(f"  {'edges (total)':<18} {e:>9,}")
    if e != csv_edge_count:
        print(f"  [WARN] DB edge count != CSV row count ({csv_edge_count:,}); "
              f"duplicate triples were merged (max-weight rule)")

    print("\nLabel provenance")
    for rec in session.run(
            "MATCH (n:Concept) "
            "RETURN n.label_source AS src, count(n) AS c ORDER BY c DESC"):
        print(f"  {rec['src']:<16} {rec['c']:>9,}")


def main():
    print(f"Reading {NODES_FILE} ...")
    nodes = read_csv(NODES_FILE)
    print(f"  {len(nodes):,} nodes")
    print(f"Reading {EDGES_FILE} ...")
    edges = read_csv(EDGES_FILE)
    print(f"  {len(edges):,} edges")

    relations = sorted({e["relation"].strip() for e in edges})
    driver = GraphDatabase.driver(MEMGRAPH_URI, auth=MEMGRAPH_AUTH)
    try:
        with driver.session() as session:
            print("\nSchema setup")
            setup_schema(session, relations)
            print("\nLoading nodes")
            load_nodes(session, nodes)
            print("\nLoading edges")
            csv_edge_count = load_edges(session, edges)
            report(session, csv_edge_count)
    finally:
        driver.close()
    print("\nDone.")


if __name__ == "__main__":
    main()