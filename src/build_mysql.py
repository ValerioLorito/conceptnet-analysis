#!/usr/bin/env python3
"""
build_mysql.py — load the typed ConceptNet subgraph into MySQL.

Counterpart of build_memgraph.py; consumes the SAME two files produced by
label_concepts.py:

    typed_nodes.csv   uri, name, pos, node_type, label_source
    typed_edges.csv   relation, subject, object, weight, edge_class,
                      permitted

Table schema (created beforehand by conceptnet_schema.sql — this script
never creates or alters tables, it only fills `nodes` and `edges`):

    nodes (node_id, uri, name, pos, node_type, label_source)
    edges (edge_id, subject_id, subject_type, object_id, object_type,
           relation_id, edge_class, weight)

Identity rules — mirrored exactly by build_memgraph.py's MERGE clauses:
    node = uri
    edge = (subject, relation, object); a duplicate triple keeps the max
    weight, via INSERT ... ON DUPLICATE KEY UPDATE weight = GREATEST(...)
    (the SQL image of the Cypher MERGE ... ON MATCH SET rule). Both loaders
    parse the same CSV weight strings, so stored weights are identical.

Stored vs derived (the relational answer to "where is the schema?"):
    edge_class    stored, and verified by the engine: CHECK
                  chk_edges_class + the composite FKs to nodes(node_id,
                  node_type) reject bad input with errors 3819 / 1452;
    permitted     NOT stored — derived by view v_edges from
                  relation_class_perms. The static relation->class schema
                  is asserted at startup against
                  label_concepts.RELATION_TO_EDGE_CLASSES.

Run order
    1. python label_concepts.py          (produces the two CSVs)
    2. mysql -u <user> -p < conceptnet_schema.sql
    3. python build_mysql.py             (this file)
    4. python build_memgraph.py          (same CSVs, same rules)

Everything that can be validated (endpoints, relations, node types,
pos<->type consistency, edge_class derivation, label sources) is validated
BEFORE any TRUNCATE/INSERT, so a bad input file fails fast without leaving
a half-loaded database.

Requires: mysql-connector-python  (pip install mysql-connector-python)
          MySQL >= 8.0.16 (the schema needs CHECK enforcement).
          TRUNCATE requires the DROP privilege on the schema.

Paired files: label_concepts.py (upstream), build_memgraph.py (twin loader),
conceptnet_schema.sql (DDL).
"""

import csv
import json
import os
import re
import sys
import time
from collections import defaultdict

try:
    import mysql.connector
    from mysql.connector import errorcode
except ImportError:
    print("Please install the MySQL connector: "
          "pip install mysql-connector-python", file=sys.stderr)
    sys.exit(1)


# --- Configuration (mirror of build_memgraph.py's constants block) --------

NODES_FILE = "data/preprocessed/conceptnet_science_2hop_concepts.csv"
EDGES_FILE = "data/preprocessed/conceptnet_science_2hop.csv"

MYSQL_CONFIG = {
    "host":     os.environ.get("MYSQL_HOST", "localhost"),
    "port":     int(os.environ.get("MYSQL_PORT", "3306")),
    "user":     os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", ""),
    "database": "conceptnet",
    "charset":  "utf8mb4",
    "autocommit": False,
}

BATCH_SIZE         = 5_000      # rows per multi-row INSERT; lower it if you
                                # hit max_allowed_packet limits
RESET_FIRST        = True       # truncate nodes+edges first (like DETACH
                                # DELETE in build_memgraph.py)
ANALYZE_AFTER_LOAD = True       # refresh optimizer stats before benchmarking
RUN_SYNC_CHECK     = True       # assert relation_edge_classes matches
                                # label_concepts.RELATION_TO_EDGE_CLASSES

NODE_TYPES  = {"EntityNode", "ActionEventNode", "PropertyNode"}
TYPE_LETTER = {"EntityNode": "E", "ActionEventNode": "A", "PropertyNode": "P"}
POS_TO_TYPE = {"n": "EntityNode", "v": "ActionEventNode",
               "a": "PropertyNode", "r": "PropertyNode", "s": "PropertyNode"}

REQUIRED_TABLES = (
    "node_types", "label_sources", "relations", "edge_classes",
    "relation_edge_classes", "relation_class_perms",
    "nodes", "edges", "v_edges", "v_triples",
)

EDGE_COLUMNS = ["subject_id", "subject_type", "object_id", "object_type",
                "relation_id", "edge_class", "weight"]


# --- Small helpers ---------------------------------------------------------

def chunks(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def insert_rows(cursor, table, columns, rows, suffix=""):
    """
    Parameterized multi-row INSERT, built explicitly.

    Why not cursor.executemany(): the connector accelerates executemany only
    for plain `INSERT ... VALUES` statements (regex-based rewrite), and its
    behaviour once an ON DUPLICATE KEY UPDATE clause — especially the
    MySQL 8.0.19+ row-alias form `... AS new ON DUPLICATE ...` — is in the
    clause varies across connector versions. Building the statement by hand
    keeps the batching identical everywhere. Statement size is roughly
    rows x row width; tune BATCH_SIZE if needed.
    """
    if not rows:
        return
    tuple_ph = "(" + ", ".join(["%s"] * len(columns)) + ")"
    sql = (f"INSERT INTO {table} ({', '.join(columns)}) "
           f"VALUES {', '.join([tuple_ph] * len(rows))}{suffix}")
    params = [v for row in rows for v in row]
    cursor.execute(sql, params)


# --- Preflight --------------------------------------------------------------

def preflight(cursor):
    cursor.execute("SHOW TABLES")
    present = {row[0] for row in cursor.fetchall()}
    missing = [t for t in REQUIRED_TABLES if t not in present]
    if missing:
        raise SystemExit(
            "Schema incomplete — missing: " + ", ".join(missing) +
            "\nApply it first:  mysql -u <user> -p < conceptnet_schema.sql")
    cursor.execute("SELECT VERSION()")
    version = cursor.fetchone()[0]
    print(f"  MySQL {version}")
    return version


def supports_row_alias(version_str):
    """
    INSERT ... VALUES ... AS new ON DUPLICATE KEY UPDATE exists since
    MySQL 8.0.19 (the legacy VALUES() form was deprecated in 8.0.20 and
    removed in newer majors). MariaDB never implemented the row alias, so
    it always uses the legacy form.
    """
    if "mariadb" in version_str.lower():
        return False
    m = re.match(r"(\d+)\.(\d+)\.(\d+)", version_str)
    if not m:
        return False
    major, minor, patch = (int(g) for g in m.groups())
    if (major, minor) > (8, 0):
        return True
    return (major, minor) == (8, 0) and patch >= 19


def sync_check(cursor):
    """Assert relation_edge_classes == label_concepts.RELATION_TO_EDGE_CLASSES."""
    if not RUN_SYNC_CHECK:
        return
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        # NB: importing label_concepts pulls in nltk/requests (and may
        # quietly download WordNet data on first use); that is acceptable
        # for a startup check, and we degrade gracefully if it is missing.
        from label_concepts import (RELATION_TO_EDGE_CLASSES,
                                    check_static_schema)
        check_static_schema()
    except (Exception, SystemExit) as exc:
        print(f"  [WARN] relation-schema sync check skipped — "
              f"label_concepts.py not importable ({exc!r})")
        return

    cursor.execute("""
        SELECT r.relation_name, rec.edge_class
        FROM relation_edge_classes rec
        JOIN relations r ON r.relation_id = rec.relation_id
    """)
    db_map = defaultdict(set)
    for name, cls in cursor.fetchall():
        db_map[name].add(cls)
    py_map = {rel: set(cls) for rel, cls in RELATION_TO_EDGE_CLASSES.items()}

    bad = [rel for rel in set(db_map) | set(py_map)
           if db_map.get(rel) != py_map.get(rel)]
    if bad:
        lines = [f"    {rel:<30} DB={sorted(db_map.get(rel, set()))} "
                 f"pipeline={sorted(py_map.get(rel, set()))}"
                 for rel in sorted(bad)]
        raise SystemExit(
            "relation_edge_classes is out of sync with "
            "label_concepts.RELATION_TO_EDGE_CLASSES "
            "(update conceptnet_schema.sql):\n" + "\n".join(lines))
    grants = sum(len(v) for v in py_map.values())
    print(f"  sync ok: relation_edge_classes == RELATION_TO_EDGE_CLASSES "
          f"({grants} grants over {len(py_map)} relations)")


def fetch_reference_sets(cursor):
    cursor.execute("SELECT relation_id, relation_name FROM relations")
    rel2id = {name: rid for rid, name in cursor.fetchall()}
    cursor.execute("SELECT label_source FROM label_sources")
    sources = {row[0] for row in cursor.fetchall()}
    print(f"  {len(rel2id)} relations, {len(sources)} label sources "
          f"available in the schema")
    return rel2id, sources


# --- Reading and validating the CSVs ---------------------------------------

def read_nodes(path):
    """
    Read typed_nodes.csv and validate everything the engine would reject
    (node_type, pos, pos<->type rule) plus what the engine cannot check
    (column lengths, duplicate URIs). Identical duplicate rows are
    collapsed (MERGE-like); conflicting ones abort.
    """
    nodes, seen, duplicates = [], {}, 0
    for row in read_csv(path):
        uri = (row.get("uri") or "").strip()
        name = (row.get("name") or "").strip()
        pos = (row.get("pos") or "").strip()
        node_type = (row.get("node_type") or "").strip()
        label_source = (row.get("label_source") or "").strip()

        if not uri:
            raise SystemExit("typed_nodes.csv: row with empty uri")
        if len(uri) > 255 or len(name) > 255:
            raise SystemExit(f"typed_nodes.csv: uri/name longer than 255 "
                             f"chars for {uri[:60]!r}")
        if node_type not in NODE_TYPES:
            raise SystemExit(f"typed_nodes.csv: unknown node_type "
                             f"{node_type!r} for {uri}")
        if pos not in POS_TO_TYPE:
            raise SystemExit(f"typed_nodes.csv: bad pos {pos!r} for {uri}")
        if POS_TO_TYPE[pos] != node_type:
            raise SystemExit(
                f"typed_nodes.csv: pos {pos!r} inconsistent with "
                f"{node_type} for {uri} "
                f"(CHECK chk_nodes_pos_type would reject it)")

        rec = (name, pos, node_type, label_source)
        if uri in seen:
            if seen[uri] == rec:
                duplicates += 1
                continue
            raise SystemExit(f"typed_nodes.csv: conflicting rows for {uri}")
        seen[uri] = rec
        nodes.append((uri, name, pos, node_type, label_source))
    return nodes, duplicates


def assign_ids(nodes):
    """
    node_id = position in the file (label_concepts.py writes typed_nodes.csv
    sorted by uri, so the numbering is deterministic across runs and
    machines). This also makes edge_id numbering deterministic, because
    TRUNCATE resets AUTO_INCREMENT.
    """
    uri2info, rows = {}, []
    for i, (uri, name, pos, node_type, label_source) in enumerate(nodes, 1):
        uri2info[uri] = (i, node_type)
        rows.append((i, uri, name, pos, node_type, label_source))
    return uri2info, rows


def read_edges(path, uri2info, rel2id):
    """
    Read typed_edges.csv, validate against the node map and the relations
    table, derive edge_class from the endpoint types and cross-check it
    against the CSV value. Violations of the relation->class permission are
    NOT rejected here (they are data, surfaced by v_edges) — only
    inconsistencies that would break the load or the input contract are.
    Returns (rows, count_of_permitted0).
    """
    rows, problems, csv_permitted0 = [], [], 0
    for row in read_csv(path):
        rel = (row.get("relation") or "").strip()
        s = (row.get("subject") or "").strip()
        o = (row.get("object") or "").strip()
        where = f"({rel} {s} {o})"

        try:
            w = float(row.get("weight"))
        except (TypeError, ValueError):
            problems.append(f"bad weight {row.get('weight')!r} {where}")
            continue

        si, oi = uri2info.get(s), uri2info.get(o)
        if si is None or oi is None:
            problems.append(f"unknown endpoint "
                            f"{s if si is None else o} {where}")
            continue
        rid = rel2id.get(rel)
        if rid is None:
            problems.append(f"unknown relation {rel!r} — is the relations "
                            f"seed in conceptnet_schema.sql up to date?")
            continue

        ec = TYPE_LETTER[si[1]] + "2" + TYPE_LETTER[oi[1]]
        if row.get("edge_class") != ec:
            problems.append(f"edge_class mismatch: CSV "
                            f"{row.get('edge_class')!r} vs derived {ec} "
                            f"{where}")
            continue
        if row.get("permitted") not in ("0", "1"):
            problems.append(f"bad permitted {row.get('permitted')!r} {where}")
            continue
        if row["permitted"] == "0":
            csv_permitted0 += 1

        rows.append({
            "relation": rel, "relation_id": rid,
            "subject_id": si[0], "subject_type": si[1],
            "object_id": oi[0], "object_type": oi[1],
            "edge_class": ec, "weight": w,
        })

    if problems:
        shown = "\n".join(f"    {p}" for p in problems[:10])
        more = ("" if len(problems) <= 10
                else f"\n    ... and {len(problems) - 10:,} more")
        raise SystemExit(
            f"typed_edges.csv failed validation ({len(problems):,} problems "
            f"— regenerate it with label_concepts.py):\n{shown}{more}")
    return rows, csv_permitted0


# --- Loading ----------------------------------------------------------------

def reset(conn, cursor):
    if not RESET_FIRST:
        return
    # TRUNCATE resets AUTO_INCREMENT too, so edge_id numbering restarts at 1
    # and stays deterministic for a given input. FK checks must be off to
    # truncate the parent table of edges' composite FKs.
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    cursor.execute("TRUNCATE TABLE edges")
    cursor.execute("TRUNCATE TABLE nodes")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
    conn.commit()
    print("  truncated nodes and edges (reference tables untouched)")


def load_nodes(conn, cursor, node_rows):
    t0 = time.time()
    for batch in chunks(node_rows, BATCH_SIZE):
        insert_rows(cursor, "nodes",
                    ["node_id", "uri", "name", "pos",
                     "node_type", "label_source"], batch)
        conn.commit()
    print(f"  inserted {len(node_rows):,} nodes  ({time.time() - t0:.1f}s)")


def load_edges(conn, cursor, edges, row_alias):
    # The duplicate-triple rule, in the dialect the server supports.
    if row_alias:
        suffix = (" AS new ON DUPLICATE KEY UPDATE "
                  "weight = GREATEST(weight, new.weight)")
    else:
        suffix = (" ON DUPLICATE KEY UPDATE "
                  "weight = GREATEST(weight, VALUES(weight))")

    by_rel = defaultdict(list)
    for e in edges:
        by_rel[e["relation"]].append(e)

    t_total = time.time()
    for rel in sorted(by_rel):
        rows = [(e["subject_id"], e["subject_type"], e["object_id"],
                 e["object_type"], e["relation_id"], e["edge_class"],
                 e["weight"]) for e in by_rel[rel]]
        t0 = time.time()
        for batch in chunks(rows, BATCH_SIZE):
            insert_rows(cursor, "edges", EDGE_COLUMNS, batch, suffix)
            conn.commit()
        print(f"  {rel:<28} {len(rows):>9,} edges  "
              f"({time.time() - t0:.1f}s)")
    print(f"  inserted {len(edges):,} edge rows "
          f"({time.time() - t_total:.1f}s total)")


def analyze(cursor):
    cursor.execute("ANALYZE TABLE nodes, edges")
    msgs = "; ".join(r[3] for r in cursor.fetchall())
    print(f"  ANALYZE TABLE nodes, edges: {msgs}")


# --- Report (mirrors build_memgraph.py's report, plus parity checks) -------

def load_pipeline_stats():
    path = os.path.join(os.path.dirname(os.path.abspath(NODES_FILE)),
                        "pipeline_stats.json")
    if os.path.isfile(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def report(cursor, csv_nodes, csv_edges, csv_permitted0, stats):
    print("\nPost-load summary")
    for nt in ("EntityNode", "ActionEventNode", "PropertyNode"):
        cursor.execute("SELECT COUNT(*) FROM nodes WHERE node_type = %s",
                       (nt,))
        c = cursor.fetchone()[0]
        print(f"  {nt:<15} {c:>9,} nodes")
    cursor.execute("SELECT COUNT(*) FROM nodes")
    n = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM edges")
    e = cursor.fetchone()[0]
    print(f"  {'nodes (total)':<18} {n:>9,}")
    print(f"  {'edges (total)':<18} {e:>9,}")
    if e != csv_edges:
        print(f"  [WARN] DB edge count != CSV row count ({csv_edges:,}); "
              f"{csv_edges - e:,} duplicate triples merged (max-weight rule)")

    print("\nLabel provenance")
    cursor.execute("SELECT label_source, COUNT(*) AS c FROM nodes "
                   "GROUP BY label_source ORDER BY c DESC")
    for src, c in cursor.fetchall():
        print(f"  {src:<16} {c:>9,}")

    print("\nContract violations (permission parity)")
    cursor.execute("SELECT COUNT(*) FROM v_edges WHERE NOT permitted")
    db_viol = cursor.fetchone()[0]
    print(f"  {'typed_edges.csv (permitted=0)':<30}: {csv_permitted0:>9,}")
    print(f"  {'v_edges (NOT permitted)':<30}: {db_viol:>9,}")
    stats_viol = None
    if stats is not None:
        stats_viol = sum(stats.get("contract_violations", {}).values())
        print(f"  {'pipeline_stats.json':<30}: {stats_viol:>9,}")
    if not (csv_permitted0 == db_viol and
            (stats_viol is None or stats_viol == db_viol)):
        print("  [WARN] violation counts disagree — check the sync of "
              "relation_edge_classes / relation_class_perms")

    if stats is not None:
        print("\nIsomorphism cross-check vs pipeline_stats.json")
        exp_nodes = stats.get("nodes_kept")
        exp_edges = (stats.get("edges_kept", 0)
                     - stats.get("edges_duplicate_triples", 0))
        print(f"  nodes: DB {n:>9,}  expected {exp_nodes:>9,}  "
              f"[{'ok' if exp_nodes == n else 'MISMATCH'}]")
        print(f"  edges: DB {e:>9,}  expected {exp_edges:>9,}  "
              f"[{'ok' if exp_edges == e else 'MISMATCH'}]")
    else:
        print("\n(pipeline_stats.json not found — isomorphism cross-check "
              "skipped)")

    print("\nStorage (InnoDB estimates — the MySQL side of the "
          "memory metric)")
    cursor.execute("""
        SELECT table_name,
               ROUND(data_length  / 1024 / 1024, 1),
               ROUND(index_length / 1024 / 1024, 1)
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name IN ('nodes', 'edges')
        ORDER BY table_name
    """, (MYSQL_CONFIG["database"],))
    for name, dmb, imb in cursor.fetchall():
        print(f"  {name:<6} data {dmb:>8} MB   index {imb:>8} MB")


# --- Main -------------------------------------------------------------------

def main():
    t_start = time.time()

    print(f"Reading {NODES_FILE} ...")
    nodes, node_dups = read_nodes(NODES_FILE)
    dup_note = (f" ({node_dups:,} identical duplicate rows collapsed)"
                if node_dups else "")
    print(f"  {len(nodes):,} nodes{dup_note}")

    print(f"Connecting to MySQL "
          f"({MYSQL_CONFIG['database']}@{MYSQL_CONFIG['host']}:"
          f"{MYSQL_CONFIG['port']}) ...")
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            raise SystemExit(
                f"Database {MYSQL_CONFIG['database']!r} does not exist — "
                f"apply the schema first:\n"
                f"    mysql -u <user> -p < conceptnet_schema.sql")
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            raise SystemExit(
                "Access denied — set MYSQL_USER / MYSQL_PASSWORD or edit "
                "MYSQL_CONFIG in this file.")
        raise SystemExit(f"MySQL connection failed: {err}")

    try:
        cursor = conn.cursor()

        print("\nPreflight")
        version = preflight(cursor)
        row_alias = supports_row_alias(version)
        print("  duplicate handling: "
              + ("row alias 'AS new' (MySQL >= 8.0.19)" if row_alias
                 else "VALUES() (legacy form)"))
        sync_check(cursor)
        rel2id, sources = fetch_reference_sets(cursor)

        bad_sources = sorted({n[4] for n in nodes} - sources)
        if bad_sources:
            raise SystemExit(
                "typed_nodes.csv uses label_source values not seeded in the "
                f"label_sources table: {bad_sources} — update "
                "conceptnet_schema.sql.")

        uri2info, node_rows = assign_ids(nodes)

        print(f"\nReading {EDGES_FILE} ...")
        edges, csv_permitted0 = read_edges(EDGES_FILE, uri2info, rel2id)
        print(f"  {len(edges):,} edges ({csv_permitted0:,} flagged "
              f"permitted=0)")

        print("\nReset")
        reset(conn, cursor)

        print("\nLoading nodes")
        load_nodes(conn, cursor, node_rows)

        print("\nLoading edges")
        load_edges(conn, cursor, edges, row_alias)

        if ANALYZE_AFTER_LOAD:
            print("\nOptimizer statistics")
            analyze(cursor)

        report(cursor, len(nodes), len(edges), csv_permitted0,
               load_pipeline_stats())
    finally:
        conn.close()

    print(f"\nDone. ({time.time() - t_start:.1f}s total)")


if __name__ == "__main__":
    main()