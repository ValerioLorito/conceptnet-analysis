#!/usr/bin/env python3
"""
inspect_query.py — run one battery query on BOTH systems, print the rows
side by side, and report a canonical diff.

Why this exists: benchmark.py stores only HASHES of result sets (by
design — timing runs shouldn't build giant Python lists); when the
harness reports verify OK / MISMATCH / ERROR, this is the tool for
eyeballing the ACTUAL rows. It reuses build_specs() from benchmark.py,
so the queries it runs are byte-identical to the ones the harness timed
— the two scripts can never drift apart.

Usage
    python src/inspect_query.py A5            # run on both systems, diff
    python src/inspect_query.py B5 --limit 15 # show more/fewer rows
    python src/inspect_query.py B5 --diff-only  # only the differing rows
    python src/inspect_query.py A2 --show     # also print the resolved texts
    python src/inspect_query.py --list        # available query ids

Do NOT run this while benchmark.py is timing: same databases, and these
queries would perturb the measurements.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import mysql.connector
from neo4j import GraphDatabase

from benchmark import (MEMGRAPH_AUTH, MEMGRAPH_URI, MYSQL_CONFIG,
                       build_specs, canon_rows, run_memgraph, run_mysql)


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("qid", nargs="?", help="query id, e.g. A5, B1k2, C2")
    ap.add_argument("--limit", type=int, default=20,
                    help="rows to print per system (default 20)")
    ap.add_argument("--show", action="store_true",
                    help="print the resolved SQL/Cypher texts too")
    ap.add_argument("--diff-only", action="store_true",
                    help="print only rows present in one system but not "
                         "the other")
    args = ap.parse_args()

    specs = {s.qid: s for s in build_specs()}

    if not args.qid or args.qid == "--list" or args.qid not in specs:
        if args.qid and args.qid not in specs and args.qid != "--list":
            print(f"unknown qid {args.qid!r}")
        print("available query ids:")
        for s in specs.values():
            print(f"  {s.qid:<10} {s.mechanism}")
        sys.exit(0 if not args.qid else 2)
    spec = specs[args.qid]

    conn = mysql.connector.connect(**MYSQL_CONFIG)
    driver = GraphDatabase.driver(MEMGRAPH_URI, auth=MEMGRAPH_AUTH)
    session = driver.session()

    sql_text = cy_text = None
    sql_rows = cy_rows = None
    try:
        if spec.sql:
            sql_text = spec.resolve(spec.sql)
            sql_rows = run_mysql(conn, sql_text)
        if spec.cypher:
            cy_text = spec.resolve(spec.cypher)
            cy_rows = run_memgraph(session, cy_text)
    finally:
        conn.close()
        driver.close()

    print(f"=== {spec.qid} — {spec.mechanism} ===")
    print(f"family: {spec.family}   expected: {spec.expected}")
    if spec.note:
        print(f"note:   {spec.note}")
    print(f"SQL rows:    {len(sql_rows) if sql_rows is not None else '(no SQL version)'}")
    print(f"Cypher rows: {len(cy_rows) if cy_rows is not None else '(no Cypher version)'}")

    if args.show:
        if sql_text:
            print("\n--- resolved SQL ---\n" + sql_text.strip())
        if cy_text:
            print("\n--- resolved Cypher ---\n" + cy_text.strip())

    n = args.limit
    if not args.diff_only:
        if sql_rows is not None:
            print(f"\n--- SQL, first {n} rows ---")
            for r in sql_rows[:n]:
                print("  ", r)
        if cy_rows is not None:
            print(f"\n--- Cypher, first {n} rows ---")
            for r in cy_rows[:n]:
                print("  ", r)

    if sql_rows is not None and cy_rows is not None:
        cs, cc = canon_rows(sql_rows), canon_rows(cy_rows)
        if cs == cc:
            print("\nCANONICAL MATCH — identical result multisets "
                  "(sorted, floats rounded to 4)")
        else:
            only_sql = [r for r in cs if r not in set(cc)]
            only_cy = [r for r in cc if r not in set(cs)]
            print(f"\nCANONICAL DIFF — {len(only_sql)} row(s) only in SQL, "
                  f"{len(only_cy)} only in Cypher")
            for r in only_sql[:max(n, 10)]:
                print("   SQL only:  ", r)
            for r in only_cy[:max(n, 10)]:
                print("   Cypher only:", r)
            print("(a diff here = the two engines answered different "
                  "questions, or non-isomorphism — fix before trusting "
                  "any timing)")


if __name__ == "__main__":
    main()