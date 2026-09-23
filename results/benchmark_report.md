# Benchmark results — MySQL vs Memgraph (ConceptNet typed subgraph)

* generated: 2026-09-23T18:50:47
* protocol: 2 warm-up runs discarded, 10 measured, median reported; shortest-path depth (B3/B4) = 3
* MySQL 8.0.46 (buffer pool 128 MB, warm) · Memgraph (unavailable: {neo4j_code: Memgraph.ClientError.MemgraphError.MemgraphError} {message: There is no procedure named 'mg.info'.} {gql_status: 50N42} {gql_status_description: error: general processing exception - unexpected error. There is no procedure named 'mg.info'.}) (in-RAM)
* platform: macOS-26.6.2-arm64-arm-64bit, 10 CPUs, Python 3.10.0

## Anchor degrees (out-degree in the loaded slice)

| anchor | out-degree |
|---|---|
| cell | 357 |
| gene | 40 |
| mitochondrion | 27 |
| photosynthesis | 22 |
| chlorophyll | 19 |
| microscope | 9 |
| enzyme | 7 |
| laboratory | 6 |
| beaker | 2 |
| abundant | 1 |
| sunlight | 1 |

## Results

| query | mechanism (course concept) | expected | SQL med (ms) | Cypher med (ms) | ratio | measured winner | rows | verify | LOC S/C | plan |
|---|---|---|---|---|---|---|---|---|---|---|
| A1 | equality selection on the unique key (baseline) | tie | 0.34 | 31.83 | 92.52x | sql | 1 | OK | 2/3 |  |
| A2 | 1-hop fan-out + tuple reassembly (join-backs) | slight graph | 0.46 | 1.31 | 2.85x | sql | 22 | OK | 8/3 | index |
| A3 | discriminated access (relation id vs. edge type) | tie | 0.34 | 0.50 | 1.48x | sql | 0 | OK | 6/3 |  |
| A4a | directed (reverse) 1-hop: second index vs. free adjacency | tie | 0.36 | 0.65 | 1.81x | sql | 6 | OK | 6/3 |  |
| A4b | undirected 1-hop: OR predicate vs. one pattern | graph | 1.99 | 0.58 | 0.29x | graph | 0 | OK | 7/3 | index |
| A5 | two-anchor intersection (join topology in the pattern) | tie (readability: graph) | 0.34 | 0.51 | 1.47x | sql | 0 | OK | 8/4 |  |
| A6 | range selection on a sorted index | MySQL | 127.98 | 755.01 | 5.90x | sql | 60507 | OK | 3/4 |  |
| A7 | set difference (anti-join vs. negated pattern) | near tie (readability: graph) | 0.56 | 0.35 | 0.62x | graph | 0 | OK | 11/4 | index |
| B1k1 | 1-hop expansion (index-join chain vs. traversal) | near tie | 0.34 | 0.42 | 1.26x | sql | 7 | OK | 6/4 |  |
| B1k2 | 2-hop expansion (index-join chain vs. traversal) | graph, gap grows with k | 0.39 | 0.64 | 1.65x | sql | 22 | OK | 7/4 | index |
| B1k3 | 3-hop expansion (index-join chain vs. traversal) | graph, gap grows with k | 0.59 | 1.10 | 1.85x | sql | 63 | OK | 8/4 |  |
| B1k4 | 4-hop expansion (index-join chain vs. traversal) | graph, gap grows with k | 3.26 | 2.21 | 0.68x | graph | 100 | OK | 9/4 | index |
| B2 | transitive closure (recursive CTE vs. *1..d) | graph, modest | 0.34 | 0.40 | 1.17x | sql | 12 | OK | 11/3 |  |
| B3 | shortest chain, undirected, any relation (depth-bounded) | graph (expressiveness) | 1519.70 | 91.50 | 0.06x | graph | 1 | OK | 15/3 |  |
| B4 | weighted path scoring — expressiveness boundary | no SQL counterpart | — | 2.11 | — | graph (no SQL counterpart) | 0 | n/a | —/3 |  |
| B5 | anchored triangle (cyclic pattern) | graph, clearly | — | 0.51 | — | graph (SQL failed) | 0 | SQL: 1054 (42S22): Unknown column 'y.name' in 'field list' | —/3 |  |
| B6 | traversal feeding aggregation (bags vs sets!) | graph, moderate | 0.43 | 0.63 | 1.48x | sql | 5 | OK | 8/3 | index |
| C1 | hub ranking (full scan + one-pass grouping) | MySQL | 725.60 | 390.62 | 0.54x | graph | 10 | MISMATCH(!) | 4/3 | index |
| C2 | stored vs. derived grouping key (relation x class) | either — trade-off is the finding | 567.55 | — | — | sql (Cypher failed) | 110 | CYPHER: {neo4j_code: Memgraph.ClientError.MemgraphError.MemgraphErro | 5/— | index |
| D1 | violation check (view vs. meta-graph anti-join) | qualitative | 66.19 | 4713.34 | 71.21x | sql | 1 | MISMATCH(!) | 1/7 |  |

## Verification

* result sets are canonicalized (sorted, floats rounded to 4) and compared by hash across systems.** MISMATCH on: C1, D1 — investigate before trusting any timing.**

## Storage

```json
{
  "mysql_mb": {
    "edges": {
      "data": 34.6,
      "index": 85.3
    },
    "nodes": {
      "data": 22.5,
      "index": 34.6
    }
  },
  "memgraph_storage_info": [
    {
      "storage info": "vm_max_map_count",
      "value": 262144
    },
    {
      "storage info": "memory_res",
      "value": "1.58GiB"
    },
    {
      "storage info": "peak_memory_res",
      "value": "2.35GiB"
    },
    {
      "storage info": "disk_usage",
      "value": "231.17MiB"
    },
    {
      "storage info": "memory_tracked",
      "value": "1.10GiB"
    },
    {
      "storage info": "memory_limit",
      "value": "15.60GiB"
    },
    {
      "storage info": "license_memory_limit",
      "value": "unlimited"
    },
    {
      "storage info": "query+graph_memory_tracked",
      "value": "1.10GiB"
    },
    {
      "storage info": "vector_index_memory_tracked",
      "value": "0B"
    },
    {
      "storage info": "global_isolation_level",
      "value": "SNAPSHOT_ISOLATION"
    },
    {
      "storage info": "session_isolation_level",
      "value": ""
    },
    {
      "storage info": "next_session_isolation_level",
      "value": ""
    },
    {
      "storage info": "global_storage_mode",
      "value": "IN_MEMORY_TRANSACTIONAL"
    }
  ]
}
```

## Reading guide (one line per family)

* A1 is the noise floor: read every other number relative to it.
* A2 minus A1 = tuple reassembly (join-backs); the B1 slope = index-join chain vs. pointer traversal — the headline figure.
* A4a tie means SQL *prepaid* for it (ix_obj); A6/B1 plans: look for 'range'/'ref' vs 'ScanAll'.
* C1 is expected to favor SQL — it keeps the table honest.
* D1 is qualitative: both 0 under STRICT_CONTRACT; D2/D3 (write demos) are manual by design.
