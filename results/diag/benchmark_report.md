# Benchmark results — MySQL vs Memgraph (ConceptNet typed subgraph)

* generated: 2026-09-23T19:52:09
* protocol: 1 warm-up runs discarded, 3 measured, median reported; shortest-path depth (B3/B4) = 3
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
| B5 | anchored triangle (cyclic pattern) | graph, clearly | — | 0.92 | — | graph (SQL failed) | 0 | SQL: 1054 (42S22): Unknown column 'y.name' in 'field list' | —/3 |  |
| C2 | stored vs. derived grouping key (relation x class) | either — trade-off is the finding | 575.44 | — | — | sql (Cypher failed) | 110 | CYPHER: {neo4j_code: Memgraph.ClientError.MemgraphError.MemgraphErro | 5/— | index |

## Verification

* result sets are canonicalized (sorted, floats rounded to 4) and compared by hash across systems.** All matched.**

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
      "value": "1.09GiB"
    },
    {
      "storage info": "peak_memory_res",
      "value": "2.35GiB"
    },
    {
      "storage info": "disk_usage",
      "value": "456.32MiB"
    },
    {
      "storage info": "memory_tracked",
      "value": "588.14MiB"
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
      "value": "588.14MiB"
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
