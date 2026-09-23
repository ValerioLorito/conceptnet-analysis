# Benchmark results — MySQL vs Memgraph (ConceptNet typed subgraph)

* generated: 2026-09-23T16:26:02
* protocol: 1 warm-up runs discarded, 3 measured, median reported
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
| A0 | equality selection via index (baseline) | tie | 0.48 | 0.48 | 1.00x | tie | 3 | OK | 3/4 |  |
| A1 | 1-hop fan-out + tuple reassembly | slight graph | 0.52 | 0.67 | 1.29x | sql | 22 | OK | 8/3 | index |
| A2 | discriminated access (relation id vs. label) | tie | 0.37 | 0.37 | 1.00x | tie | 0 | OK | 6/3 |  |
| A2v | multi-class relation (DefinedAs) — UNION-penalty control | tie | 0.29 | 0.42 | 1.42x | sql | 1 | OK | 6/3 |  |
| A2w | + weight-range predicate (non-indexed filter) | tie | 0.33 | 0.34 | 1.03x | tie | 0 | OK | 6/4 |  |
| A3a | reverse-direction access (second index vs. free) | tie | 0.62 | 0.37 | 0.60x | graph | 6 | OK | 6/3 |  |
| A3b | undirected access (OR predicate vs. one pattern) | graph | 1.88 | 0.33 | 0.18x | graph | 0 | OK | 7/3 | index |
| A4 | two-anchor intersection (join topology in pattern) | tie (readability: graph) | 0.34 | 0.36 | 1.06x | tie | 0 | OK | 8/4 |  |
| A5 | range selection on a sorted index | MySQL | 138.62 | 738.13 | 5.33x | sql | 60507 | OK | 3/4 |  |
| A6 | set difference (anti-join vs. negated pattern) | near tie (readability: graph) | 1.72 | 0.54 | 0.31x | graph | 0 | OK | 11/4 | index |
| B1k1 | 1-hop expansion (index-join chain vs. traversal) | near tie | 0.30 | 0.47 | 1.57x | sql | 7 | OK | 6/4 |  |
| B1k2 | 2-hop expansion (index-join chain vs. traversal) | graph, gap grows with k | 0.39 | 0.60 | 1.52x | sql | 22 | OK | 7/4 | index |
| B1k3 | 3-hop expansion (index-join chain vs. traversal) | graph, gap grows with k | 0.59 | 1.27 | 2.13x | sql | 63 | OK | 8/4 |  |
| B1k4 | 4-hop expansion (index-join chain vs. traversal) | graph, gap grows with k | 3.43 | 2.45 | 0.71x | graph | 100 | OK | 9/4 | index |
| B2 | transitive closure (recursive CTE vs. *1..d) | graph, modest | 0.54 | 0.50 | 0.92x | tie | 12 | OK | 11/3 |  |
| B2v | bounded RPQ with alternation (IsA|PartOf) | graph, modest | 0.37 | 0.55 | 1.48x | sql | 16 | OK | 11/3 |  |

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
      "value": "1.11GiB"
    },
    {
      "storage info": "peak_memory_res",
      "value": "2.13GiB"
    },
    {
      "storage info": "disk_usage",
      "value": "776.73MiB"
    },
    {
      "storage info": "memory_tracked",
      "value": "599.77MiB"
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
      "value": "599.77MiB"
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

* A0 is the noise floor: read every other number relative to it.
* A1 minus A0 = tuple reassembly (join-backs); B1 slope = index-join chain vs. pointer traversal — the headline figure.
* A3a tie means SQL *prepaid* for it (ix_obj); A5/B1 plans: look for 'range'/'ref' vs 'ScanAll'.
* C1 is expected to favor SQL — it keeps the table honest.
* D1 is qualitative: both 0 under STRICT_CONTRACT; D2/D3 (write demos) are manual by design.
