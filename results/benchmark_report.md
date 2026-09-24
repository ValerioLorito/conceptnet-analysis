# Benchmark results — MySQL vs Memgraph (ConceptNet typed subgraph)

* generated: 2026-09-24T11:24:28
* protocol: 2 warm-up runs discarded, 10 measured, median reported; shortest-path depth (B3/B4) = 3; D2/D3 are single-shot write demos (state signatures)
* MySQL 8.0.46 (buffer pool 128 MB, warm) · Memgraph (unavailable: {neo4j_code: Memgraph.ClientError.MemgraphError.MemgraphError} {message: There is no procedure named 'mg.info'.} {gql_status: 50N42} {gql_status_description: error: general processing exception - unexpected error. There is no procedure named 'mg.info'.}) (in-RAM)
* platform: macOS-26.6.2-arm64-arm-64bit, 10 CPUs, Python 3.10.0

## Anchor degrees (out-degree in the loaded slice)

| anchor | out-degree |
|---|---|
| person | 1,103 |
| horse | 574 |
| body | 364 |
| cell | 357 |
| plant | 318 |
| human | 310 |
| blood | 200 |
| desert | 157 |
| beaver | 155 |
| animals | 81 |
| gene | 40 |
| mitochondrion | 27 |
| photosynthesis | 22 |
| enzyme | 7 |

## Results

| query | mechanism (course concept) | expected | SQL med (ms) | Cypher med (ms) | ratio | measured winner | rows | verify | LOC S/C | plan |
|---|---|---|---|---|---|---|---|---|---|---|
| A1 | equality selection on the unique key (baseline) | tie | 0.25 | 32.34 | 128.32x | sql | 1 | OK | 2/3 |  |
| A2 | 1-hop fan-out + tuple reassembly (join-backs) | slight graph | 0.51 | 1.64 | 3.21x | sql | 22 | OK | 8/3 | index |
| A3 | discriminated access (relation id vs. edge type) | tie | 0.39 | 0.95 | 2.44x | sql | 19 | OK | 6/3 |  |
| A4a | directed (reverse) 1-hop: second index vs. free adjacency | tie | 0.47 | 0.93 | 1.98x | sql | 15 | OK | 6/3 |  |
| A4b | undirected 1-hop: OR predicate vs. one pattern | graph | 5.54 | 1.08 | 0.19x | graph | 22 | OK | 7/3 | index |
| A5 | two-anchor intersection (join topology in the pattern) | tie (readability: graph) | 3.70 | 1.19 | 0.32x | graph | 3 | OK | 8/4 |  |
| A6 | range selection on a sorted index | MySQL | 133.80 | 728.32 | 5.44x | sql | 60507 | OK | 3/4 |  |
| A7 | set difference (anti-join vs. negated pattern) | near tie (readability: graph) | 1.23 | 0.46 | 0.38x | graph | 6 | OK | 11/4 | index |
| B1k1 | 1-hop expansion (index-join chain vs. traversal) | near tie | 0.37 | 0.43 | 1.17x | sql | 7 | OK | 6/4 |  |
| B1k2 | 2-hop expansion (index-join chain vs. traversal) | graph, gap grows with k | 0.38 | 0.51 | 1.34x | sql | 22 | OK | 7/4 | index |
| B1k3 | 3-hop expansion (index-join chain vs. traversal) | graph, gap grows with k | 0.54 | 0.93 | 1.71x | sql | 63 | OK | 8/4 |  |
| B1k4 | 4-hop expansion (index-join chain vs. traversal) | graph, gap grows with k | 2.78 | 2.32 | 0.83x | graph | 100 | OK | 9/4 | index |
| B2 | transitive closure (recursive CTE vs. *1..d) | graph, modest | 0.33 | 0.40 | 1.22x | sql | 12 | OK | 11/3 |  |
| B3 | shortest chain, undirected, any relation (depth-bounded) | graph (expressiveness) | 1638.09 | 89.96 | 0.05x | graph | 1 | OK | 15/3 |  |
| B4 | weighted path scoring — expressiveness boundary | no SQL counterpart | — | 4.29 | — | graph (no SQL counterpart) | 5 | n/a | —/3 |  |
| B5 | anchored triangle (cyclic pattern) | graph, clearly | 18.81 | 19.64 | 1.04x | tie | 51 | OK | 12/3 | index |
| B6 | traversal feeding aggregation (bags vs sets!) | graph, moderate | 15.05 | 6.01 | 0.40x | graph | 50 | OK | 8/3 | index |
| C1 | hub ranking (full scan + one-pass grouping) | MySQL | 901.92 | 565.30 | 0.63x | graph | 10 | OK | 4/3 | index |
| C2 | stored vs. derived grouping key (relation x class) | either — trade-off is the finding | 591.38 | 169.98 | 0.29x | graph | 110 | OK | 5/6 | index |
| D1 | violation check (view vs. meta-graph anti-join) | qualitative | 66.27 | 4659.21 | 70.30x | sql | 1 | OK | 1/7 |  |
| D2 | invalid insert: write-time enforcement vs. read-time detection | qualitative — engine vs. query | 75.20 | 4664.76 | 62.03x | sql | 4 | OK during=1;after=0 | 0/0 |  |
| D3 | concept delete: ordering+transaction vs. atomic cascade | qualitative — referential semantics | 2.45 | 30.39 | 12.39x | sql | 2 | OK deleted_edges=3 | 0/0 |  |

## Write demos (D2/D3) — step detail

**D2 / memgraph** (parity: during=1;after=0)

| step | outcome | ms |
|---|---|---|
| create 2 demo EntityNodes + invalid HasProperty edge -> ACCEPTED silently | ok | 3.213 |
| violation check vs :Schema meta-graph (read-time detection) | violations=1 | 4661.545 |

**D2 / mysql** (parity: during=1;after=0)

| step | outcome | ms |
|---|---|---|
| insert(a): truthful class E2E, unpermitted combo -> ACCEPTED (permitted=0) | ok | 2.549 |
| violation check: v_edges NOT permitted (read-time visibility) | violations=1 | 69.749 |
| insert(b): lying edge_class E2P -> ERROR 3819 (CHECK chk_edges_class) | errno 3819 | 0.668 |
| insert(c): lying subject_type -> ERROR 1452 (composite FK) | errno 1452 | 2.237 |

**D3 / memgraph** (parity: deleted_edges=3)

| step | outcome | ms |
|---|---|---|
| count demo edges (pre-delete) | edges=3 | 0.000 |
| DETACH DELETE — single atomic cascade | ok | 30.386 |

**D3 / mysql** (parity: deleted_edges=3)

| step | outcome | ms |
|---|---|---|
| wrong order: DELETE parent with children present -> ERROR 1451 | errno 1451 | 0.845 |
| correct order: tx { DELETE children; DELETE parent; COMMIT } | ok | 1.608 |

## Verification

* read queries: result sets are canonicalized (sorted, floats rounded to 4) and compared by hash across systems; write demos: state signatures (violations during/after, deleted edges) must match.** All matched.**

## Write-demo state check

```json
{
  "before": {
    "mysql": {
      "nodes": 291746,
      "edges": 454139
    },
    "memgraph": {
      "nodes": 291746,
      "edges": 454139
    }
  },
  "after": {
    "mysql": {
      "nodes": 291746,
      "edges": 454139
    },
    "memgraph": {
      "nodes": 291746,
      "edges": 454139
    }
  },
  "ok": true
}
```

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
      "value": "1.78GiB"
    },
    {
      "storage info": "peak_memory_res",
      "value": "2.71GiB"
    },
    {
      "storage info": "disk_usage",
      "value": "456.60MiB"
    },
    {
      "storage info": "memory_tracked",
      "value": "1.31GiB"
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
      "value": "1.31GiB"
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
* D1 is qualitative: both 0 under STRICT_CONTRACT. D2/D3 are write demos: the engines are SUPPOSED to disagree on the write outcome (3819/1452/1451 vs. silent accept) — the verification compares the shared state signatures, and the step tables above are the reportable result.
