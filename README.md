# ConceptNet 5.7 — Typed Subgraph for MySQL & Memgraph

Reproducible pipeline that carves a biology-oriented subgraph out of
ConceptNet 5.7, types every concept as **EntityNode / ActionEventNode /
PropertyNode**, classifies every edge into one of 9 endpoint-type classes,
and loads the *same* data into a relational schema (MySQL) and a property
graph (Memgraph) under identical identity rules — so the two systems can be
benchmarked fairly (time, memory, expressiveness) for a commonsense-QA
grounding use case.

## Pipeline overview

```
conceptnet-assertions-5.7.0.csv.gz            (data/original — downloaded)
        │
        ▼  1. src/graph_filtering.py           English-only edge filter
conceptnet_english_cleaned.csv                (data/preprocessed)
        │
        ▼  2. src/subgraph_extraction.py       2-hop BFS around biology seeds
conceptnet_science_2hop.csv                   (+ _concepts.csv, informational)
        │
        ▼  3. src/label_concepts.py            POS typing cascade + edge
        │                                      classification + a priori
        │                                      contract flagging
typed_nodes.csv, typed_edges.csv,             (data/preprocessed)
dropped_concepts.csv, pipeline_stats.json
        │
        ├────────────────────────┐
        ▼                        ▼
4a. sql/conceptnet_schema.sql   4b. src/build_memgraph.py
    + src/build_mysql.py           (Memgraph, Cypher MERGE)
    (MySQL, InnoDB + FKs/CHECKs)
```

Both loaders consume **exactly the same two CSVs** and apply the same rules:

- **node identity** = ConceptNet URI (sense level; `abandon/n` and
  `abandon/v` are distinct nodes);
- **edge identity** = (subject, relation, object); duplicate triples keep
  the **max weight** (SQL: `ON DUPLICATE KEY UPDATE ... GREATEST(...)`,
  Cypher: `MERGE ... ON MATCH SET`);
- **typing rule** `n→EntityNode, v→ActionEventNode, a|s|r→PropertyNode`
  (SQL enforces it in-engine via `CHECK chk_nodes_pos_type`; the graph
  maintains it in the loader and verifies it with a post-load query);
- **a priori edge contract**: each relation may only realize certain
  endpoint-type classes (`RELATION_TO_EDGE_CLASSES` in `label_concepts.py`
  = `relation_edge_classes` in the DDL; the MySQL loader asserts the sync
  at startup). Under `STRICT_CONTRACT = True` (default) violating edges are
  *not* loaded in either system; they remain recorded in
  `typed_edges.csv` (`permitted=0`) and `pipeline_stats.json`.

## Repository layout

```
.
├── data/
│   ├── original/          conceptnet-assertions-5.7.0.csv.gz (downloaded)
│   └── preprocessed/      all pipeline outputs (regenerable)
├── sql/
│   └── conceptnet_schema.sql
├── src/
│   ├── graph_filtering.py
│   ├── subgraph_extraction.py
│   ├── label_concepts.py
│   ├── build_mysql.py
│   └── build_memgraph.py
└── README.md
```

> `pos_tagger.py` and `prepare_graph_inputs.py` are **legacy** (superseded
> by `label_concepts.py`) and are not part of the pipeline.

## Prerequisites

- **Python 3.10+**, then:

  ```bash
  pip install mysql-connector-python neo4j requests tqdm nltk spacy
  python -m spacy download en_core_web_sm      # optional but recommended
  ```

  NLTK data (`wordnet`, `averaged_perceptron_tagger`) downloads
  automatically on first use. **For full reproducibility, pin exact
  versions** (`pip freeze > requirements.lock.txt`) — spaCy/NLTK model
  versions can re-label the small residue of untagged concepts (audit the
  residue via `label_source` in `pipeline_stats.json`).
- **MySQL ≥ 8.0.16** (CHECK constraint enforcement). The schema script
  creates/drops the `conceptnet` database (needs `CREATE`); the loader
  needs `SELECT/INSERT/UPDATE/DELETE/DROP` on it.
- **Memgraph** (any recent version), e.g.:

  ```bash
  docker run -it -p 7687:7687 -p 3000:3000 memgraph/memgraph-platform
  ```

  In-memory by default; since the loader wipes and rebuilds the graph each
  run, no persistence configuration is needed for reproduction.
- **ConceptNet 5.7 assertions dump** → `data/original/`
  (from https://conceptnet.io / the conceptnet5 GitHub downloads page).

## Step-by-step build

Run everything **from the repository root** (all paths are relative).

### Step 1 — English-only filter

`graph_filtering.py` expects the *decompressed* dump by default. Either:

```bash
gunzip -k data/original/conceptnet-assertions-5.7.0.csv.gz
# (produces conceptnet-assertions-5.7.0.csv — the expected name)
```

…or edit `INPUT_FILE` to point at the `.gz` (gzip input is auto-detected).
Then:

```bash
python src/graph_filtering.py
# → data/preprocessed/conceptnet_english_cleaned.csv
```

### Step 2 — 2-hop subgraph extraction

Seeds (`ROOTS`), hop radius (`MAX_HOP = 2`), caps and whitelist are
constants inside the script:

```bash
python src/subgraph_extraction.py
# → conceptnet_science_2hop.csv        (pipeline input)
# → conceptnet_science_2hop_concepts.csv (informational only — NOT consumed
#   downstream; label_concepts.py derives nodes from edge endpoints)
```

### Step 3 — Typing and edge classification

```bash
python src/label_concepts.py
```

Defaults: input `data/preprocessed/conceptnet_science_2hop.csv`, dump
`data/original/conceptnet-assertions-5.7.0.csv.gz`, `--source dump`
(fully offline & deterministic; `--source api` exists but is **not
reproducible** — external service).

Typing policy: URI POS segment first (`label_source='uri'`), then the
cascade dump → spaCy → NLTK → WordNet → suffix heuristics, each level
recorded in `label_source`. Outputs:

| File | Contents |
|---|---|
| `typed_nodes.csv` | `uri, name, pos, node_type, label_source` (uri-sorted) |
| `typed_edges.csv` | `relation, subject, object, weight, edge_class, permitted` |
| `dropped_concepts.csv` | dropped endpoints with reason + edges lost |
| `pipeline_stats.json` | full funnel (audit record of everything filtered) |

### Step 4a — MySQL

```bash
mysql -u root -p < sql/conceptnet_schema.sql

export MYSQL_USER=root MYSQL_PASSWORD=...   # or edit MYSQL_CONFIG
python src/build_mysql.py
```

The loader validates everything before touching the database (fail-fast,
no half-loaded state), truncates `nodes`/`edges`, batch-inserts with the
max-weight dedup rule, runs `ANALYZE TABLE`, and prints a post-load report
with isomorphism and permission-parity checks.

### Step 4b — Memgraph

```bash
python src/build_memgraph.py
```

Creates the `:Concept(uri)` uniqueness constraint + indexes, loads nodes
as `(:Concept:EntityNode|ActionEventNode|PropertyNode)`, edges as one type
per relation (Cypher-safe names only), stores the declared contract as a
`:Schema` meta-graph, and prints the mirror of the MySQL report.

## Reference run & parity verification

Reference values from our run (yours must match all three sources):

| Metric | `pipeline_stats.json` | MySQL report | Memgraph report |
|---|---|---|---|
| nodes | `nodes_kept` = 291,746 | 291,746 | 291,746 |
| typed edge rows | `edges_kept` = 481,706 | 481,706 eligible | 481,706 eligible |
| contract violations | 27,567 (`permitted=0`) | filtered at load; `v_edges NOT permitted` = 0 | filtered at load; meta-graph check = 0 |
| edges stored (strict) | ≈ 454,139 (− duplicates) | same | same |
| label provenance | `label_sources` | `GROUP BY label_source` | identical |

A run is **reproducible iff** all four rows agree across the three
sources; both loaders check this automatically and warn on mismatch.

## Configuration knobs

| Knob | File | Effect |
|---|---|---|
| `ROOTS`, `MAX_HOP`, caps, `RELATION_WHITELIST` | `subgraph_extraction.py` | shape of the subgraph |
| `--source dump\|api\|wordnet`, `--no-*` | `label_concepts.py` | which cascade tiers run |
| `STRICT_CONTRACT` | **both loaders** (keep in sync) | `True`: violating edges not loaded; `False` (faithful): loaded + queryable in-DB |
| `LOAD_SCHEMA_META` | `build_memgraph.py` | store the declared contract as the `:Schema` meta-graph |
| `BATCH_SIZE` | both loaders | insert batch size (lower if `max_allowed_packet` errors) |
| `MYSQL_*` env vars / `MEMGRAPH_URI` | loaders | connection settings |

## Reproducibility notes

- **No randomness** anywhere in the pipeline; extraction is set-based,
  outputs are sorted, `node_id` is the (uri-sorted) file position and
  `TRUNCATE` resets `AUTO_INCREMENT` — repeated runs of the same input
  yield byte-identical databases.
- The **max-weight dedup is order-independent** (`GREATEST` / `CASE`), so
  insertion order cannot change stored weights.
- **Not reproducible:** `--source api` (external ConceptNet API).
  Reproducible-but-version-sensitive: spaCy/NLTK model versions (pin them);
  the `label_source` column quantifies exactly how much they matter.
- MySQL behavior guards: the loader aborts if `nodes.uri/name` are not
  `utf8mb4_bin` (accent-insensitive collations conflate distinct URIs) and
  picks the correct `ON DUPLICATE KEY UPDATE` dialect for your server
  version (≥ 8.0.19 row-alias vs legacy `VALUES()`).

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Duplicate entry ... for key 'nodes.uq_nodes_uri'` (1062) | `nodes.uri` collation is CI — apply the `utf8mb4_bin` schema (the loader now blocks this in preflight) |
| `Column 'weight' ... ambiguous` (1052) / syntax error near `AS e` (1064) | stale loader version — use the current `build_mysql.py` (qualified `edges.weight`, no INSERT table alias) |
| `Access denied` | set `MYSQL_USER`/`MYSQL_PASSWORD` or edit `MYSQL_CONFIG` |
| `Schema incomplete — missing ...` | apply `sql/conceptnet_schema.sql` first |
| Packet-size errors during load | lower `BATCH_SIZE` or raise `max_allowed_packet` |
| Memgraph connection refused | start Memgraph (`docker run -p 7687:7687 memgraph/memgraph`) |
| `graph_filtering.py` finds no input | decompressed filename mismatch — see Step 1 |
````

Two small follow-ups if you want them: I can generate the `requirements.lock.txt`-style pin list from your conda env once you run `pip freeze`, and/or add a `Makefile` (`make data`, `make mysql`, `make graph`, `make all`) that encodes the README's step order so the documented procedure and the executable procedure can never drift apart.