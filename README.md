# ConceptNet 5.7 — Typed Subgraph for MySQL & Memgraph

Reproducible pipeline that carves a biology-oriented subgraph out of
ConceptNet 5.7, types every concept as **EntityNode / ActionEventNode /
PropertyNode**, classifies every edge into one of 9 endpoint-type classes,
loads the *same* data into a relational schema (MySQL) and a property graph
(Memgraph) under identical identity rules — and benchmarks the two systems
with a **cross-verified query battery** (time, memory, expressiveness,
integrity semantics) for a commonsense-QA grounding use case.

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
        ├────────────────────────┬─────────────────────────┐
        ▼                        ▼                         ▼
4a. sql/conceptnet_schema.sql   4b. src/build_memgraph.py   5. src/benchmark.py
    + src/build_mysql.py           (Cypher, planner-          (query battery,
    (MySQL, InnoDB +               agnostic loader +          both systems,
     FKs/CHECKs/views)             :Schema meta-graph)        verified results)
                                                                  │
                                                                  ▼
                                                        results/ (report, csv,
                                                        json, plans/)
```

Both loaders consume **exactly the same two CSVs** and apply the same rules:

- **node identity** = ConceptNet URI (sense level; `abandon/n` and
  `abandon/v` are distinct nodes);
- **edge identity** = (subject, relation, object); duplicate triples keep
  the **max weight** (SQL: `ON DUPLICATE KEY UPDATE ... GREATEST(...)`;
  Cypher side: the rule is enforced in the loader before creation);
- **typing rule** `n→EntityNode, v→ActionEventNode, a|s|r→PropertyNode`
  (SQL enforces it in-engine via `CHECK chk_nodes_pos_type`; the graph
  maintains it in the loader and verifies it with a post-load query);
- **a priori edge contract**: each relation may only realize certain
  endpoint-type classes. Under `STRICT_CONTRACT = True` (default) violating
  edges are *not* loaded in either system; they remain recorded in
  `typed_edges.csv` (`permitted=0`) and `pipeline_stats.json` (27,567 in
  the reference slice).

The **contract has five materializations** that must stay in sync —
`RELATION_TO_EDGE_CLASSES` (dict, source of truth), the `permitted` column
(CSV snapshot), `relation_edge_classes` (declared grants), 
`relation_class_perms` (expanded grants, what `v_edges` reads), and the
`:Schema` meta-graph. Both loaders *verify* sync at startup rather than
trusting it (see *Known incidents* below).

## The query battery (`src/benchmark.py`)

One **mechanism per query** (course-concept aligned), 18 queries in four
families, mirroring `Queries.pdf`:

| Family | Queries | What it isolates |
|---|---|---|
| A — Local retrieval | A1 URI lookup · A2 full relation table · A3 relation 1-hop · A4a/A4b directed/undirected · A5 pair intersection · A6 range selection · A7 set difference | index equality, tuple reassembly, composite-key prefix, bidirectional access, join topology, range scan, anti-join |
| B — Traversal paths | B1 k-hop (k=1..4) · B2 transitive closure · B3 shortest chain · B4 weighted scoring · B5 anchored triangle · B6 neighborhood degree profile | expansion cost, recursion vs. variable-length, path operators (expressiveness), cyclic patterns, traversal feeding aggregation |
| C — Global retrieval | C1 hub ranking · C2 relation-class matrix | full scan + one-pass grouping; stored vs. derived grouping key |
| D — Schema & integrity | D1 violation check (timed) · D2 invalid insert · D3 concept delete (both **manual** write demos) | where the schema lives; write-time vs. read-time enforcement |

**Protocol:** per query, 2 warm-up runs discarded + 10 measured (3/1 with
`--quick`; B3/B4 use 3 reps, B5 uses 5); **median** reported with
min/max/mean/stdev. MySQL timings are warm-buffer by default; Memgraph is
in-RAM by architecture — state this asymmetry when reporting.

**Result verification is built in:** both systems' result sets are
canonicalized (sorted, floats rounded) and compared by hash — a `verify`
MISMATCH means a query bug or non-isomorphism, and no timing from that run
should be trusted until resolved. Queries where *both* systems return 0
rows verify OK but benchmark nothing: the harness warns; substitute anchors
in `ANCHORS` (top of the file) — check candidates first with
`anchor degrees` in the generated report.

**Plans are captured** (`EXPLAIN ANALYZE` / `PROFILE`) into `results/plans/`
for the key queries, with heuristic flags (`range`, `ref`, `ScanAll(!)`)
surfaced into the report table — inspect them before interpreting timings.

**Depth bound:** B3/B4 run at `SP_MAX_DEPTH = 3` (override with
`--sp-depth`). Depth 5 on undirected all-relation traversals is
exponential — see *Known incidents*. Keep `Queries.pdf` in sync with the
depth actually executed.

```bash
# 0. sanity: is the anchor pair connected at the chosen depth?
python src/inspect_query.py B3            # rows >= 1 required

# 1. smoke test (~1 min): all verify OK, no zero-row warnings
python src/benchmark.py --quick

# 2. full battery
python src/benchmark.py

# 3. headline figure data (B1 over low/med/high-degree anchors)
python src/benchmark.py --stratify-b1 --out-dir results/bench_strat

# extras: subset, more reps, single system
python src/benchmark.py --queries A2,B1k4 --reps 20
python src/benchmark.py --mysql-only
```

The harness is **read-only** (D2/D3 stay manual so it can be re-run
freely), and saves results incrementally — a Ctrl-C keeps everything
measured so far.

`src/inspect_query.py` runs any battery query on **both** systems, prints
the rows side by side and a canonical diff — for eyeballing outputs before
or after a benchmark run (do not use *during*: it would perturb timings).

Outputs (in `--out-dir`, default `results/`):

| File | Contents |
|---|---|
| `benchmark_report.md` | paste-ready table: expected vs. measured winner, medians, ratio, rows, verify, LOC, plan flags; anchor degrees; storage; reading guide |
| `benchmark_results.csv` | one row per (query, system) — for plotting |
| `benchmark_results.json` | raw per-rep timings, environment, storage; rewritten after every query |
| `plans/<qid>_<system>.txt` | resolved query text + physical plan |

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
│   ├── build_memgraph.py
│   ├── benchmark.py
│   └── inspect_query.py
├── results/               benchmark artifacts (regenerable)
└── README.md
```

> `pos_tagger.py` and `prepare_graph_inputs.py` were **legacy** (superseded
> by `label_concepts.py`) and are not part of the pipeline — delete them if
> still present; a superseded script with live output paths is a landmine,
> not documentation.

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

  In-memory by default; the loader wipes and rebuilds each run, so no
  persistence is needed for reproduction. On Docker for Mac, remember the
  VM has its **own** disk cap — if it fills, `docker system prune` and
  restart Docker Desktop (see Troubleshooting).
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

Preflight verifies **before any data is touched**: server version (ODKU
dialect selection), `utf8mb4_bin` collation on `nodes.uri/name`, dict ↔
`relation_edge_classes` sync, and `relation_class_perms` ↔ its expansion.
Then: truncate, batch-insert with the max-weight dedup, `ANALYZE TABLE`,
and a post-load report with isomorphism and permission-parity checks.

### Step 4b — Memgraph

```bash
python src/build_memgraph.py
```

Wipes the store, creates the `:Concept(uri)` uniqueness constraint +
property indexes, loads nodes with `CREATE` (uniqueness validated in
Python — no property matching), picks the edge-loading strategy with a
**behavioral plan canary** (by-id seeks vs. parameterized lookups — see
*Known incidents*), applies the same strict-contract filter and max-weight
dedup as the SQL loader, stores the declared contract as the `:Schema`
meta-graph, and prints the mirror of the MySQL report.

### Step 5 — Benchmark

```bash
python src/inspect_query.py B3     # anchor pair connected at sp-depth?
python src/benchmark.py --quick    # all verify OK?
python src/benchmark.py            # full battery → results/
```

## Reference run & parity verification

Reference values from our run (yours must match all three sources):

| Metric | `pipeline_stats.json` | MySQL report | Memgraph report |
|---|---|---|---|
| nodes | `nodes_kept` = 291,746 | 291,746 | 291,746 |
| typed edge rows | `edges_kept` = 481,706 | 481,706 eligible | 481,706 eligible |
| contract violations | 27,567 (`permitted=0`) | filtered at load; `v_edges NOT permitted` = **0** | filtered at load; meta-graph check = **0** |
| edges stored (strict) | ≈ 454,139 (− duplicates) | 454,139 | 454,139 (**Concept-scoped**) |
| label provenance | `label_sources` | `GROUP BY label_source` | identical |

**Contract-vocabulary note:** with the full documented relation vocabulary
the sync/meta-graph lines read *44 grants over 40 relations* / *124
expanded grants*; after the dump verification that removed `PropertyOf`
(and optionally `LocationOf` — both documented in ConceptNet's relation
list but never materialized in the 5.7 assertions), they read *42/39 and
122* (or *41/38 and 121*). **All data counts above are unaffected either
way.** The 3×3 class partition is complete *by design*: classes with zero
population in a slice (P2P, P2E, P2A, A2E here) are empirical findings
that query C2 reports, not redundancy — do not remove them.

A run is **reproducible iff** all rows agree across the three sources; the
loaders check this automatically. For the benchmark, the additional target
is: **every `verify` column reads OK** (B4 is `n/a` — Cypher-only).

## Configuration knobs

| Knob | File | Effect |
|---|---|---|
| `ROOTS`, `MAX_HOP`, caps, `RELATION_WHITELIST` | `subgraph_extraction.py` | shape of the subgraph |
| `--source dump\|api\|wordnet`, `--no-*` | `label_concepts.py` | which cascade tiers run |
| `STRICT_CONTRACT` | **both loaders** (keep in sync) | `True`: violating edges not loaded; `False` (faithful): loaded + queryable in-DB |
| `LOAD_SCHEMA_META` | `build_memgraph.py` | store the declared contract as the `:Schema` meta-graph |
| `BATCH_SIZE` | both loaders | insert batch size (lower if `max_allowed_packet` errors) |
| `MYSQL_*` env vars / `MEMGRAPH_URI` | loaders & benchmark | connection settings |
| `REPS`, `WARMUP`, `--reps/--warmup/--quick` | `benchmark.py` | measurement protocol |
| `SP_MAX_DEPTH`, `--sp-depth` | `benchmark.py` | depth bound for B3/B4 (default 3; higher = exponential) |
| `ANCHORS` | `benchmark.py` | anchor concepts per query — substitute dead ones (zero-row warning) |
| `B1_STRAT_POOL`, `--stratify-b1` | `benchmark.py` | B1 over low/med/high-degree anchors |
| `PLAN_QUERIES`, `--no-plans` | `benchmark.py` | which physical plans are captured |

## Known incidents & the guards they produced

This project's methodology in one table — every silent failure below was
caught by a check that now runs permanently:

| Incident | Symptom | Permanent guard |
|---|---|---|
| Accent-insensitive collation folded distinct URIs | `Duplicate entry '/c/en/oögenetic'` (1062) | `check_collation` preflight (`utf8mb4_bin` enforced) |
| ODKU dialect traps (row-alias ambiguity 1052; no INSERT table alias 1064) | load crash on edges | version-detected dialect; qualified `edges.weight` |
| Memgraph planner compiled UNWIND-driven property matches to full scans | **21-hour load** (≈3×10¹¹ node visits) | planner-agnostic loader + behavioral plan canary |
| Unbounded undirected variable-length traversal | ~30-min B3 + Docker VM freeze | `SP_MAX_DEPTH = 3`, `--sp-depth`, BFS expansion |
| Contract edit desynchronized the five materializations | **61,416 phantom violations** in `v_edges` | `check_perms_expansion` (bidirectional) at every load |
| Harness f-string templates collapsed `{{token}}` | wrong queries sent to both engines | leftover-brace detector + cross-system hash verification |
| Sense-level duplicate names | C1 verify MISMATCH | group by URI (identity), tiebreak by URI |
| Meta-graph pollutes unscoped counts | `454,139 + 140` edges counted | Concept-scoped counts in all reports |
| Engine IDs non-isomorphic by design | would break A1/B3 verification | A1 returns URI; B3 returns hop count |

## Reproducibility notes

- **No randomness** anywhere in the pipeline; extraction is set-based,
  outputs are sorted, `node_id` is the (uri-sorted) file position and
  `TRUNCATE` resets `AUTO_INCREMENT` — repeated runs of the same input
  yield byte-identical databases.
- The **max-weight dedup is order-independent** (`GREATEST` / max-in-Python),
  so insertion order cannot change stored weights.
- **Not reproducible:** `--source api` (external ConceptNet API).
  Reproducible-but-version-sensitive: spaCy/NLTK model versions (pin them);
  the `label_source` column quantifies exactly how much they matter.
- **Contract edits propagate to five places** (dict, CSV regeneration,
  schema re-application, perms rebuild, meta-graph) — or the loaders refuse
  to run. This is enforced, not advised.
- Benchmark timings: warm MySQL vs. always-in-RAM Memgraph; medians over
  10 reps; plans captured as evidence. Timings are machine-specific — the
  *verification* results (OK/MISMATCH, row counts) are the reproducible
  part.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Duplicate entry ... for key 'nodes.uq_nodes_uri'` (1062) | CI collation — apply the `utf8mb4_bin` schema (loader blocks this in preflight) |
| `Column 'weight' ... ambiguous` (1052) / syntax near `AS e` (1064) | stale `build_mysql.py` — use the current one |
| `TypeError: ... unexpected keyword argument 'plan'` / `Decimal is not JSON serializable` | stale `benchmark.py` — use the current one |
| `relation_class_perms is out of sync ...` / `v_edges NOT permitted` ≠ 0 | stale expansion — re-apply `conceptnet_schema.sql`, re-run the loader |
| benchmark `verify: MISMATCH` | query bug or non-isomorphism — resolve **before** trusting any timing (check loader reports first) |
| benchmark `[WARN] both systems returned 0 rows` | dead anchors — substitute in `ANCHORS` |
| B3 runs "forever" | depth too high — keep `--sp-depth 3`; depth 5 is exponential |
| Memgraph counts look wrong (+140 edges / +51 nodes) | unscoped counts include the `:Schema` meta-graph — use Concept-scoped queries |
| Memgraph empty after restart | in-memory store was wiped — re-run `build_memgraph.py` (or enable snapshots/WAL) |
| Docker "disk full" though the Mac isn't | Docker VM's own disk cap — `docker system prune`, restart Docker Desktop; consider `--log-opt max-size=10m` |
| Memgraph load takes hours | planner is scanning (old loader, or a build where the canary misjudges) — check the `plan canary:` line and the four `ok:` index lines |
| `Access denied` / `Schema incomplete` / packet errors | env vars / apply the schema first / lower `BATCH_SIZE` |
| `graph_filtering.py` finds no input | decompressed filename mismatch — see Step 1 |
````

Two notes on choices I made: (1) the reference table's contract numbers are written to cover both the 40-relation and post-PropertyOf-removal states, since I don't know which your repo settled on — if you tell me, I'll pin the exact single set of numbers; (2) I left the `requirements.lock.txt` / `Makefile` offer out of the README itself (it's meta-documentation, not usage) — say the word and I'll generate the Makefile so `make data`, `make mysql`, `make graph`, `make bench` encode the step order above.