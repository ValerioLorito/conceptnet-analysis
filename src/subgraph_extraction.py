#!/usr/bin/env python3
"""
N-hop subgraph extraction around a semantic field (default: biology).

Outputs
-------
    <OUTPUT_FILE>            relation, subject, object, weight
    <CONCEPTS_OUTPUT_FILE>   uri, name, label, pos

The edge file keeps the raw ConceptNet URIs as subject / object, so
downstream stages (POS labelling, Memgraph loading) have the full
information they need.

POS handling
------------
`pos` is derived from the ConceptNet URI suffix when present
(/n -> n, /v -> v, /a|/s -> a, /r -> r). When the URI does not carry
an unambiguous POS suffix, `pos` is left EMPTY. Filling those rows
is the responsibility of the dedicated POS-tagging stage
(label_concepts.py); this script never guesses.

Configuration
-------------
  * ROOTS                    -- seed names (bare names, not URIs);
                                resolved to URIs during Pass -1
  * MAX_HOP                  -- hop radius
  * SUPER_HUBS               -- URI names never expanded (edges kept)
  * MAX_DEGREE_FOR_EXPANSION -- nodes above this degree are never expanded
  * EXPANSION_EDGE_CAP       -- max incident edges a single node may use
                                to expand (soft per-node budget)
  * MIN_EDGE_WEIGHT          -- drop very weak assertions
  * RELATION_WHITELIST       -- if non-empty, only these relations are used
"""

import csv
import re
import time
from collections import defaultdict

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MAX_HOP = 2

INPUT_FILE  = "data/preprocessed/conceptnet_english_cleaned.csv"
OUTPUT_FILE = f"data/preprocessed/conceptnet_science_{MAX_HOP}hop.csv"
CONCEPTS_OUTPUT_FILE = (
    f"data/preprocessed/conceptnet_science_{MAX_HOP}hop_concepts.csv"
)

# --- Seeds ----------------------------------------------------------------
# Bare names. They are resolved to ConceptNet URIs during Pass -1.
# Add / remove names here freely; the resolution is done automatically.
ROOTS = {
    "biology",
    "organism",
    "cell",
    "gene",
    "species",
    "evolution",
    "ecology",
    "anatomy",
    "botany",
    "zoology",
    "genetics",
    "microbiology",
    "physiology",
    "biochemistry",
    "biome",
    "biodiversity",
    "ecosystem",
    "habitat",
    "population",
    "adaptation",
    "mutation",
    "natural_selection",
    "environment",
    "animal",
    "metabolism",
    "photosynthesis",
    "reproduction",
    "conservation",
}

# --- Generic concepts that must not be expanded --------------------------
# These are compared by NAME (the middle segment of the URI), so you can
# list them without worrying about POS suffixes.
SUPER_HUBS = set()
# SUPER_HUBS = {
#     "person", "people", "object", "objects", "entity", "entities",
#     "thing", "things", "location", "locations",
#     "word", "words", "concept", "concepts",
#     "idea", "ideas", "human", "humans",
#     "everything", "anything", "something", "nothing",
#     "part", "parts", "kind", "kinds", "type", "types",
#     "science", "nature", "life", "world", "universe",
#     "knowledge", "study", "research", "field", "subject",
#     "process", "system", "structure", "function", "change",
#     "cause", "effect", "event", "activity",
# }

# --- Degree cap -----------------------------------------------------------
MAX_DEGREE_FOR_EXPANSION = 0     # 0 = disabled

# --- Per-node expansion edge cap -----------------------------------------
EXPANSION_EDGE_CAP = 100000      # effectively off for this graph

# --- Edge weight threshold ------------------------------------------------
MIN_EDGE_WEIGHT = 0.0

# --- Relation whitelist (optional) ----------------------------------------
RELATION_WHITELIST = set()
# RELATION_WHITELIST = {
#     "IsA", "PartOf", "HasA", "MadeOf",
#     "AtLocation", "LocatedNear",
#     "HasProperty", "NotHasProperty",
#     "CapableOf", "NotCapableOf", "UsedFor", "ReceivesAction",
#     "HasSubevent", "HasFirstSubevent", "HasLastSubevent",
#     "HasPrerequisite", "Entails", "MannerOf", "MotivatedByGoal",
#     "Causes", "CausesDesire",
#     "DerivedFrom", "RelatedTo", "SimilarTo", "Synonym", "Antonym",
# }


# ---------------------------------------------------------------------------
# ConceptNet URI helpers
# ---------------------------------------------------------------------------

URI_RE = re.compile(r"^/c/([a-z]{2,3})/(.+)/([nvars])$")

POS_TO_LABEL = {
    "n": "Entity",
    "v": "ActionEvent",
    "a": "Property",
    "s": "Property",
    "r": "Property",
}


def parse_uri(uri):
    """
    Return (name, pos) from a ConceptNet URI.

    ConceptNet URI forms:
      /c/en/name
      /c/en/name/pos
      /c/en/name/pos/source
      /c/en/name/pos/source/sense
      ...
    """
    parts = uri.split("/")

    # Expected: ["", "c", lang, name, ...]
    if len(parts) >= 4 and parts[1] == "c":
        name = parts[3]
        pos = ""

        # POS is the next segment only if it is a valid ConceptNet POS tag
        if len(parts) >= 5 and parts[4] in {"n", "v", "a", "r", "s"}:
            pos = parts[4]

        return name, pos

    # Fallback for unexpected URIs
    return (parts[-1] if parts else uri), ""


def name_of(uri):
    """Convenience: just the name segment of a URI."""
    return parse_uri(uri)[0]


def make_concept_record(uri):
    name, pos = parse_uri(uri)
    return {
        "uri":   uri,
        "name":  name,
        "label": name.replace("_", " "),
        "pos":   pos,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def is_expandable(uri, degree, root_uris):
    """Return True if this node may be used as an expansion frontier."""
    if uri in root_uris:
        return True
    if name_of(uri) in SUPER_HUBS:
        return False
    if MAX_DEGREE_FOR_EXPANSION and degree.get(uri, 0) > MAX_DEGREE_FOR_EXPANSION:
        return False
    return True


def has_budget(uri, counts):
    if not EXPANSION_EDGE_CAP:
        return True
    return counts[uri] < EXPANSION_EDGE_CAP


def compute_degrees(path):
    """One full pass: URI -> undirected degree."""
    print("Pass 0: computing node degrees ...")
    degree = defaultdict(int)
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) < 4:
                continue
            _, subj, obj, _ = row
            degree[subj] += 1
            degree[obj] += 1
    print(f"  degree table size: {len(degree):,}")
    return degree


def resolve_roots(path, seed_names):
    """
    Pass -1: scan the input once and return the set of URIs whose name
    segment matches any seed name. Also reports which seeds were not
    found in the graph (useful to catch typos).
    """
    print("Pass -1: resolving seed names to URIs ...")
    seeds_lower = {s.lower() for s in seed_names}
    found_names = set()
    root_uris = set()

    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            if len(row) < 4:
                continue
            _, subj, obj, _ = row
            for uri in (subj, obj):
                name, _pos = parse_uri(uri)
                if name.lower() in seeds_lower:
                    root_uris.add(uri)
                    found_names.add(name.lower())

    missing = seeds_lower - found_names
    print(f"  resolved {len(root_uris):,} URIs from "
          f"{len(found_names)}/{len(seeds_lower)} seed names")
    if missing:
        print(f"  ⚠️  seeds not present in graph: {sorted(missing)}")
    return root_uris


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    t0 = time.time()

    print(f"Starting {MAX_HOP}-hop extraction")
    print(f"  input : {INPUT_FILE}")
    print(f"  edges : {OUTPUT_FILE}")
    print(f"  nodes : {CONCEPTS_OUTPUT_FILE}")
    print(f"  max_hop={MAX_HOP}  "
          f"degree_cap={MAX_DEGREE_FOR_EXPANSION or 'off'}  "
          f"edge_cap={EXPANSION_EDGE_CAP or 'off'}  "
          f"min_weight={MIN_EDGE_WEIGHT}")
    print()

    # --- Pass -1: resolve seed names to URIs -----------------------------
    root_uris = resolve_roots(INPUT_FILE, ROOTS)
    if not root_uris:
        print("No seed URIs found in the input. Aborting.")
        return
    print()

    # --- Pass 0: degrees (only if we need the cap) -----------------------
    if MAX_DEGREE_FOR_EXPANSION:
        degree = compute_degrees(INPUT_FILE)
    else:
        degree = defaultdict(int)

    # --- Hop bookkeeping -------------------------------------------------
    all_in_scope = set(root_uris)
    hop_sizes = {0: len(root_uris)}
    collected_edges = {}          # (rel, subj, obj) -> weight string

    frontier = set(root_uris)

    for hop in range(1, MAX_HOP + 1):
        print(f"Pass {hop}: expanding frontier of {len(frontier):,} nodes ...")
        t_pass = time.time()
        next_hop = set()
        scanned = 0
        expansion_count = defaultdict(int)

        with open(INPUT_FILE, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if len(row) < 4:
                    continue
                scanned += 1
                rel, subj, obj, weight_s = row

                if RELATION_WHITELIST and rel not in RELATION_WHITELIST:
                    continue

                subj_in_frontier = subj in frontier
                obj_in_frontier = obj in frontier
                if not subj_in_frontier and not obj_in_frontier:
                    continue

                if MIN_EDGE_WEIGHT:
                    try:
                        if float(weight_s) < MIN_EDGE_WEIGHT:
                            continue
                    except ValueError:
                        continue

                sig = (rel, subj, obj)

                # --- Subject side ------------------------------------
                if subj_in_frontier and has_budget(subj, expansion_count):
                    if sig not in collected_edges:
                        collected_edges[sig] = weight_s
                    other = obj
                    if other not in all_in_scope and other not in next_hop:
                        if is_expandable(other, degree, root_uris):
                            next_hop.add(other)
                        else:
                            all_in_scope.add(other)
                    expansion_count[subj] += 1

                # --- Object side (independent budget) ----------------
                if obj_in_frontier and has_budget(obj, expansion_count):
                    if sig not in collected_edges:
                        collected_edges[sig] = weight_s
                    other = subj
                    if other not in all_in_scope and other not in next_hop:
                        if is_expandable(other, degree, root_uris):
                            next_hop.add(other)
                        else:
                            all_in_scope.add(other)
                    expansion_count[obj] += 1

        hop_sizes[hop] = len(next_hop)
        all_in_scope |= next_hop
        frontier = next_hop

        n_used = len(expansion_count)
        n_capped = sum(
            1 for v in expansion_count.values()
            if EXPANSION_EDGE_CAP and v >= EXPANSION_EDGE_CAP
        )

        print(f"  scanned {scanned:,} rows in {time.time() - t_pass:.1f}s")
        print(f"  hop-{hop} nodes discovered : {len(next_hop):,}")
        print(f"  cumulative subgraph nodes  : {len(all_in_scope):,}")
        print(f"  cumulative subgraph edges  : {len(collected_edges):,}")
        print(f"  nodes that used budget     : {n_used:,}")
        print(f"  nodes that hit the cap     : {n_capped:,}")
        print()

        if not frontier:
            print(f"  Frontier is empty at hop {hop}; stopping early.")
            break

    # --- Build the concept table from the collected edges ----------------
    # We only emit concepts that actually appear in some edge, which by
    # construction is exactly `all_in_scope`.
    print(f"Building concept table for {len(all_in_scope):,} nodes ...")
    concepts = {}
    for uri in all_in_scope:
        concepts[uri] = make_concept_record(uri)

    # Diagnostics: how many concepts have no POS from the URI?
    no_pos = [c for c in concepts.values() if c["pos"] == ""]
    print(f"  concepts with URI-derived POS : "
          f"{len(concepts) - len(no_pos):,}")
    print(f"  concepts needing POS tagging  : {len(no_pos):,}")

    # --- Write edges -----------------------------------------------------
    print(f"Writing {len(collected_edges):,} edges to {OUTPUT_FILE} ...")
    with open(OUTPUT_FILE, "w", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(["relation", "subject", "object", "weight"])
        for (rel, subj, obj), weight in sorted(collected_edges.items()):
            writer.writerow([rel, subj, obj, weight])

    # --- Write concepts --------------------------------------------------
    print(f"Writing {len(concepts):,} concepts to "
          f"{CONCEPTS_OUTPUT_FILE} ...")
    with open(CONCEPTS_OUTPUT_FILE, "w", encoding="utf-8", newline="") as f_out:
        writer = csv.writer(f_out)
        writer.writerow(["uri", "name", "label", "pos"])
        for uri, c in sorted(concepts.items()):
            writer.writerow([c["uri"], c["name"], c["label"], c["pos"]])

    # --- Summary ---------------------------------------------------------
    print()
    print("Done.")
    print(f"  hops executed : {max(hop_sizes)}")
    for h in sorted(hop_sizes):
        print(f"    hop {h:<2} nodes : {hop_sizes[h]:>10,}")
    print(f"  total nodes   : {len(all_in_scope):>10,}")
    print(f"  total edges   : {len(collected_edges):>10,}")
    print(f"  elapsed       : {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()