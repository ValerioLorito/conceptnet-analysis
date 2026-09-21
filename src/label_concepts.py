#!/usr/bin/env python3
"""
label_concepts.py — type a ConceptNet subgraph and emit loader-ready files.

This is the single entry point of the preprocessing stage. It reads the raw
2-hop slice (relation, subject, object, weight; endpoints are ConceptNet
URIs), assigns every endpoint a node type, classifies every edge, and writes
the two files consumed by BOTH database loaders:

    typed_nodes.csv   uri, name, pos, node_type, label_source
    typed_edges.csv   relation, subject, object, weight, edge_class, permitted

    conceptnet_science_2hop.csv
                  |
                  |  python label_concepts.py ...
                  v
    typed_nodes.csv + typed_edges.csv + dropped_concepts.csv + pipeline_stats.json
        |                               |
   MySQL loader                  build_memgraph.py   (unchanged)

Typing policy (conceptnet_schema.sql enforces the same rule as CHECK
chk_nodes_pos_type):
    1. the URI carries a POS segment in {n,v,a,r,s} -> use it    label_source='uri'
    2. otherwise the labeling cascade decides:
           ConceptNet dump > ConceptNet API > spaCy > NLTK > WordNet > suffixes
       n -> EntityNode, v -> ActionEventNode, a|s|r -> PropertyNode

Filtering happens HERE, once, upstream of both databases, so the two loads
are isomorphic by construction:
    non-English URIs, stopwords, function-POS concepts (c|p|d|x|t) and
    concepts the cascade cannot resolve are dropped, together with every
    edge touching them; edges whose relation is outside the static schema
    are dropped too. Contract violations are NOT dropped: they are kept and
    flagged (permitted=0) so the D-family queries can find them in both DBs.

The static relation schema (RELATION_TO_EDGE_CLASSES) must stay in sync with
conceptnet_schema.sql (table relation_edge_classes); the MySQL loader
asserts the two match at startup.

Usage
-----
    python label_concepts.py
        (defaults match the project layout; or explicit:)
    python label_concepts.py \
        --input  data/preprocessed/conceptnet_science_2hop.csv \
        --dump   data/original/conceptnet-assertions-5.7.0.csv.gz \
        --out-dir data/preprocessed

    --source dump|api|wordnet   how the cascade is seeded (dump = default)

Dependencies: requests, nltk (+ wordnet data), optionally spacy
              (en_core_web_sm) for better tagging of untagged concepts.
"""

import argparse
import csv
import gzip
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

import requests

# --- NLTK / WordNet --------------------------------------------------------

try:
    import nltk
    from nltk.corpus import wordnet as wn
    _NLTK_AVAILABLE = True
except ImportError:
    print("Please install nltk:  pip install nltk", file=sys.stderr)
    sys.exit(1)

try:
    wn.synsets("dog")
except LookupError:
    nltk.download("wordnet", quiet=True)
    nltk.download("omw-1.4", quiet=True)

# --- spaCy (optional) ------------------------------------------------------

try:
    import spacy
    _SPACY_IMPORT_OK = True
except ImportError:
    spacy = None
    _SPACY_IMPORT_OK = False


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

E = "EntityNode"
A = "ActionEventNode"
P = "PropertyNode"

CONCEPTNET_POS_TO_NODE_TYPE = {"n": E, "v": A, "a": P, "r": P, "s": P}
FUNCTION_POS = {"c", "p", "d", "x", "t"}
TYPE_LETTER = {E: "E", A: "A", P: "P"}

# Python mirror of the edge_classes table in conceptnet_schema.sql.
# (subject_type, object_type) contract per class; None = any (ALL wildcard).
EDGE_CLASS_TYPES = {
    "E2E": (E, E),
    "E2P": (E, P),
    "E2A": (E, A),
    "P2P": (P, P),      # no relation maps here today; kept for completeness
    "P2E": (P, E),
    "P2A": (P, A),
    "A2A": (A, A),
    "A2P": (A, P),
    "A2E": (A, E),      # no relation maps here today; kept for completeness
    "ALL": (None, None),
}

# Relation -> set of edge classes it may realise.
# MUST match the relation_edge_classes seed in conceptnet_schema.sql.
# Note: mixing 'ALL' with concrete classes is rejected by check_static_schema
# below, because 'ALL' makes the concrete entries dead code.
RELATION_TO_EDGE_CLASSES = {
    # --- E2E -------------------------------------------------------------
    "AtLocation":                {"E2E"},
    "DerivedFrom":               {"E2E"},
    "EtymologicallyDerivedFrom": {"E2E"},
    "EtymologicallyRelatedTo":   {"E2E"},
    "HasA":                      {"E2E"},
    "InstanceOf":                {"E2E"},
    "IsA":                       {"E2E"},
    "LocatedNear":               {"E2E"},
    "LocationOf":                {"E2E"},
    "MadeOf":                    {"E2E"},
    "PartOf":                    {"E2E"},
    "SymbolOf":                  {"E2E"},
    # --- E2P / A2P -------------------------------------------------------
    "DefinedAs":                 {"E2E", "E2P"},
    "HasProperty":               {"E2P", "A2P"},
    "NotHasProperty":            {"E2P", "A2P"},
    # --- E2A -------------------------------------------------------------
    "CapableOf":                 {"E2A"},
    "CreatedBy":                 {"E2A"},
    "Desires":                   {"E2A"},
    "NotCapableOf":              {"E2A"},
    "NotDesires":                {"E2A"},
    "ReceivesAction":            {"E2A"},
    "UsedFor":                   {"E2A"},
    # --- P2E / P2A -------------------------------------------------------
    "PropertyOf":                {"P2E", "P2A"},
    # --- A2A -------------------------------------------------------------
    "Entails":                   {"A2A"},
    "HasFirstSubevent":          {"A2A"},
    "HasLastSubevent":           {"A2A"},
    "HasPrerequisite":           {"A2A"},
    "HasSubevent":               {"A2A"},
    "MannerOf":                  {"A2A"},
    "MotivatedByGoal":           {"A2A"},
    # --- Wildcard (ALL) --------------------------------------------------
    "Antonym":                   {"ALL"},
    "Causes":                    {"ALL"},
    "CausesDesire":              {"ALL"},
    "dbpedia":                   {"ALL"},
    "DistinctFrom":              {"ALL"},
    "FormOf":                    {"ALL"},
    "HasContext":                {"ALL"},
    "RelatedTo":                 {"ALL"},
    "SimilarTo":                 {"ALL"},
    "Synonym":                   {"ALL"},
}

STOPWORD_CONCEPTS = {
    "a", "an", "the", "and", "or", "not",
    "be", "is", "are", "was", "were", "am",
    "do", "does", "did", "done",
    "have", "has", "had", "having",
    "of", "in", "on", "at", "to", "for", "with", "by", "from",
    "if", "but", "so", "as", "that", "this", "these", "those",
    "it", "its", "he", "she", "they", "them", "his", "her",
    "i", "you", "we", "us", "me", "my", "your", "our",
    "thing", "things", "someone", "something", "anything",
    "anyone", "everyone", "everything", "somewhere", "anywhere",
    "nothing", "nobody", "none", "no",
}

_WEIGHT_RE = re.compile(r'"weight"\s*:\s*([0-9eE.+\-]+)')


def check_static_schema():
    """Guard the invariants of RELATION_TO_EDGE_CLASSES (sync with the DDL)."""
    for rel, classes in RELATION_TO_EDGE_CLASSES.items():
        if not classes or not classes <= set(EDGE_CLASS_TYPES):
            raise SystemExit(
                f"static schema: {rel} references unknown classes {classes!r}")
        if "ALL" in classes and len(classes) > 1:
            raise SystemExit(
                f"static schema: {rel} mixes 'ALL' with concrete classes; "
                f"'ALL' makes the concrete entries dead code")


# ---------------------------------------------------------------------------
# POS tagging cascade: spaCy, NLTK, heuristics
# ---------------------------------------------------------------------------

# spaCy Universal POS -> ConceptNet POS.
SPACY_UPOS_TO_CN_POS = {
    "NOUN": "n", "PROPN": "n",
    "VERB": "v", "AUX": "v",
    "ADJ":  "a",
    "ADV":  "r",
    # Everything else (DET, ADP, PRON, CCONJ, SCONJ, PART, NUM, INTJ,
    # PUNCT, SYM, X, SPACE) is intentionally unmapped.
}

# Penn Treebank -> ConceptNet POS, for NLTK's pos_tag.
PTB_TO_CN_POS = {
    "NN": "n", "NNS": "n", "NNP": "n", "NNPS": "n",
    "VB": "v", "VBD": "v", "VBG": "v", "VBN": "v", "VBP": "v", "VBZ": "v",
    "JJ": "a", "JJR": "a", "JJS": "a",
    "RB": "r", "RBR": "r", "RBS": "r",
}

_NLP = None
_NLP_LOAD_FAILED = False


def _load_spacy():
    """Lazily load spaCy's small English model. Returns None on failure."""
    global _NLP, _NLP_LOAD_FAILED
    if _NLP is not None:
        return _NLP
    if _NLP_LOAD_FAILED or not _SPACY_IMPORT_OK:
        return None
    try:
        # Only tagger + parser: tagger gives POS, parser gives noun_chunks
        # and the syntactic root for head-of-phrase selection.
        _NLP = spacy.load("en_core_web_sm",
                          disable=["ner", "textcat", "lemmatizer"])
        return _NLP
    except OSError:
        print("  [WARN] spaCy model 'en_core_web_sm' not installed; "
              "falling back to NLTK/WordNet.", file=sys.stderr)
        print("         Install with:  python -m spacy download en_core_web_sm",
              file=sys.stderr)
        _NLP_LOAD_FAILED = True
        return None
    except Exception as exc:
        print(f"  [WARN] spaCy failed to load ({exc}); "
              f"falling back to NLTK/WordNet.", file=sys.stderr)
        _NLP_LOAD_FAILED = True
        return None


def spacy_pos(concept: str):
    """
    Return (conceptnet_pos, tag) for a single ConceptNet *term*, or
    (None, None) if we cannot decide. `tag` is the raw spaCy POS for
    diagnostics. Expects a bare term, not a URI.
    """
    nlp = _load_spacy()
    if nlp is None:
        return None, None

    text = concept.replace("_", " ").strip()
    if not text:
        return None, None

    try:
        doc = nlp(text)
    except Exception:
        return None, None

    # Prefer the head of the first noun chunk. This correctly resolves
    # multi-word concepts like "sweet_and_sour_sauce" -> sauce.
    for chunk in doc.noun_chunks:
        head = chunk.root
        cn = SPACY_UPOS_TO_CN_POS.get(head.pos_)
        if cn:
            return cn, head.pos_

    # Otherwise fall back to the syntactic root.
    root = next(
        (t for t in doc
         if t.head == t and not t.is_punct and not t.is_space),
        None,
    )
    if root is None:
        return None, None
    return SPACY_UPOS_TO_CN_POS.get(root.pos_), root.pos_


_POS_TAG_READY = False


def _ensure_nltk_tagger():
    """Make sure NLTK's POS tagger data is available. Idempotent."""
    global _POS_TAG_READY
    if _POS_TAG_READY:
        return True
    try:
        nltk.pos_tag(["dog"])
        _POS_TAG_READY = True
        return True
    except LookupError:
        try:
            nltk.download("averaged_perceptron_tagger", quiet=True)
            nltk.download("averaged_perceptron_tagger_eng", quiet=True)
            nltk.pos_tag(["dog"])
            _POS_TAG_READY = True
            return True
        except Exception:
            return False
    except Exception:
        return False


def nltk_pos(concept: str):
    """POS via NLTK's averaged-perceptron tagger; (None, None) on failure."""
    if not _NLTK_AVAILABLE or not _ensure_nltk_tagger():
        return None, None

    text = concept.replace("_", " ").strip()
    if not text:
        return None, None

    try:
        tagged = nltk.pos_tag([text])
    except Exception:
        return None, None

    if not tagged:
        return None, None
    _, tag = tagged[0]
    return PTB_TO_CN_POS.get(tag), tag


_VERB_SUFFIXES = ("ing", "ed", "ise", "ize", "ify", "ate")
_ADJ_SUFFIXES = ("ous", "ful", "less", "able", "ible", "ive",
                 "ish", "al", "ic", "ant", "ent", "ly")
_NOUN_SUFFIXES = ("tion", "sion", "ment", "ness", "ity", "er", "or",
                  "ist", "ism", "ance", "ence", "ship", "hood")


def heuristic_pos(concept: str):
    """Conservative suffix-based guess. Returns (conceptnet_pos, reason)."""
    term = concept.rsplit("/", 1)[0] if "/" in concept else concept
    term = term.replace("_", " ").strip().lower()
    if not term:
        return None, None

    last = term.split()[-1]

    if last.endswith("ly") and len(last) > 3:
        return "r", "heuristic:-ly"

    for suf in _ADJ_SUFFIXES:
        if last.endswith(suf) and len(last) > len(suf) + 1:
            return "a", f"heuristic:-{suf}"

    for suf in _VERB_SUFFIXES:
        if last.endswith(suf) and len(last) > len(suf) + 1:
            return "v", f"heuristic:-{suf}"

    for suf in _NOUN_SUFFIXES:
        if last.endswith(suf) and len(last) > len(suf) + 1:
            return "n", f"heuristic:-{suf}"

    return None, None


def wordnet_pos(concept: str):
    term = concept.replace("_", " ").lower()
    for wn_pos, cn_pos in (("n", "n"), ("v", "v"), ("a", "a"), ("r", "r")):
        try:
            if wn.synsets(term, pos=wn_pos):
                return cn_pos
        except Exception:
            continue
    return None


# ---------------------------------------------------------------------------
# Seeding source: local ConceptNet dump
# ---------------------------------------------------------------------------

def build_pos_index_from_dump(dump_path, target_terms,
                              progress_every=5_000_000):
    """
    Stream the ConceptNet assertions dump and build:
        term -> {pos: weight_sum}
    Only /c/en/ URIs are considered. Returns (index, diagnostics_dict).
    Keys are BARE TERMS ('photosynthesis'), not URIs.
    """
    index = defaultdict(lambda: defaultdict(float))
    opener = gzip.open if dump_path.endswith(".gz") else open

    lines = 0
    en_start = 0
    en_end = 0
    en_with_pos = 0
    matched = 0
    sample_matches = []
    sample_en_uris = []
    t0 = time.time()

    with opener(dump_path, "rt", encoding="utf-8", errors="replace") as f:
        for line in f:
            lines += 1
            if lines % progress_every == 0:
                elapsed = time.time() - t0
                print(f"    scanned {lines:,} lines "
                      f"({len(index):,} target terms indexed, "
                      f"{matched:,} matches, "
                      f"{lines/max(elapsed,1e-6):,.0f} lines/s)")

            parts = line.split("\t", 4)
            if len(parts) < 4:
                continue

            weight = 1.0
            if len(parts) >= 5:
                m = _WEIGHT_RE.search(parts[4])
                if m:
                    try:
                        weight = float(m.group(1))
                    except ValueError:
                        weight = 1.0

            for idx, uri in enumerate((parts[2], parts[3])):
                if not uri.startswith("/c/en/"):
                    continue
                if idx == 0:
                    en_start += 1
                else:
                    en_end += 1

                rest = uri[6:]  # everything after "/c/en/"
                if not rest:
                    continue
                if "/" in rest:
                    term, _, tail = rest.partition("/")
                    pos = tail.split("/", 1)[0] if tail else ""
                else:
                    term, pos = rest, ""

                if not term:
                    continue
                if len(sample_en_uris) < 10:
                    sample_en_uris.append(uri)
                if pos:
                    en_with_pos += 1

                if term not in target_terms:
                    continue
                matched += 1
                if pos in CONCEPTNET_POS_TO_NODE_TYPE:
                    index[term][pos] += weight
                    if len(sample_matches) < 20:
                        sample_matches.append(f"{term}/{pos}")

    diagnostics = {
        "lines": lines,
        "en_start": en_start,
        "en_end": en_end,
        "en_with_pos": en_with_pos,
        "matched": matched,
        "sample_en_uris": sample_en_uris,
        "sample_matches": sample_matches,
        "elapsed": time.time() - t0,
    }
    return index, diagnostics


def pick_pos_from_index(term, index):
    dist = index.get(term)
    if not dist:
        return None
    return max(dist.items(), key=lambda kv: kv[1])[0]


# ---------------------------------------------------------------------------
# Seeding source: concurrent ConceptNet API
# ---------------------------------------------------------------------------

def query_one_concept(concept, api_base, timeout=10):
    encoded = quote(concept, safe="")
    url = f"{api_base}/c/en/{encoded}"
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code == 200:
            return r.json()
    except requests.RequestException:
        pass
    return None


def seed_via_api(concepts, api_base, workers, sleep_per_worker):
    results = {}

    def task(concept):
        data = query_one_concept(concept, api_base)
        time.sleep(sleep_per_worker)
        return concept, data

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(task, c): c for c in concepts}
        for i, fut in enumerate(as_completed(futures), 1):
            concept, data = fut.result()
            results[concept] = data
            if i % 200 == 0 or i == len(concepts):
                print(f"    {i}/{len(concepts)} API responses received")
    return results


def extract_pos_from_api_payload(concept, data):
    if not data:
        return None, None
    target = normalize(concept)
    votes = {}

    def add(pos, weight=1.0):
        if pos in CONCEPTNET_POS_TO_NODE_TYPE:
            votes[pos] = votes.get(pos, 0.0) + weight

    uri = data.get("@id") or ""
    parts = uri.split("/")
    uri_pos = None
    if len(parts) > 4 and parts[1] == "c" and parts[2] == "en":
        uri_pos = parts[4]
        add(uri_pos, weight=2.0)

    for edge in data.get("edges", []):
        for key in ("start", "end"):
            ep = edge.get(key, {}).get("@id") or ""
            parts = ep.split("/")
            if len(parts) > 4 and parts[1] == "c" and parts[2] == "en":
                if normalize(parts[3]) == target:
                    add(parts[4], weight=float(edge.get("weight", 1.0)))

    if not votes:
        return None, uri_pos
    return max(votes.items(), key=lambda kv: kv[1])[0], uri_pos


# ---------------------------------------------------------------------------
# The cascade
# ---------------------------------------------------------------------------

def resolve_pos(concept,
                dump_index=None,
                api_payload=None,
                use_wordnet=True,
                use_spacy=True,
                use_nltk=True,
                use_heuristics=True):
    """
    Return (conceptnet_pos, source_label) for a single BARE TERM.

    Order of preference:
        1. dump index (if provided)          -> "conceptnet_dump"
        2. API payload (if provided)         -> "conceptnet_api" / "function_pos"
        3. spaCy    (if enabled/installed)   -> "spacy"
        4. NLTK tagger (if enabled/installed)-> "nltk"
        5. WordNet  (if enabled)             -> "wordnet"
        6. suffix heuristics (if enabled)    -> "heuristic"
        otherwise                             -> (None, "none")
    """
    if dump_index is not None:
        pos = pick_pos_from_index(normalize(concept), dump_index)
        if pos:
            return pos, "conceptnet_dump"

    if api_payload is not None:
        pos, uri_pos = extract_pos_from_api_payload(concept, api_payload)
        if uri_pos in FUNCTION_POS:
            return None, "function_pos"
        if pos:
            return pos, "conceptnet_api"

    if use_spacy:
        pos, _ = spacy_pos(concept)
        if pos:
            return pos, "spacy"

    if use_nltk:
        pos, _ = nltk_pos(concept)
        if pos:
            return pos, "nltk"

    if use_wordnet:
        pos = wordnet_pos(concept)
        if pos:
            return pos, "wordnet"

    if use_heuristics:
        pos, _ = heuristic_pos(concept)
        if pos:
            return pos, "heuristic"

    return None, "none"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize(term: str) -> str:
    return term.strip().lower().replace(" ", "_")


def parse_uri(uri: str):
    """
    '/c/en/term/pos/sense' -> (lang, term, pos), or None if not a concept URI.
    `pos` is '' when the URI carries no POS segment (the untagged case the
    cascade must resolve).
    """
    parts = uri.split("/")
    if len(parts) < 4 or parts[1] != "c" or not parts[3]:
        return None
    lang = parts[2]
    term = parts[3]
    pos = parts[4] if len(parts) > 4 else ""
    return lang, term, pos


def load_cache(path: str) -> dict:
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(path: str, cache: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False)


def validate_input_path(path: str) -> str:
    p = os.path.abspath(os.path.expanduser(path))
    if not os.path.isfile(p):
        raise FileNotFoundError(f"Input CSV not found: {p}")
    if not os.access(p, os.R_OK):
        raise PermissionError(f"Input CSV not readable: {p}")
    return p


def write_csv(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def format_weight(w: float) -> str:
    return f"{w:.6g}"


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def read_edges(path):
    """Read the raw slice: [(relation, subject_uri, object_uri, weight)]."""
    edges = []
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            rel = (row.get("relation") or "").strip()
            subj = (row.get("subject") or "").strip()
            obj = (row.get("object") or "").strip()
            if not (rel and subj and obj):
                continue
            try:
                w = float(row.get("weight", 1.0))
            except (TypeError, ValueError):
                w = 1.0
            edges.append((rel, subj, obj, w))
    return edges


def type_endpoints(edges):
    """
    First typing pass over all edge endpoints.

    Returns
        info     : uri -> {name, pos, node_type, label_source} for typed
                   endpoints, or a reason string for dropped ones, or the
                   placeholder "pending" for those the cascade must resolve.
        untagged : uri -> term, only for "pending" entries.
    """
    info, untagged = {}, {}
    for _rel, subj, obj, _w in edges:
        for uri in (subj, obj):
            if uri in info:
                continue
            parsed = parse_uri(uri)
            if parsed is None:
                info[uri] = "uri_malformed"
                continue
            lang, term, pos = parsed
            if lang != "en":
                info[uri] = "non_english"
            elif normalize(term) in STOPWORD_CONCEPTS:
                info[uri] = "stopword"
            elif pos in CONCEPTNET_POS_TO_NODE_TYPE:
                info[uri] = {
                    "name": term,
                    "pos": pos,
                    "node_type": CONCEPTNET_POS_TO_NODE_TYPE[pos],
                    "label_source": "uri",
                }
            elif pos in FUNCTION_POS:
                info[uri] = "function_pos"
            else:
                info[uri] = "pending"
                untagged[uri] = term
    return info, untagged


def resolve_untagged(untagged, args, stats):
    """
    Run the labeling cascade over the untagged endpoints.

    Returns uri -> node dict for the resolvable ones; the rest are recorded
    in stats.cascade_unresolved (uri -> source label) and will be dropped.
    """
    resolved = {}
    if not untagged:
        return resolved

    dump_index = None
    api_cache = None

    if args.source == "dump":
        dump_path = os.path.abspath(os.path.expanduser(args.dump))
        if not os.path.isfile(dump_path):
            print(f"  [WARN] ConceptNet dump not found ({dump_path}); "
                  f"cascading with spaCy/NLTK/WordNet/heuristics only.",
                  file=sys.stderr)
        else:
            size_mb = os.path.getsize(dump_path) / (1024 * 1024)
            print(f"  dump size : {size_mb:,.1f} MB")
            terms = {normalize(t) for t in untagged.values()}
            print(f"Streaming ConceptNet dump for {len(terms):,} terms ...")
            dump_index, diag = build_pos_index_from_dump(dump_path, terms)
            stats.dump = {
                "lines": diag["lines"],
                "en_start": diag["en_start"],
                "en_end": diag["en_end"],
                "matched": diag["matched"],
                "elapsed": diag["elapsed"],
            }
            print(f"    lines scanned    : {diag['lines']:,}")
            print(f"    /c/en/ endpoints : {diag['en_start'] + diag['en_end']:,}")
            print(f"    terms matched    : {diag['matched']:,}")
            print(f"    elapsed          : {diag['elapsed']:.1f}s")
            if diag["lines"] < 1_000_000:
                print("    [WARN] fewer than 1M lines scanned; "
                      "is the dump complete?")
            if diag["matched"] == 0 and terms:
                print("    [WARN] no terms matched the dump; check dump file "
                      "and term normalization.")

    elif args.source == "api":
        api_cache = load_cache(args.cache)
        missing = sorted({t for t in untagged.values() if t not in api_cache})
        print(f"API mode: {len(api_cache):,} cached, {len(missing):,} to fetch "
              f"({args.workers} workers, ~3 req/s limit).")
        if missing:
            payloads = seed_via_api(missing, args.api_base,
                                    args.workers, args.sleep)
            api_cache.update(payloads)
            save_cache(args.cache, api_cache)

    # --source wordnet honours its contract: no ConceptNet, no taggers.
    use_spacy = not args.no_spacy and args.source != "wordnet"
    use_nltk = not args.no_nltk and args.source != "wordnet"

    print(f"Tagging {len(untagged):,} untagged concepts "
          f"(spaCy={'on' if use_spacy else 'off'}, "
          f"NLTK={'on' if use_nltk else 'off'}, "
          f"WordNet={'on' if not args.no_wordnet else 'off'}, "
          f"heuristics={'on' if not args.no_heuristics else 'off'}) ...")

    t0 = time.time()
    for i, (uri, term) in enumerate(sorted(untagged.items()), 1):
        pos, source = resolve_pos(
            term,
            dump_index=dump_index,
            api_payload=(api_cache.get(term) if api_cache is not None else None),
            use_spacy=use_spacy,
            use_nltk=use_nltk,
            use_wordnet=not args.no_wordnet,
            use_heuristics=not args.no_heuristics,
        )
        stats.cascade_sources[source] += 1
        if pos in CONCEPTNET_POS_TO_NODE_TYPE:
            resolved[uri] = {
                "name": term,
                "pos": pos,
                "node_type": CONCEPTNET_POS_TO_NODE_TYPE[pos],
                "label_source": source,
            }
        else:
            stats.cascade_unresolved[uri] = source
        if i % 10_000 == 0:
            print(f"    {i:,}/{len(untagged):,}")
    stats.cascade_seconds = time.time() - t0
    print(f"  cascade finished in {stats.cascade_seconds:.1f}s")
    return resolved


def classify_edges(edges, info):
    """
    Classify every kept edge and flag contract violations.

    Returns (rows, dropped_endpoint, unknown_relations, violations,
             duplicates, edges_lost) where `rows` are dicts ready for
    typed_edges.csv. Violations are KEPT with permitted=0.
    """
    rows = []
    dropped_endpoint = 0
    unknown_relations = Counter()
    violations = Counter()
    edges_lost = Counter()
    seen_triples = set()
    duplicates = 0

    for rel, s, o, w in edges:
        si, oi = info.get(s), info.get(o)
        if not (isinstance(si, dict) and isinstance(oi, dict)):
            dropped_endpoint += 1
            for uri in (s, o):
                if not isinstance(info.get(uri), dict):
                    edges_lost[uri] += 1
            continue

        classes = RELATION_TO_EDGE_CLASSES.get(rel)
        if classes is None:
            unknown_relations[rel] += 1
            continue

        key = (s, rel, o)
        if key in seen_triples:
            duplicates += 1
        seen_triples.add(key)

        ec = TYPE_LETTER[si["node_type"]] + "2" + TYPE_LETTER[oi["node_type"]]
        permitted = 1 if ("ALL" in classes or ec in classes) else 0
        if not permitted:
            violations[f"{rel}:{ec}"] += 1

        rows.append({
            "relation": rel,
            "subject": s,
            "object": o,
            "weight": format_weight(w),
            "edge_class": ec,
            "permitted": permitted,
        })

    return rows, dropped_endpoint, unknown_relations, violations, \
        duplicates, edges_lost


# ---------------------------------------------------------------------------
# Pipeline accounting
# ---------------------------------------------------------------------------

class PipelineStats:
    def __init__(self):
        self.edges_raw = 0
        self.endpoints_unique = 0
        self.untagged_total = 0
        self.label_sources = Counter()
        self.node_types = Counter()
        self.drop_reasons = Counter()
        self.cascade_sources = Counter()
        self.cascade_unresolved = {}          # uri -> source label
        self.cascade_seconds = 0.0
        self.dump = {}
        self.nodes_kept = 0
        self.edges_kept = 0
        self.edges_dropped_endpoint = 0
        self.edges_dropped_unknown_relation = Counter()
        self.edges_duplicate_triples = 0
        self.contract_violations = Counter()

    def as_dict(self):
        return {
            "edges_raw": self.edges_raw,
            "endpoints_unique": self.endpoints_unique,
            "untagged_total": self.untagged_total,
            "label_sources": dict(self.label_sources),
            "node_types": dict(self.node_types),
            "drop_reasons": dict(self.drop_reasons),
            "cascade_sources": dict(self.cascade_sources),
            "cascade_unresolved_count": len(self.cascade_unresolved),
            "cascade_seconds": round(self.cascade_seconds, 2),
            "dump": self.dump,
            "nodes_kept": self.nodes_kept,
            "edges_kept": self.edges_kept,
            "edges_dropped_endpoint": self.edges_dropped_endpoint,
            "edges_dropped_unknown_relation":
                dict(self.edges_dropped_unknown_relation),
            "edges_duplicate_triples": self.edges_duplicate_triples,
            "contract_violations": dict(self.contract_violations),
        }


def report_stats(stats, out=sys.stdout):
    print("\nPipeline funnel", file=out)
    print("===============", file=out)
    print(f"  raw edges                     : {stats.edges_raw:>9,}",
          file=out)
    print(f"  unique endpoints              : {stats.endpoints_unique:>9,}",
          file=out)

    print("\nStage 1 - endpoint typing", file=out)
    for src, n in sorted(stats.label_sources.items(), key=lambda kv: -kv[1]):
        print(f"  kept   {src:<22}: {n:>9,}", file=out)
    for reason, n in sorted(stats.drop_reasons.items(), key=lambda kv: -kv[1]):
        print(f"  drop   {reason:<22}: {n:>9,}", file=out)
    print(f"  nodes kept                    : {stats.nodes_kept:>9,}",
          file=out)
    for t in ("EntityNode", "ActionEventNode", "PropertyNode"):
        print(f"    {t:<24}: {stats.node_types.get(t, 0):>9,}", file=out)

    print("\nStage 2 - cascade (untagged endpoints only)", file=out)
    print(f"  untagged                      : {stats.untagged_total:>9,}",
          file=out)
    for src, n in sorted(stats.cascade_sources.items(), key=lambda kv: -kv[1]):
        print(f"  {src:<30}: {n:>9,}", file=out)
    print(f"  time                          : "
          f"{stats.cascade_seconds:>9.1f}s", file=out)

    print("\nStage 3 - edges", file=out)
    print(f"  kept                          : {stats.edges_kept:>9,}",
          file=out)
    print(f"  dropped (endpoint dropped)    : "
          f"{stats.edges_dropped_endpoint:>9,}", file=out)
    for rel, n in sorted(stats.edges_dropped_unknown_relation.items(),
                         key=lambda kv: -kv[1]):
        print(f"  dropped (unknown relation) {rel:<11}: {n:>9,}", file=out)
    print(f"  duplicate triples (kept)      : "
          f"{stats.edges_duplicate_triples:>9,}", file=out)
    print(f"  contract violations (kept)    : "
          f"{sum(stats.contract_violations.values()):>9,}", file=out)
    for key, n in sorted(stats.contract_violations.items(),
                         key=lambda kv: -kv[1])[:10]:
        print(f"    {key:<30}: {n:>9,}", file=out)

    print(file=out)
    accounted_nodes = stats.nodes_kept + sum(stats.drop_reasons.values())
    accounted_edges = (stats.edges_kept + stats.edges_dropped_endpoint
                       + sum(stats.edges_dropped_unknown_relation.values()))
    if accounted_nodes != stats.endpoints_unique:
        print(f"  [WARN] endpoint accounting mismatch: "
              f"{accounted_nodes} != {stats.endpoints_unique}", file=out)
    if accounted_edges != stats.edges_raw:
        print(f"  [WARN] edge accounting mismatch: "
              f"{accounted_edges} != {stats.edges_raw}", file=out)
    if (accounted_nodes == stats.endpoints_unique
            and accounted_edges == stats.edges_raw):
        print(f"  accounting ok: {accounted_nodes:,} endpoints, "
              f"{accounted_edges:,} edges all accounted for.", file=out)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-i", "--input", dest="input",
                        default="data/preprocessed/conceptnet_science_2hop.csv")
    parser.add_argument("--out-dir", dest="out_dir",
                        default="data/preprocessed")
    parser.add_argument("--source", choices=("dump", "api", "wordnet"),
                        default="dump")
    parser.add_argument("--dump", dest="dump",
                        default="data/original/conceptnet-assertions-5.7.0.csv.gz")
    parser.add_argument("--api-base", default="https://api.conceptnet.io")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--sleep", type=float, default=0.35)
    parser.add_argument("--cache", dest="cache", default=None,
                        help="API cache file "
                             "(default: <out-dir>/conceptnet_api_cache.json)")
    parser.add_argument("--no-spacy", action="store_true",
                        help="Disable the spaCy fallback.")
    parser.add_argument("--no-nltk", action="store_true",
                        help="Disable the NLTK fallback.")
    parser.add_argument("--no-wordnet", action="store_true",
                        help="Disable the WordNet fallback.")
    parser.add_argument("--no-heuristics", action="store_true",
                        help="Disable the suffix heuristics fallback.")
    args = parser.parse_args()
    if args.cache is None:
        args.cache = os.path.join(
            os.path.abspath(os.path.expanduser(args.out_dir)),
            "conceptnet_api_cache.json")
    return args


def main():
    args = parse_args()
    check_static_schema()

    try:
        input_path = validate_input_path(args.input)
    except (FileNotFoundError, PermissionError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(2)

    out_dir = os.path.abspath(os.path.expanduser(args.out_dir))
    os.makedirs(out_dir, exist_ok=True)
    nodes_path = os.path.join(out_dir, "typed_nodes.csv")
    edges_path = os.path.join(out_dir, "typed_edges.csv")
    dropped_path = os.path.join(out_dir, "dropped_concepts.csv")
    stats_path = os.path.join(out_dir, "pipeline_stats.json")

    print("Configuration")
    print("-------------")
    print(f"  input      : {input_path}")
    print(f"  out-dir    : {out_dir}")
    print(f"  source     : {args.source}")
    if args.source == "dump":
        print(f"  dump       : "
              f"{os.path.abspath(os.path.expanduser(args.dump))}")
    print()

    stats = PipelineStats()

    # --- 1. read the raw slice ---------------------------------------------
    print(f"Reading {input_path} ...")
    edges = read_edges(input_path)
    stats.edges_raw = len(edges)
    print(f"  {len(edges):,} edges")

    # --- 2. type endpoints: URI POS first, cascade for the rest ------------
    info, untagged = type_endpoints(edges)
    stats.endpoints_unique = len(info)
    stats.untagged_total = len(untagged)
    print(f"  {len(info):,} unique endpoints "
          f"({len(untagged):,} without a POS in the URI)")

    resolved = resolve_untagged(untagged, args, stats)
    for uri, node in resolved.items():
        info[uri] = node
    for uri, source in stats.cascade_unresolved.items():
        info[uri] = f"unresolved_{source}"

    for val in info.values():
        if isinstance(val, dict):
            stats.label_sources[val["label_source"]] += 1
            stats.node_types[val["node_type"]] += 1
        else:
            stats.drop_reasons[val] += 1
    stats.nodes_kept = sum(stats.label_sources.values())

    # --- 3. classify edges ---------------------------------------------------
    (rows, dropped_endpoint, unknown_rel, violations,
     duplicates, edges_lost) = classify_edges(edges, info)
    stats.edges_kept = len(rows)
    stats.edges_dropped_endpoint = dropped_endpoint
    stats.edges_dropped_unknown_relation = unknown_rel
    stats.edges_duplicate_triples = duplicates
    stats.contract_violations = violations

    # --- 4. write outputs ----------------------------------------------------
    nodes = sorted(
        ({"uri": uri, **val} for uri, val in info.items()
         if isinstance(val, dict)),
        key=lambda r: r["uri"],
    )
    write_csv(nodes_path,
              ["uri", "name", "pos", "node_type", "label_source"], nodes)
    print(f"Wrote {len(nodes):,} typed nodes -> {nodes_path}")

    write_csv(edges_path,
              ["relation", "subject", "object", "weight",
               "edge_class", "permitted"], rows)
    print(f"Wrote {len(rows):,} edges -> {edges_path}")
    print(f"  dropped: {dropped_endpoint:,} (endpoint), "
          f"{sum(unknown_rel.values()):,} (unknown relation)")
    print(f"  contract violations kept + flagged: "
          f"{sum(violations.values()):,}")

    dropped_rows = []
    for uri, val in info.items():
        if isinstance(val, dict):
            continue
        parsed = parse_uri(uri)
        term = parsed[1] if parsed else uri
        dropped_rows.append({"uri": uri, "term": term, "reason": val,
                             "edges_lost": edges_lost.get(uri, 0)})
    write_csv(dropped_path,
              ["uri", "term", "reason", "edges_lost"], dropped_rows)
    if dropped_rows:
        print(f"Wrote {len(dropped_rows):,} dropped concepts -> {dropped_path}")

    report_stats(stats)
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats.as_dict(), f, indent=2, ensure_ascii=False)
    print(f"\nWrote pipeline statistics -> {stats_path}")

    print("\nNext steps")
    print("----------")
    print("  MySQL loader    : ingest typed_nodes.csv, then typed_edges.csv")
    print("                    (dedup rule: (subject, relation, object),")
    print("                     duplicates keep the max weight)")
    print("  Memgraph loader : python build_memgraph.py  (same two files)")


if __name__ == "__main__":
    main()