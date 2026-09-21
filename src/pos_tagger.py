"""
Local POS tagging of ConceptNet concepts using spaCy, with graceful
fallback to NLTK and WordNet.

The output vocabulary matches ConceptNet's own POS codes so the rest of
the pipeline (CONCEPTNET_POS_TO_NODE_TYPE) is unchanged:

    'n'  noun          -> EntityNode
    'v'  verb          -> ActionEventNode
    'a'  adjective     -> PropertyNode
    'r'  adverb        -> PropertyNode
    's'  satellite_adj -> PropertyNode  (rare, mapped to PropertyNode)
"""

import re

# --- spaCy ----------------------------------------------------------------

_SPACY_AVAILABLE = False
_nlp = None

try:
    import spacy
    _SPACY_AVAILABLE = True
except ImportError:
    spacy = None


def _get_nlp():
    global _nlp
    if _nlp is not None:
        return _nlp
    if not _SPACY_AVAILABLE:
        return None
    try:
        _nlp = spacy.load("en_core_web_sm",
                          disable=["ner", "textcat", "lemmatizer"])
    except OSError:
        raise RuntimeError(
            "spaCy model 'en_core_web_sm' is not installed. Run:\n"
            "    python -m spacy download en_core_web_sm"
        )
    return _nlp


# Penn Treebank tag -> ConceptNet POS
PTB_TO_CONCEPTNET_POS = {
    "NN": "n", "NNS": "n", "NNP": "n", "NNPS": "n",
    "VB": "v", "VBD": "v", "VBG": "v", "VBN": "v", "VBP": "v", "VBZ": "v",
    "JJ": "a", "JJR": "a", "JJS": "a",
    "RB": "r", "RBR": "r", "RBS": "r",
    # Everything else (DT, IN, PRP, CC, TO, ...) is intentionally
    # unmapped -> caller treats it as "no signal".
}


def spacy_pos(concept: str):
    """
    Return (conceptnet_pos, tag) for a single ConceptNet concept,
    or (None, None) if we cannot decide.

    Strategy:
      1. If the phrase contains a noun chunk, take the head of the first
         one. This correctly handles 'sweet_and_sour_sauce' -> sauce.
      2. Otherwise take the syntactic root of the sentence.
      3. Bail out on punctuation / whitespace / empty input.
    """
    nlp = _get_nlp()
    if nlp is None:
        return None, None

    text = concept.replace("_", " ").strip()
    if not text:
        return None, None

    doc = nlp(text)

    # 1. Noun chunk head
    for chunk in doc.noun_chunks:
        tag = chunk.root.tag_
        if tag in PTB_TO_CONCEPTNET_POS:
            return PTB_TO_CONCEPTNET_POS[tag], tag

    # 2. Dependency root (skipping punctuation and whitespace)
    root = next(
        (t for t in doc
         if t.head == t and not t.is_punct and not t.is_space),
        None,
    )
    if root is None:
        return None, None
    return PTB_TO_CONCEPTNET_POS.get(root.tag_), root.tag_


# --- NLTK (optional secondary fallback) -----------------------------------

_NLTK_AVAILABLE = False
try:
    import nltk
    _NLTK_AVAILABLE = True
except ImportError:
    nltk = None


def nltk_pos(concept: str):
    """
    POS via NLTK's averaged-perceptron tagger. Returns a single
    ConceptNet POS code, or None. Used only if spaCy fails.
    """
    if not _NLTK_AVAILABLE:
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
    return PTB_TO_CONCEPTNET_POS.get(tag), tag


# --- Heuristics (last resort) ---------------------------------------------

_VERB_SUFFIXES = ("ing", "ed", "ise", "ize", "ify", "ate")
_ADJ_SUFFIXES = ("ly", "ous", "ful", "less", "able", "ible", "ive",
                 "ish", "al", "ic", "ant", "ent")
_NOUN_SUFFIXES = ("tion", "sion", "ment", "ness", "ity", "er", "or",
                  "ist", "ism", "ance", "ence", "ship", "hood")


def heuristic_pos(concept: str):
    """
    Very conservative suffix-based guess. Returns (pos, reason) or
    (None, None). Order matters: adjective suffixes are checked before
    noun suffixes because e.g. 'ful' should win over a generic noun
    guess for 'beautiful'.
    """
    term = concept.rsplit("/", 1)[0] if "/" in concept else concept
    term = term.replace("_", " ").strip().lower()
    if not term:
        return None, None

    last = term.split()[-1]

    # adverbs end in -ly and are usually not adjectives
    if last.endswith("ly") and len(last) > 3:
        return "r", "heuristic:-ly"

    for suf in _ADJ_SUFFIXES:
        if last.endswith(suf):
            return "a", f"heuristic:-{suf}"

    for suf in _VERB_SUFFIXES:
        if last.endswith(suf) and len(last) > len(suf) + 1:
            return "v", f"heuristic:-{suf}"

    for suf in _NOUN_SUFFIXES:
        if last.endswith(suf):
            return "n", f"heuristic:-{suf}"

    return None, None