"""Tests for the source-review extraction/digest helpers.

These cover the pure-Python logic (DOI parsing, key-point heuristics, digest
rendering) without requiring pypdf or any PDFs, so they run in the normal test
suite. The pypdf-backed extraction path is exercised by `task sources:digest`.
"""

import sys
from pathlib import Path

# Resolve import paths by adding the backend directory to sys.path (matches the
# convention in test_settings.py).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sources_review.extract import (
    ExtractedDocument,
    _clean_doi,
    _find_doi,
    _extract_abstract,
    _fix_mojibake,
)
from sources_review.digest import _key_points, build_digest


def _doc(**kw) -> ExtractedDocument:
    base = dict(path=Path("sources/example.pdf"), pages=10, text="", title="Example")
    base.update(kw)
    return ExtractedDocument(**base)


def test_clean_doi_strips_trailing_punctuation():
    assert _clean_doi("10.1016/j.jcjd.2020.06.003.") == "10.1016/j.jcjd.2020.06.003"
    assert _clean_doi("10.1056/NEJMoa2203913)") == "10.1056/NEJMoa2203913"


def test_find_doi_prefers_metadata():
    meta = {"/doi": "10.2337/dc20-2250"}
    assert _find_doi(meta, "irrelevant body text") == "10.2337/dc20-2250"


def test_find_doi_falls_back_to_subject_and_body():
    meta = {"/Subject": "Canadian Journal of Diabetes, 44 (2020) 740. doi:10.1016/j.jcjd.2020.06.003"}
    assert _find_doi(meta, "") == "10.1016/j.jcjd.2020.06.003"
    assert _find_doi({}, "see https://doi.org/10.1111/jdi.13356 for details") == "10.1111/jdi.13356"


def test_find_doi_returns_none_when_absent():
    assert _find_doi({}, "no identifier here") is None


def test_fix_mojibake_repairs_double_encoded_dash():
    # "–" (en dash) UTF-8 bytes decoded as cp1252 become "â€“".
    assert _fix_mojibake("type 1 diabetes â€“ Authors' reply") == "type 1 diabetes – Authors' reply"


def test_fix_mojibake_leaves_clean_text_untouched():
    assert _fix_mojibake("Exercise management in type 1 diabetes") == \
        "Exercise management in type 1 diabetes"
    assert _fix_mojibake(None) is None


def test_extract_abstract_trims_at_section_boundary():
    text = "Abstract " + "Glucose fell during exercise. " * 10 + "Introduction Now the body."
    abstract = _extract_abstract(text)
    assert abstract.startswith("Glucose fell")
    assert "Introduction" not in abstract


def test_key_points_prefers_domain_and_numeric_sentences():
    doc = _doc(text=(
        "The weather was pleasant on the day of testing. "
        "Time in range improved from 61% to 71% with closed-loop insulin delivery. "
        "Participants enjoyed the study. "
        "Hypoglycaemia was significantly reduced during aerobic exercise (p<0.001)."
    ))
    points = _key_points(doc, limit=2)
    assert len(points) == 2
    joined = " ".join(points).lower()
    assert "time in range" in joined
    assert "hypoglyc" in joined
    assert "weather was pleasant" not in joined


def test_build_digest_contains_sections_and_bibliography():
    docs = [
        _doc(title="Closed-loop in exercise", doi="10.2337/dc17-0883",
             journal="Diabetes Care",
             text="Time in range improved with closed-loop control during exercise (p=0.005)."),
        _doc(path=Path("sources/broken.pdf"), title=None, text="", error="no extractable text"),
    ]
    md = build_digest(docs)
    assert "# Source Digest" in md
    assert "Closed-loop in exercise" in md
    assert "doi:[10.2337/dc17-0883]" in md
    assert "## Could not extract" in md
    assert "broken.pdf" in md
    assert "Feature-review prompts" in md


def test_build_digest_includes_llm_summary_when_present():
    docs = [_doc(title="Paper A", text="Glucose dropped during walking exercise.")]
    md = build_digest(docs, summaries={"example.pdf": "An LLM-written summary."}, llm_used=True)
    assert "An LLM-written summary." in md
    assert "summaries: Claude" in md
