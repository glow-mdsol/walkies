"""Compact extracted documents into a single reviewable key-resource (Markdown)."""

from __future__ import annotations

import datetime as _dt
import re
from pathlib import Path

from .extract import ExtractedDocument

DEFAULT_DIGEST_PATH = Path(__file__).resolve().parent.parent.parent / "sources" / "DIGEST.md"

# Terms that, when found near a sentence, make it a candidate "key point" for a
# walk/glucose analytics project. Tuned to the clinical domain of this repo.
_KEY_TERMS = (
    "time in range", "tir", "hypoglyc", "hyperglyc", "glucose", "insulin",
    "basal", "bolus", "closed-loop", "closed loop", "hybrid", "smartguard",
    "predictive low", "aerobic", "anaerobic", "intensity", "heart rate",
    "carbohydrate", "exercise", "recovery", "overnight", "nocturnal",
    "mmol", "mg/dl", "set point", "target", "suspend",
)

_SENT_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z0-9])")


def _sentences(text: str) -> list[str]:
    return [s.strip() for s in _SENT_SPLIT.split(text) if len(s.strip()) > 30]


def _key_points(doc: ExtractedDocument, limit: int = 6) -> list[str]:
    """Heuristic key-point extraction: sentences dense in domain terms + numbers."""
    scored: list[tuple[float, str]] = []
    seen: set[str] = set()
    for sentence in _sentences(doc.text):
        low = sentence.lower()
        term_hits = sum(1 for t in _KEY_TERMS if t in low)
        if term_hits == 0:
            continue
        has_number = bool(re.search(r"\d", sentence))
        score = term_hits + (1.5 if has_number else 0)
        # Prefer findings-shaped sentences.
        if any(w in low for w in ("improved", "reduced", "increased", "decreased",
                                  "associated", "significant", "vs", "compared")):
            score += 1.5
        key = low[:60]
        if key in seen:
            continue
        seen.add(key)
        scored.append((score, _collapse(sentence)))
    scored.sort(key=lambda x: x[0], reverse=True)
    return [s for _, s in scored[:limit]]


def _collapse(sentence: str) -> str:
    sentence = " ".join(sentence.split())
    return (sentence[:300].rstrip() + "…") if len(sentence) > 300 else sentence


def _doc_heading(doc: ExtractedDocument, index: int) -> str:
    title = doc.title or doc.filename
    return f"### {index}. {title}"


def _doc_section(doc: ExtractedDocument, index: int, summary: str | None = None) -> list[str]:
    lines = [_doc_heading(doc, index), ""]
    bib = []
    if doc.journal:
        bib.append(doc.journal)
    if doc.doi:
        bib.append(f"doi:[{doc.doi}](https://doi.org/{doc.doi})")
    bib.append(f"`{doc.filename}`")
    lines.append("- **Source:** " + " · ".join(bib))
    lines.append(f"- **Extracted:** {doc.pages} pages, {doc.word_count:,} words")
    lines.append("")

    if not doc.ok:
        lines.append(f"> ⚠️ Extraction issue: {doc.error}")
        lines.append("")
        return lines

    if summary:
        lines.append("**Summary**")
        lines.append("")
        lines.append(summary.strip())
        lines.append("")

    if doc.abstract:
        lines.append("**Abstract (extracted)**")
        lines.append("")
        lines.append("> " + doc.abstract.replace("\n", " "))
        lines.append("")

    points = _key_points(doc)
    if points:
        lines.append("**Key points (heuristic)**")
        lines.append("")
        lines.extend(f"- {p}" for p in points)
        lines.append("")
    return lines


def build_digest(
    docs: list[ExtractedDocument],
    summaries: dict[str, str] | None = None,
    llm_used: bool = False,
) -> str:
    """Render the compacted Markdown key-resource from extracted docs.

    ``summaries`` maps a document filename to an LLM-written summary (optional).
    """
    summaries = summaries or {}
    today = _dt.date.today().isoformat()
    ok_docs = [d for d in docs if d.ok]
    failed = [d for d in docs if not d.ok]

    out: list[str] = []
    out.append("# Source Digest — key resource for feature review")
    out.append("")
    out.append(
        "Auto-generated compaction of the reference PDFs in `sources/`. Use it to "
        "evaluate existing features and brainstorm new ones against the published "
        "evidence. Regenerate with `task sources:digest`."
    )
    out.append("")
    out.append(
        f"_Generated {today} · {len(ok_docs)} document(s) digested"
        + (f", {len(failed)} failed" if failed else "")
        + (" · summaries: Claude (--llm)" if llm_used else " · summaries: heuristic only")
        + "._"
    )
    out.append("")
    out.append(
        "> ⚠️ Heuristic key points are machine-extracted sentences, not verified "
        "claims — confirm against the source PDF before relying on a figure."
    )
    out.append("")

    # Contents
    out.append("## Contents")
    out.append("")
    for i, doc in enumerate(ok_docs, start=1):
        out.append(f"{i}. {doc.title or doc.filename}")
    out.append("")
    out.append("---")
    out.append("")

    out.append("## Documents")
    out.append("")
    for i, doc in enumerate(ok_docs, start=1):
        out.extend(_doc_section(doc, i, summaries.get(doc.filename)))
        out.append("---")
        out.append("")

    if failed:
        out.append("## Could not extract")
        out.append("")
        for doc in failed:
            out.append(f"- `{doc.filename}` — {doc.error}")
        out.append("")

    out.append("## Feature-review prompts")
    out.append("")
    out.append(
        "Questions to ask while reading the digest above against the current app "
        "(see `references.md` for the curated citation list):"
    )
    out.append("")
    out.extend(
        f"- {q}"
        for q in (
            "Which analyses does the app already compute that these papers validate "
            "(e.g. time-in-range, glucose drop rate, HR-zone vs glycaemia)?",
            "Do any papers describe a metric or window the app does **not** yet "
            "surface (e.g. post-exercise / overnight recovery window, intermittent "
            "high-intensity protection)?",
            "Are the app's clinical constants (target 4–8 mmol/L, hypo ≤3.9, "
            "set points) consistent with the thresholds in these sources?",
            "What hypo-risk prediction signal could be added based on the reported "
            "glucose-drop dynamics during sustained activity?",
        )
    )
    out.append("")
    return "\n".join(out)


def write_digest(content: str, path: Path = DEFAULT_DIGEST_PATH) -> Path:
    path.write_text(content, encoding="utf-8")
    return path
