"""Extract text and bibliographic metadata from source PDFs."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

# pypdf is provided by the `sources` dependency group. Import lazily inside the
# functions so that importing this package (e.g. for tests of the pure helpers)
# does not hard-require it.

SOURCES_DIR = Path(__file__).resolve().parent.parent.parent / "sources"

DOI_RE = re.compile(r"10\.\d{4,9}/[-._;()/:A-Za-z0-9]+")
# Trailing punctuation that frequently sticks to a DOI scraped from running text.
_DOI_TRAILING = ".,;:)]}>"

# Substitutions for a known pypdf encoding bug where PDFDocEncoding / Latin-9
# symbol characters arrive as /Cxx sequences (e.g. /C6 → ±, /C21 → ≥).
# These appear consistently in the Zaharieva et al. 2020 Canadian Journal of
# Diabetes PDF.
_ENCODING_SUBS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"/C6\b"),  "±"),
    (re.compile(r"/C2\b"),  "×"),
    (re.compile(r"/C21\b"), "≥"),
    (re.compile(r"/C15\b"), "•"),
    (re.compile(r"¼"),      "="),   # ¼ used as = in that PDF (e.g. N¼12)
]

# Terms used to select the most relevant abstract when a PDF contains multiple
# articles (e.g. a journal-issue PDF where the first article is unrelated).
_ABSTRACT_KEY_TERMS = (
    "type 1 diabetes", "t1d", "t1dm", "exercise", "glucose", "insulin",
    "hypoglycemia", "hypoglycaemia", "glycemic", "glycaemic", "aerobic",
    "closed-loop", "closed loop", "physical activity",
)


@dataclass
class ExtractedDocument:
    """The text and metadata pulled from a single source PDF."""

    path: Path
    title: str | None = None
    doi: str | None = None
    journal: str | None = None
    pages: int = 0
    text: str = ""
    abstract: str = ""
    error: str | None = None
    meta: dict = field(default_factory=dict)

    @property
    def filename(self) -> str:
        return self.path.name

    @property
    def ok(self) -> bool:
        return self.error is None and bool(self.text)

    @property
    def word_count(self) -> int:
        return len(self.text.split())


def _clean_doi(raw: str) -> str:
    return raw.rstrip(_DOI_TRAILING)


def _normalise_ws(text: str) -> str:
    return " ".join(text.split())


def _clean_text(text: str) -> str:
    """Post-process raw pypdf text to fix common extraction artefacts."""
    # Reassemble words broken across lines with a soft hyphen, e.g.
    # "aerobic\nexer-\ncise" → "aerobicexercise".  Only join when both sides
    # are word characters so we don't clobber legitimate hyphens.
    text = re.sub(r"(\w)-\s*\n\s*(\w)", r"\1\2", text)
    # Apply known encoding substitutions (Zaharieva PDF and similar).
    for pattern, replacement in _ENCODING_SUBS:
        text = pattern.sub(replacement, text)
    return text


def _fix_mojibake(text: str | None) -> str | None:
    """Repair UTF-8-decoded-as-Latin-1 mojibake in PDF metadata (e.g. 'â€“').

    Some exporters double-encode the Title/Subject fields. If the string contains
    the tell-tale 'Ã'/'â€' sequences, round-trip it back through latin-1/utf-8.
    """
    if not text:
        return text
    if "Ã" in text or "â€" in text:
        # cp1252 (not latin-1): the mojibake originates from UTF-8 bytes decoded
        # as Windows-1252, whose 0x80-0x9F range includes chars like € (U+20AC)
        # that latin-1 cannot round-trip.
        try:
            return text.encode("cp1252").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return text
    return text


def _find_doi(meta: dict, text: str) -> str | None:
    for key in ("/doi", "/DOI", "doi"):
        value = meta.get(key)
        if value:
            match = DOI_RE.search(str(value))
            if match:
                return _clean_doi(match.group(0))
    # Subject often carries a "doi:10.xxxx/yyyy" tail on Elsevier exports.
    for key in ("/Subject", "/Keywords"):
        value = meta.get(key)
        if value:
            match = DOI_RE.search(str(value))
            if match:
                return _clean_doi(match.group(0))
    # Search the first 5 000 chars — the paper's own DOI appears near the top.
    # Searching the full text risks picking up a cited reference's DOI instead.
    match = DOI_RE.search(text[:5000])
    return _clean_doi(match.group(0)) if match else None


def _find_journal(meta: dict) -> str | None:
    subject = str(meta.get("/Subject") or "")
    if not subject:
        return None
    # Elsevier sets Subject to e.g. "THE LANCET Diabetes & Endocrinology, 5 (2017) 377-390. doi:..."
    head = subject.split(",")[0].strip()
    return head or None


def _extract_abstract(text: str, title: str | None = None) -> str:
    """Best-effort pull of the abstract / opening summary paragraph.

    When a PDF contains multiple articles (e.g. a journal-issue file), the first
    "Abstract" heading may belong to an unrelated article.  Strategy (in order):

    1. If a title is supplied and is found in the body text, use the text
       immediately following it (best for multi-article issue PDFs).
    2. Try every "Abstract" occurrence and prefer the one that contains the most
       T1D/exercise domain terms.
    3. Fall back to the first "Abstract" occurrence, or the first prose block.
    """
    def _trim(raw: str) -> str:
        chunk = _normalise_ws(raw)
        for marker in ("Introduction", "Background", "Keywords", "1. ", "Methods"):
            idx = chunk.find(marker)
            if 200 < idx < 2500:
                chunk = chunk[:idx]
                break
        return chunk[:1500].strip()

    def _term_score(s: str) -> int:
        low = s.lower()
        return sum(1 for t in _ABSTRACT_KEY_TERMS if t in low)

    # Strategy 1: locate the document's own title in the body and start there.
    # Use rfind (last occurrence) because journal running headers can print the
    # article title on earlier pages that belong to a different article.
    if title:
        key_words = title.split()[:10]
        key = " ".join(key_words[:6])  # 6-word anchor is usually unique enough
        if len(key) > 15:
            lowered = text.lower()
            pos = lowered.rfind(key.lower())
            if pos != -1:
                candidate = _trim(text[pos:])
                if _term_score(candidate) > 0:
                    return candidate

    # Strategy 2: rank all "Abstract" occurrences by domain-term score.
    lowered = text.lower()
    positions = [m.start() for m in re.finditer(r"\babstract\b", lowered)]
    if not positions:
        # No "Abstract" heading. Scan for the densest key-term region rather
        # than blindly taking the first 3 000 chars (which may belong to an
        # unrelated article in a multi-paper issue PDF).
        window = 1500
        best_start = 0
        best_score_window = -1
        for start in range(0, min(len(text), 15000) - window, 300):
            s = _term_score(text[start:start + window].lower())
            if s > best_score_window:
                best_score_window = s
                best_start = start
        return _trim(text[best_start:best_start + 3000])

    candidates = [_trim(text[p + len("abstract"):]) for p in positions]
    scored = sorted(enumerate(candidates), key=lambda x: _term_score(x[1]), reverse=True)
    best_candidate_score = _term_score(scored[0][1])

    # If all abstract candidates appear late in the document (>60% through) the
    # "Abstract" keyword likely belongs to a different article in a multi-paper
    # issue PDF.  Fall back to the window scan if it scores higher.
    all_late = all(p > 0.6 * len(text) for p in positions)
    if all_late:
        window = 1500
        best_window_start = 0
        best_window_score = -1
        for start in range(0, min(len(text), 15000) - window, 300):
            s = _term_score(text[start:start + window].lower())
            if s > best_window_score:
                best_window_score = s
                best_window_start = start
        if best_window_score > best_candidate_score:
            return _trim(text[best_window_start:best_window_start + 3000])

    # Only use a non-first candidate if it clearly beats the first one.
    if best_candidate_score > 0 and scored[0][0] != 0 and _term_score(candidates[0]) < best_candidate_score - 1:
        return scored[0][1]

    return candidates[0]


def extract_document(path: Path) -> ExtractedDocument:
    """Extract text + metadata from one PDF. Never raises; errors are captured."""
    try:
        from pypdf import PdfReader
    except ModuleNotFoundError as exc:  # pragma: no cover - dependency guard
        raise ModuleNotFoundError(
            "pypdf is required for source extraction. Install the dependency "
            "group with `uv sync --group sources`."
        ) from exc

    doc = ExtractedDocument(path=path)
    try:
        reader = PdfReader(str(path))
        meta = {k: str(v) for k, v in (reader.metadata or {}).items()}
        doc.meta = meta
        doc.pages = len(reader.pages)
        parts: list[str] = []
        for page in reader.pages:
            extracted = page.extract_text() or ""
            if extracted:
                parts.append(extracted)
        doc.text = _clean_text("\n".join(parts))
        doc.title = _fix_mojibake((meta.get("/Title") or "").strip()) or None
        doc.doi = _find_doi(meta, doc.text)
        doc.journal = _fix_mojibake(_find_journal(meta))
        doc.abstract = _extract_abstract(doc.text, title=doc.title)
        if not doc.text:
            doc.error = "no extractable text (scanned/image PDF?)"
    except Exception as exc:  # noqa: BLE001 - surface as data, keep batch going
        doc.error = f"{type(exc).__name__}: {exc}"
    return doc


def extract_sources(sources_dir: Path = SOURCES_DIR) -> list[ExtractedDocument]:
    """Extract every PDF in ``sources_dir``, sorted by filename.

    Deduplicates by DOI so that a paper stored under two different filenames
    (e.g. the publisher's filename and a manually named copy) only appears once.
    When duplicates are found the copy with more extracted text is kept.
    """
    if not sources_dir.exists():
        raise FileNotFoundError(f"sources directory not found: {sources_dir}")
    pdfs = sorted(sources_dir.glob("*.pdf"))
    docs = [extract_document(p) for p in pdfs]

    seen_dois: dict[str, ExtractedDocument] = {}
    unique: list[ExtractedDocument] = []
    for doc in docs:
        if doc.doi and doc.doi in seen_dois:
            existing = seen_dois[doc.doi]
            if doc.word_count > existing.word_count:
                # Swap in the richer copy.
                unique[unique.index(existing)] = doc
                seen_dois[doc.doi] = doc
            # Otherwise drop this duplicate silently.
        else:
            if doc.doi:
                seen_dois[doc.doi] = doc
            unique.append(doc)
    return unique
