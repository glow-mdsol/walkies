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
    match = DOI_RE.search(text[:4000])
    return _clean_doi(match.group(0)) if match else None


def _find_journal(meta: dict) -> str | None:
    subject = str(meta.get("/Subject") or "")
    if not subject:
        return None
    # Elsevier sets Subject to e.g. "THE LANCET Diabetes & Endocrinology, 5 (2017) 377-390. doi:..."
    head = subject.split(",")[0].strip()
    return head or None


def _extract_abstract(text: str) -> str:
    """Best-effort pull of the abstract / opening summary paragraph."""
    lowered = text.lower()
    start = lowered.find("abstract")
    if start != -1:
        chunk = text[start + len("abstract"):]
    else:
        # No explicit "Abstract" heading (common in Lancet reviews) — take the
        # first substantial run of prose after the front matter.
        chunk = text[:3000]
    chunk = _normalise_ws(chunk)
    # Stop at common section boundaries that follow an abstract.
    for marker in ("Introduction", "Background", "Keywords", "1. ", "Methods"):
        idx = chunk.find(marker)
        if 200 < idx < 2500:
            chunk = chunk[:idx]
            break
    return chunk[:1500].strip()


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
        doc.text = "\n".join(parts)
        doc.title = _fix_mojibake((meta.get("/Title") or "").strip()) or None
        doc.doi = _find_doi(meta, doc.text)
        doc.journal = _fix_mojibake(_find_journal(meta))
        doc.abstract = _extract_abstract(doc.text)
        if not doc.text:
            doc.error = "no extractable text (scanned/image PDF?)"
    except Exception as exc:  # noqa: BLE001 - surface as data, keep batch going
        doc.error = f"{type(exc).__name__}: {exc}"
    return doc


def extract_sources(sources_dir: Path = SOURCES_DIR) -> list[ExtractedDocument]:
    """Extract every PDF in ``sources_dir``, sorted by filename."""
    if not sources_dir.exists():
        raise FileNotFoundError(f"sources directory not found: {sources_dir}")
    pdfs = sorted(sources_dir.glob("*.pdf"))
    return [extract_document(p) for p in pdfs]
