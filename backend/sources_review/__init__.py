"""Extract, summarise and compact the reference PDFs in ../sources.

This package turns the downloaded source documents (journal PDFs) into a single
reviewable digest that can be used to evaluate existing features and suggest new
ones. It is tooling, not part of the web app runtime — install its deps with the
``sources`` dependency group (``uv sync --group sources``) and run it via
``task sources:digest`` or ``python -m sources_review``.

Pipeline:
    extract  -> pull text + metadata from each PDF (pypdf)
    digest   -> build a structured, compacted Markdown key-resource (heuristic)
    enrich   -> optionally rewrite per-paper summaries with Claude (--llm)
"""

from .extract import ExtractedDocument, extract_document, extract_sources
from .digest import build_digest, write_digest

__all__ = [
    "ExtractedDocument",
    "extract_document",
    "extract_sources",
    "build_digest",
    "write_digest",
]
