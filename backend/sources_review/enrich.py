"""Optional LLM enrichment: rewrite per-document summaries with Claude.

This path is only used when ``--llm`` is passed and ANTHROPIC_API_KEY is set. The
``anthropic`` SDK is imported lazily so the heuristic digest works without it.
"""

from __future__ import annotations

import os

from .extract import ExtractedDocument

# Latest Claude models per project guidance; Haiku is plenty for summarisation
# and keeps per-run cost low. Override with WALKIES_SOURCES_MODEL if desired.
DEFAULT_MODEL = "claude-haiku-4-5-20251001"

_SYSTEM = (
    "You summarise clinical research papers for a software project that "
    "correlates walking/hiking activity against glucose and insulin data for a "
    "Type 1 diabetic on a MiniMed 780G hybrid closed-loop pump. For each paper, "
    "write a tight summary (4-6 sentences) covering: (1) what was studied and the "
    "population, (2) the key quantitative findings, and (3) what it implies for "
    "features of an activity-vs-glucose analytics app. Be faithful to the text; "
    "do not invent numbers. Plain prose, no preamble."
)


class LLMUnavailable(RuntimeError):
    """Raised when --llm is requested but cannot be satisfied."""


def llm_available() -> bool:
    if not os.getenv("ANTHROPIC_API_KEY"):
        return False
    try:
        import anthropic  # noqa: F401
    except ModuleNotFoundError:
        return False
    return True


def _client():
    try:
        import anthropic
    except ModuleNotFoundError as exc:  # pragma: no cover - dependency guard
        raise LLMUnavailable(
            "anthropic SDK not installed. Install the dependency group with "
            "`uv sync --group sources`."
        ) from exc
    if not os.getenv("ANTHROPIC_API_KEY"):
        raise LLMUnavailable("ANTHROPIC_API_KEY is not set.")
    return anthropic.Anthropic()


def summarise_document(client, doc: ExtractedDocument, model: str) -> str:
    # Cap the context we send; the front matter + abstract + body head is enough
    # for a faithful summary and keeps token cost predictable.
    body = doc.text[:18000]
    prompt = (
        f"Title: {doc.title or doc.filename}\n"
        f"Journal: {doc.journal or 'unknown'}\n"
        f"DOI: {doc.doi or 'unknown'}\n\n"
        f"Paper text (may be truncated):\n{body}"
    )
    message = client.messages.create(
        model=model,
        max_tokens=600,
        system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
    )
    return "".join(block.text for block in message.content if block.type == "text").strip()


def summarise_documents(
    docs: list[ExtractedDocument],
    model: str = DEFAULT_MODEL,
    on_progress=None,
) -> dict[str, str]:
    """Return {filename: summary} for every successfully extracted document."""
    client = _client()
    summaries: dict[str, str] = {}
    targets = [d for d in docs if d.ok]
    for i, doc in enumerate(targets, start=1):
        if on_progress:
            on_progress(i, len(targets), doc.filename)
        try:
            summaries[doc.filename] = summarise_document(client, doc, model)
        except Exception as exc:  # noqa: BLE001 - one failure shouldn't kill the run
            summaries[doc.filename] = f"_(LLM summary failed: {type(exc).__name__}: {exc})_"
    return summaries
