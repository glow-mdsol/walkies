"""CLI: extract, summarise and compact the source PDFs into a digest.

Examples:
    python -m sources_review                 # heuristic digest -> sources/DIGEST.md
    python -m sources_review --llm           # enrich summaries with Claude
    python -m sources_review --list          # just list what would be processed
    python -m sources_review -o out.md       # custom output path
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .digest import DEFAULT_DIGEST_PATH, build_digest, write_digest
from .extract import SOURCES_DIR, extract_sources

# Titles/abstracts contain en/em dashes and other non-Latin-1 characters. The
# Windows console defaults to cp1252, which raises on those; force UTF-8 so the
# CLI prints cleanly everywhere (the digest file is already written as UTF-8).
for _stream in (sys.stdout, sys.stderr):
    reconfigure = getattr(_stream, "reconfigure", None)
    if reconfigure is not None:
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):  # pragma: no cover - stream not reconfigurable
            pass


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sources_review",
        description="Extract, summarise and compact the reference PDFs in sources/.",
    )
    parser.add_argument(
        "--sources-dir", type=Path, default=SOURCES_DIR,
        help=f"Directory of PDFs to process (default: {SOURCES_DIR}).",
    )
    parser.add_argument(
        "-o", "--output", type=Path, default=DEFAULT_DIGEST_PATH,
        help=f"Digest output path (default: {DEFAULT_DIGEST_PATH}).",
    )
    parser.add_argument(
        "--llm", action="store_true",
        help="Enrich per-paper summaries with Claude (needs ANTHROPIC_API_KEY).",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="List the documents that would be processed, then exit.",
    )
    parser.add_argument(
        "--stdout", action="store_true",
        help="Print the digest to stdout instead of writing the file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        docs = extract_sources(args.sources_dir)
    except FileNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except ModuleNotFoundError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if not docs:
        print(f"No PDFs found in {args.sources_dir}", file=sys.stderr)
        return 1

    if args.list:
        for doc in docs:
            status = "ok" if doc.ok else f"FAILED ({doc.error})"
            print(f"- {doc.filename} [{status}] — {doc.title or '(no title)'}")
        return 0

    summaries: dict[str, str] = {}
    llm_used = False
    if args.llm:
        from .enrich import LLMUnavailable, summarise_documents

        def _progress(i: int, total: int, name: str) -> None:
            print(f"  [{i}/{total}] summarising {name}…", file=sys.stderr)

        try:
            print("Enriching summaries with Claude…", file=sys.stderr)
            summaries = summarise_documents(docs, on_progress=_progress)
            llm_used = True
        except LLMUnavailable as exc:
            print(f"error: --llm requested but unavailable: {exc}", file=sys.stderr)
            return 2

    content = build_digest(docs, summaries=summaries, llm_used=llm_used)

    if args.stdout:
        print(content)
        return 0

    out_path = write_digest(content, args.output)
    ok = sum(1 for d in docs if d.ok)
    print(f"Wrote {out_path} ({ok}/{len(docs)} documents digested).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
