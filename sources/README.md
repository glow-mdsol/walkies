# Reference Sources

This folder holds relevant resources/papers (journal PDFs). The PDFs and the
generated `DIGEST.md` are **gitignored** — they are not committed to the
repository (the PDFs are copyrighted; the digest contains verbatim extracts).

The curated, citable list of these works lives in [`../references.md`](../references.md).

## Source-review tooling

A small tool (`backend/sources_review`) extracts the text from every PDF here,
compacts it, and writes a single reviewable **key resource** — `DIGEST.md` — that
can be read alongside the code to evaluate existing features and suggest new ones.

### Install (one-off)

```bash
task sources:install        # uv sync --group sources  (installs pypdf + anthropic)
```

### Generate the digest

```bash
task sources:list           # list the PDFs that will be processed
task sources:digest         # heuristic digest  -> sources/DIGEST.md
task sources:digest:llm     # + Claude-written per-paper summaries (needs API key)
```

Pass extra CLI flags through Task with `--`, e.g.:

```bash
task sources:digest -- --stdout      # print instead of writing the file
task sources:digest -- -o notes.md   # custom output path
```

### What the digest contains

For each document: bibliographic line (journal + DOI + filename), the extracted
abstract, and machine-selected **key points** (sentences dense in domain terms
and numbers). If `--llm` is used, a faithful 4–6 sentence summary is added per
paper. The file ends with **feature-review prompts** — questions to ask while
reading the evidence against the current app.

> ⚠️ Heuristic key points are extracted sentences, **not** verified claims —
> confirm any figure against the source PDF before relying on it.

### How it works

| Stage | Module | Notes |
|-------|--------|-------|
| extract | `sources_review/extract.py` | `pypdf` text + metadata; DOI/journal parsing; mojibake repair |
| digest | `sources_review/digest.py` | heuristic key-point scoring + Markdown rendering |
| enrich | `sources_review/enrich.py` | optional Claude summaries (`--llm`), lazy import |
| CLI | `python -m sources_review` | `--list`, `--llm`, `--stdout`, `-o` |

The `anthropic` SDK is only needed for `--llm`; the heuristic digest runs without
it (and without network or an API key). Unit tests: `backend/tests/test_sources_review.py`.
