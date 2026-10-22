# Implementation Plan: Generalized Diabetic Support, Epochal Garmin Ingestion, and Multi-Provider CGM Correlation

**Branch**: `001-generalized-diabetic-telemetry` | **Date**: 2026-10-22 | **Spec**: [spec.md](file:///C:/Users/grlow.LAPTOP-FRGT971S/Documents/Projects/walkies/specs/001-generalized-diabetic-telemetry/spec.md)

**Input**: Feature specification from `/specs/001-generalized-diabetic-telemetry/spec.md`

## Summary

Expand Walkies from a per-walk GPS/MiniMed 780G analysis tool into a generalized diabetic activity and glycemic correlation platform. The implementation adds:
1. **Direct Multi-Provider & File Telemetry Ingestion**: Intraday 15-minute movement epochs and daily summaries from Garmin Connect (7-day default lookback, up to 30-day custom range), plus normalized CGM and insulin event ingestion via direct cloud sync (Medtronic CareLink, Dexcom Share) and CSV import (CareLink, Dexcom Clarity, Tandem t:connect).
2. **Generalized Diabetic Regimen & Dynamic Glucose Units**: Support for Closed-Loop Pump, Standard Pump, Multiple Daily Injections (MDI), and Non-Insulin/Lifestyle profiles, with user-selectable `mmol/L` or `mg/dL` display and input conversion backed by canonical `mmol/L` storage.
3. **Deterministic Daily & Multi-Day Correlation Engine**: Cached 24-hour epoch-to-CGM alignment (`daily_analytics`), explicit `>15m` sensor gap detection, workout-to-epoch deduplication (`linked_walk_id`), per-activity-type glycemic impact breakdowns, and revocable read-only public share links for daily and date-range summaries.

## Technical Context

**Language/Version**: Python `>=3.12` (managed via `uv`), JavaScript / JSX (ES2022+, React 19)

**Primary Dependencies**:
- **Backend**: `fastapi>=0.115.0`, `uvicorn[standard]>=0.32.0`, `garminconnect>=0.3.11`, `httpx>=0.27.0`, `psycopg[binary]>=3.2.0`, `google-auth[requests]>=2.38.0`, `python-multipart>=0.0.12`, `carelink_client2.py`
- **Frontend**: `react@^19.0.0`, `vite`, `recharts`, `leaflet` / `react-leaflet`

**Storage**: SQLite (`backend/analytics.sqlite3`) for local/dev and PostgreSQL (`ANALYTICS_DB_URL`) for cloud deployment via `backend/analytics/adapter.py`, plus tenant-isolated file/token storage in `backend/data/{user_sub}/`

**Testing**: `pytest>=8.0.0` (`uv run pytest -v`) with mocked upstream providers; Vite production build check (`npm --prefix frontend run build`)

**Target Platform**: Containerized web application (Docker / Google Cloud Run + Firebase Hosting or local dev server)

**Project Type**: Full-stack web application (`backend/` FastAPI REST service + `frontend/` React SPA)

**Performance Goals**:
- `<2 minutes` end-to-end 7-day cloud sync and initial 24-hour correlation view render (`SC-001`)
- `<1 second` response and render time for cached daily or multi-day correlation views (`SC-005`)

**Constraints**:
- Canonical `mmol/L` storage for all glycemic values and clinical thresholds (`1 mmol/L = 18.0182 mg/dL`)
- Zero double-counting of steps between 15-minute Garmin intraday epochs and recorded `.fit` walks (`<=1%` variance against provider daily totals, `SC-003`)
- Strict tenant isolation of data and provider tokens (`{DATA_DIR}/{user_sub}/.<provider>`)

**Scale/Scope**: Single-tenant and multi-tenant cloud deployments; up to 96 intraday epochs and 288 CGM readings per user per day (~2,880 epochs and ~8,640 CGM points per 30-day sync batch)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Pre-Research Check | Post-Design Check | Notes |
|-----------|--------------------|-------------------|-------|
| **I. Clinical & Metabolic Accuracy** | ✅ PASS | ✅ PASS | Preserves `4.0–8.0 mmol/L` target, `<=3.9` hypo, `>=12.9` hyper defaults; canonical `mmol/L` storage with dynamic `mmol/L`/`mg/dL` UI conversion; explicit `>15m` sensor gap exclusion. |
| **II. Data Privacy, Security & Tenant Isolation** | ✅ PASS | ✅ PASS | All tables keyed by `user_sub`; Dexcom/CareLink/Garmin session tokens isolated under `{DATA_DIR}/{user_sub}/`; public share links sanitized of PII and revocable. |
| **III. Deterministic & Versioned Analytics** | ✅ PASS | ✅ PASS | `daily_analytics` uses `source_hash` (epochs + glycemic events + clinical profile + `ANALYTICS_VERSION`) and serves `ETag` / `Cache-Control` headers. |
| **IV. Test-Verified Quality & External Mocking** | ✅ PASS | ✅ PASS | All Garmin, CareLink, and Dexcom network calls isolated in provider adapters and mocked in `backend/tests/`; frontend verified via `npm --prefix frontend run build`. |
| **V. Discrete Scope & Branch Discipline** | ✅ PASS | ✅ PASS | Scoped to branch `001-generalized-diabetic-telemetry` and its specified requirements (`FR-001`–`FR-010`). |
| **VI. Direct Provider Integration & Multi-Modal Telemetry (USP)** | ✅ PASS | ✅ PASS | Normalizes Garmin intraday epochs + CareLink/Dexcom/Tandem streams into `activity_epochs` and `glycemic_events` supporting closed-loop, pump, MDI, and lifestyle users. |
| **VII. Evidence-Based Design & Scientific Literature Review** | ✅ PASS | ✅ PASS | Phase 0 `research.md` documents the literature review across `references.md`, `sources/DIGEST.md`, and Europe PMC (ATTD TIR consensus, EASD/ISPAD CGM exercise guidelines, 15-min postprandial/sedentary break studies, and 1–2h / 6–12h post-activity lag windows). |

## Project Structure

### Documentation (this feature)

```text
specs/001-generalized-diabetic-telemetry/
├── plan.md              # This implementation plan
├── research.md          # Phase 0 architectural & provider research
├── data-model.md        # Phase 1 database schema & entity specifications
├── quickstart.md        # Phase 1 end-to-end validation & test guide
├── contracts/
│   └── api-contracts.md # Phase 1 REST API & payload contracts
└── tasks.md             # Phase 2 task breakdown (created by /speckit-tasks)
```

### Source Code (repository root)

```text
backend/
├── main.py                              # FastAPI routes, auth, and sync/import endpoints
├── carelink_client2.py                  # Medtronic CareLink client
├── analytics/
│   ├── __init__.py                      # Public analytics exports
│   ├── db.py                            # Schema init, migrations, CRUD for walks, epochs, glycemic_events, daily_analytics, settings, share_links
│   ├── service.py                       # Walk & daily analytics caching, source hashing, ETag orchestration
│   └── models/
│       ├── daily_correlation.py         # NEW: 24h & multi-day epoch-glycemic alignment, gap detection, category breakdowns
│       └── ...                          # Existing walk models (exercise risk, stress, decoupling)
├── telemetry/                           # NEW: Multi-provider ingestion & normalization package
│   ├── __init__.py
│   ├── garmin_epochs.py                 # Garmin 15-min intraday epoch & daily summary fetcher/normalizer
│   ├── dexcom_client.py                 # Dexcom Share direct cloud client & Clarity CSV parser
│   ├── carelink_normalizer.py           # CareLink API & CSV 24-hour normalizer into glycemic_events
│   └── tandem_parser.py                 # Tandem t:connect CSV parser into glycemic_events
└── tests/
    ├── test_daily_correlation.py        # NEW: Unit tests for epoch alignment, gap detection, workout deduplication
    ├── test_multi_provider_ingest.py    # NEW: Mocked tests for Dexcom cloud/CSV, CareLink 24h, Tandem CSV, Garmin epochs
    ├── test_glucose_units_settings.py   # NEW: Tests for generalized regimens, thresholds, and mmol/L <-> mg/dL settings
    └── ...                              # Existing test suite

frontend/
├── src/
│   ├── App.jsx                          # Top-level navigation (Walks, Daily & Aggregate Dashboard, Settings) & share routing
│   ├── utils/
│   │   └── glucoseUnits.js              # NEW: mmol/L <-> mg/dL conversion, formatting, and axis label helpers
│   └── components/
│       ├── DailyAggregateView.jsx       # NEW: 24-hour epochal timeline, sensor gaps, category breakdown & 7/14/30d trends
│       ├── DexcomSyncModal.jsx          # NEW: Dexcom Share connection & multi-provider CSV import modal
│       ├── UserSettings.jsx             # Extended with glucose unit selector, generalized regimens, custom thresholds, Dexcom config
│       ├── WalkAnalysisView.jsx         # Updated to consume dynamic glucose unit formatting and regimen-aware cards
│       └── WalkList.jsx                 # Updated to display glucose deltas in selected unit and date-range sync controls
```

**Structure Decision**: Extends the existing `backend/` (FastAPI + `analytics/`) and `frontend/` (React + Vite) web application layout. New provider ingestion adapters are modularized under `backend/telemetry/` and daily correlation math is placed in `backend/analytics/models/daily_correlation.py` to avoid bloating `backend/main.py`.

## Complexity Tracking

> No Constitution Check violations. No complexity exceptions required.
