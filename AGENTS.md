# AGENTS.md

This document provides system guidelines, architecture overview, testing conventions, and development practices for AI agents working on Walkies.

---

## 1. Project Overview & Clinical Context

**Walkies** is an open-source web application designed for individuals with diabetes across all regimens (Closed-Loop AID Pump such as MiniMed 780G / Tandem Control-IQ, Standard Insulin Pump, Multiple Daily Injections / MDI, and Non-Insulin / Lifestyle therapy, as well as Non-Diabetic CGM users) and Garmin GPS sport watch users who hike/walk. The platform correlates both recorded walk workouts and 24-hour / multi-day Garmin intraday step and activity epochs (`15-minute` buckets) with physiological and glycemic signals (continuous blood glucose from Medtronic CareLink, Dexcom Share/Clarity, and Tandem t:connect, basal deliveries, bolus timing, closed-loop automation, and insulin-on-board).

### Clinical & Metabolic Parameters
- **Glucose Display Units**: Configurable per user (`mmol/L` default or `mg/dL`, `1 mmol/L = 18.0182 mg/dL`); canonical backend storage and analytics use `mmol/L`.
- **Target Range (default)**: 4.0 – 8.0 mmol/L (72 – 144 mg/dL), customizable per user
- **Hypoglycemia Threshold (default)**: ≤ 3.9 mmol/L (70 mg/dL), customizable per user (ATTD consensus: ≥15 consecutive minutes for sustained episode counting)
- **Hyperglycemia Threshold (default)**: ≥ 12.9 mmol/L (232 mg/dL), customizable per user
- **Standard Carb Ratio (default)**: 5 g/U
- **Insulin Sensitivity (ISF default)**: 2.0 mmol/L per U (36 mg/dL per U)
- **Insulin Decay (DIA)**: ~4 hours (peak action ~75 min for rapid-acting analogs)
- **Closed-Loop / SmartGuard**: Detects pump suspend/resume states (`PLGM_PREDICTED_LOW_SG`, `NORMAL_PUMPING`, Control-IQ exercise/suspend, manual suspend) and calculates exercise hypo risk.

---

## 2. Repository Structure

```
walkies/
├── backend/
│   ├── main.py                      # FastAPI application: auth, routers, API endpoints, upload handlers
│   ├── pyproject.toml               # Python project configuration and dependencies managed via uv
│   ├── uv.lock                      # uv lockfile
│   ├── Dockerfile                   # Container definition for backend deployment
│   ├── carelink_client2.py          # Standalone client for Medtronic Carelink data
│   ├── telemetry/                   # Multi-provider activity & CGM/insulin normalizers and clients
│   │   ├── __init__.py              # Public exports for telemetry ingestion & unit conversion
│   │   ├── common.py                # Canonical mmol/L <-> mg/dL conversion & UTC timestamp helpers
│   │   ├── garmin_epochs.py         # Garmin 15-min intraday epoch & daily wellness fetcher/normalizer
│   │   ├── carelink_normalizer.py   # Medtronic CareLink 24h API & CSV normalizer
│   │   ├── dexcom_client.py         # Dexcom Share cloud client (US/OUS) & Dexcom Clarity CSV parser
│   │   └── tandem_parser.py         # Tandem t:connect CSV parser & multi-provider CSV auto-detector
│   ├── analytics/                   # Modular analytics engine & persistence
│   │   ├── __init__.py              # Public exports for analytics helpers
│   │   ├── db.py                    # SQLite/PostgreSQL database engine, migrations, system logs, settings
│   │   ├── migrate.py               # UUID migration & DB schema upgrades
│   │   ├── service.py               # Cache management & analytics recomputation orchestration
│   │   └── models/                  # Analytics computation modules (daily_correlation, stress, risk, etc.)
│   ├── sources_review/              # Research & PubMed literature digest tools
│   │   ├── literature.py            # Academic literature parsing and DOI resolver
│   │   └── sources_review.py        # Scientific literature reviewer CLI & digest builder
│   ├── data/                        # Local data root (gitignored)
│   │   └── {user_sub}/              # Per-user isolated directory
│   │       └── {YYYY-MM-DD}/        # Walk date directory
│   │           └── {walk_id}/       # UUID-named walk directory containing FIT, CSV, GPX, and meta.json
│   └── tests/                       # Pytest test suite
│       ├── conftest.py              # Pytest fixtures and mock setup
│       ├── test_account_deletion.py
│       ├── test_carelink_sync.py
│       ├── test_daily_correlation.py
│       ├── test_exercise_risk.py
│       ├── test_garmin_sync.py
│       ├── test_glucose_units_settings.py
│       ├── test_logging.py
│       ├── test_multi_provider_ingest.py
│       ├── test_rate_walk.py
│       ├── test_rename_walk.py
│       ├── test_settings.py
│       ├── test_sources_review.py
│       └── test_walk_uuid_migration.py
├── frontend/
│   ├── package.json                 # Node dependencies (React 19, Vite, Leaflet, Recharts)
│   ├── vite.config.js               # Vite config with API reverse proxy (/api -> :8000)
│   ├── index.html                   # Single-page application entry point
│   ├── src/
│   │   ├── main.jsx                 # React root renderer
│   │   ├── App.jsx                  # Main dashboard, tab navigation, auth state management
│   │   ├── utils/
│   │   │   └── glucoseUnits.js      # Frontend mmol/L <-> mg/dL conversion & formatting utilities
│   │   ├── components/              # Modular UI components
│   │   │   ├── WalkAnalysisView.jsx # Comprehensive walk analysis & multi-panel charts
│   │   │   ├── DailyAggregateView.jsx # 24h & multi-day epochal step/activity & CGM correlation dashboard
│   │   │   ├── GarminSyncModal.jsx  # Garmin Connect direct sync with MFA & 7–30 day lookback support
│   │   │   ├── CarelinkSyncModal.jsx# Carelink sync token management
│   │   │   ├── DexcomSyncModal.jsx  # Dexcom Share cloud sync & multi-provider CSV import modal
│   │   │   ├── UserSettings.jsx     # Diabetes regimen, glucose unit, sync & user settings manager
│   │   │   ├── SystemLogsSection.jsx# Real-time event and error logs viewer
│   │   │   ├── UploadForm.jsx       # Multi-file walk & standalone telemetry CSV upload interface
│   │   │   └── WalkList.jsx         # Walk library, search, filtering, and summary cards
│   │   └── styles/                  # Clean modern CSS stylesheets
├── .github/
│   └── workflows/                   # CI/CD workflows for frontend & backend testing and deployment
├── features.md                      # Feature backlog and acceptance criteria
├── CLAUDE.md                        # Claude assistant guidelines
└── AGENTS.md                        # AI agent documentation and architecture guide (this file)
```

---

## 3. Tooling & Execution Environment

### Python Environment (`uv`)
This project uses **`uv`** as the Python package manager and runner. Always execute Python tasks and tests with `uv` or through the active virtual environment (`.venv`).

- **Install Dependencies**:
  ```bash
  uv sync
  ```
- **Run Backend Test Suite**:
  ```bash
  # From backend/ directory
  uv run pytest -v
  # Or directly invoking pytest in .venv
  .venv/bin/pytest -v         # Linux/macOS
  .venv\Scripts\pytest -v     # Windows
  ```
- **Run a Specific Test File**:
  ```bash
  uv run pytest tests/test_rate_walk.py -v
  ```
- **Run Backend Server**:
  ```bash
  uv run uvicorn main:app --host 0.0.0.0 --port 8000 --reload
  ```

### Frontend Environment (`npm` & `vite`)
- **Install Dependencies**:
  ```bash
  npm --prefix frontend install
  ```
- **Start Dev Server**:
  ```bash
  npm --prefix frontend run dev
  ```
- **Build Production Assets**:
  ```bash
  npm --prefix frontend run build
  ```

### Unified Task Runner (`Taskfile` / `task`)
If `task` is installed:
- `task dev`: Runs backend (:8000) and frontend (:5173) in parallel.
- `task test`: Runs backend test suite via `uv run pytest`.
- `task build`: Runs frontend production build.

---

## 4. Key Architectural Patterns

### 1. Identity & Data Isolation
- User authentication uses Google OAuth ID tokens (`Bearer <token>`).
- For local testing, `WALKIES_DEV_BYPASS_AUTH=1` bypasses OAuth and defaults to a dev test user.
- All walk assets live in `{DATA_DIR}/{user_sub}/{YYYY-MM-DD}/{walk_id}/`.
- Walk directories are identified by canonical UUIDs (`meta.json` contains `"id": "<uuid>"`, `"name"`, `"date"`, `"legacy_id"`, and file metadata).

### 2. Analytics Engine & Caching
- Analytics calculations (walk distance, duration, HR decoupling, BG drop rate, Time In Range, SmartGuard events, IOB proxies, and 24-hour / multi-day epochal step-glycemic correlation) are computed deterministically.
- Computed analytics are cached in `walk_analytics` and `daily_analytics` tables in an SQLite database (or Postgres in cloud environments via `ANALYTICS_DB_URL`).
- Versioning is enforced with `ANALYTICS_VERSION`. When the version increments, user glycemic thresholds change, or source files/telemetry rows change (validated by source hashing), analytics are automatically recomputed.
- Read operations send HTTP `ETag` and `Cache-Control` headers.

### 3. Sharing Links (Public Access)
- Authenticated users can generate shareable public links for individual walks (`share_type='walk'`), single-day summaries (`share_type='daily'`), and multi-day aggregate ranges (`share_type='range'`) via the `share_links` table.
- Public share tokens allow unauthenticated users to view read-only analysis without leaking user directories or other dates/walks.
- Tokens can be revoked by the owner at any time.

### 4. Logging & Diagnostics
- Application events, sync results, and errors are captured via `WalkiesDBLogHandler` and stored in the database (`system_logs` table).
- The frontend exposes a dedicated **System Logs** dashboard to view, filter, and clear diagnostic events.

---

## 5. Workflow, Branching & Scope Discipline

### 1. Keep Work Discrete (Avoid Enormous PRs)
- **Branch-per-feature**: Always work on a dedicated, short-lived branch for each feature, fix, or chore (e.g., `feat/carb-markers`, `fix/fit-parser-offset`, `refactor/share-token-cleanup`). Never accumulate unrelated changes directly on `main`.
- **Strict Scope Boundaries**: Strongly avoid **feature creep**. Implement only what is explicitly requested or documented for the current task. Do not bundle opportunistic refactors or unrelated changes into the same branch/PR.
- **Atomic Commits & Discrete PRs**: Keep PRs small, focused, and easily reviewable. If a new idea or improvement is noticed during implementation, add it to `features.md` or a new task rather than expanding the active branch.

---

## 6. Testing & Quality Guidelines

1. **Always Verify Tests**: Before committing or pushing code, run the entire test suite using `uv run pytest -v` from `backend/`. Ensure all tests pass.
2. **Mocking External APIs**: External services (Garmin Connect, Medtronic Carelink, Google Auth) must always be mocked in unit tests (see `test_garmin_sync.py` and `test_carelink_sync.py`).
3. **Frontend Build Verification**: Run `npm --prefix frontend run build` to ensure JSX and asset compilation succeed without syntax or packaging errors.
4. **Data Privacy**: Never commit sensitive user files, actual Carelink CSVs with PII, Garmin login credentials, or OAuth client secrets to the Git repository.
