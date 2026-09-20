# AGENTS.md

This document provides system guidelines, architecture overview, testing conventions, and development practices for AI agents working on Walkies.

---

## 1. Project Overview & Clinical Context

**Walkies** is an open-source web application designed for Type 1 diabetics (specifically tuned for MiniMed 780G pump + Guardian 4 CGM users and Garmin GPS sport watches) who hike/walk. The platform correlates activity performance metrics (heart rate, pace, elevation profile, distance) with physiological and glycemic signals (continuous blood glucose, basal deliveries, bolus timing, SmartGuard automation, and insulin-on-board).

### Clinical & Metabolic Parameters
- **Target Range**: 4.0 – 8.0 mmol/L (72 – 144 mg/dL)
- **Hypoglycemia Threshold**: ≤ 3.9 mmol/L (70 mg/dL)
- **Hyperglycemia Threshold**: ≥ 12.9 mmol/L (232 mg/dL)
- **Standard Carb Ratio (default)**: 5 g/U
- **Insulin Sensitivity (ISF default)**: 2.0 mmol/L per U
- **Insulin Decay (DIA)**: ~4 hours (peak action ~75 min for rapid-acting analogs)
- **SmartGuard**: Detects pump suspend/resume states (`PLGM_PREDICTED_LOW_SG`, `NORMAL_PUMPING`, manual suspend) and calculates exercise hypo risk.

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
│   ├── analytics/                   # Modular analytics engine & persistence
│   │   ├── __init__.py              # Public exports for analytics helpers
│   │   ├── db.py                    # SQLite/PostgreSQL database engine, migrations, system logs, settings
│   │   ├── migrate.py               # UUID migration & DB schema upgrades
│   │   ├── service.py               # Cache management & analytics recomputation orchestration
│   │   └── models/                  # Analytics computation modules (stress, risk, decouple, etc.)
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
│       ├── test_exercise_risk.py
│       ├── test_garmin_sync.py
│       ├── test_logging.py
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
│   │   ├── components/              # Modular UI components
│   │   │   ├── AnalysisView.jsx     # Comprehensive walk analysis & multi-panel charts
│   │   │   ├── DynamicWalkMap.jsx   # Interactive Leaflet map (BG & HR polyline shading)
│   │   │   ├── GarminSyncModal.jsx  # Garmin Connect direct sync with MFA support
│   │   │   ├── CarelinkSyncModal.jsx# Carelink sync token management
│   │   │   ├── SettingsView.jsx     # Diabetes profile & user settings manager
│   │   │   ├── SystemLogsSection.jsx# Real-time event and error logs viewer
│   │   │   ├── UploadForm.jsx       # Multi-file upload interface
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
- Analytics calculations (distance, duration, HR decoupling, BG drop rate, Time In Range, SmartGuard events, IOB proxies) are computed deterministically.
- Computed analytics are cached in an SQLite database (or Postgres in cloud environments via `ANALYTICS_DB_URL`).
- Versioning is enforced with `ANALYTICS_VERSION`. When the version increments or files change (validated by source file hashing), analytics are automatically recomputed.
- Read operations send HTTP `ETag` and `Cache-Control` headers.

### 3. Sharing Links (Public Access)
- Authenticated users can generate shareable public links for individual walks (`share_links` table).
- Public share tokens allow unauthenticated users to view read-only analysis without leaking user directories or other walks.
- Tokens can be revoked by the owner at any time.

### 4. Logging & Diagnostics
- Application events, sync results, and errors are captured via `WalkiesDBLogHandler` and stored in the database (`system_logs` table).
- The frontend exposes a dedicated **System Logs** dashboard to view, filter, and clear diagnostic events.

---

## 5. Testing & Quality Guidelines

1. **Always Verify Tests**: Before committing or pushing code, run the entire test suite using `uv run pytest -v` from `backend/`. Ensure all tests pass.
2. **Mocking External APIs**: External services (Garmin Connect, Medtronic Carelink, Google Auth) must always be mocked in unit tests (see `test_garmin_sync.py` and `test_carelink_sync.py`).
3. **Frontend Build Verification**: Run `npm --prefix frontend run build` to ensure JSX and asset compilation succeed without syntax or packaging errors.
4. **Data Privacy**: Never commit sensitive user files, actual Carelink CSVs with PII, Garmin login credentials, or OAuth client secrets to the Git repository.
