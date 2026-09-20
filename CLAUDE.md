# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## About this project

Geoffrey Low is a Type 1 diabetic (MiniMed 780G MMT-1885 pump + Guardian 4 CGM, mostly in closed-loop AUTO mode) who is an active walker/hiker. This app stores per-walk activity files and generates analyses correlating GPS/activity metrics with glucose, insulin, and pump events.

## Commands & Testing

This project uses **`uv`** for Python dependency management and test running.

```bash
# Development & Servers
task dev       # start both servers with auto-reload (backend :8000, frontend :5173)
task           # alias for task dev
task backend   # backend only (uv run uvicorn main:app --reload)
task frontend  # frontend only (npm run dev)
task install   # install all dependencies (uv sync + npm install)
task build     # production build of frontend → frontend/dist/

# Running Tests (Backend)
uv run pytest -v                     # run full pytest test suite
uv run pytest tests/test_rate_walk.py -v  # run single test file

# Frontend Build / Linting
npm --prefix frontend run build      # verify frontend JSX/Vite production build
```

The Vite proxy forwards `/api` to `:8000`, so no CORS issues in dev.

## Architecture

```
walkies/
├── backend/
│   ├── main.py                # FastAPI app — all API routes, upload & auth middleware
│   ├── pyproject.toml         # UV/Python dependencies
│   ├── uv.lock                # UV lockfile
│   ├── Dockerfile             # Container definition for deployment
│   ├── analytics/             # Analytics engine, DB persistence, caching & migrations
│   │   ├── db.py              # SQLite/PostgreSQL engine (walk_analytics, share_links, user_settings, system_logs)
│   │   ├── migrate.py         # UUID walk ID migration & database schema maintenance
│   │   ├── service.py         # Caching and recomputation orchestration
│   │   └── models/            # Domain models (exercise risk, stress decoupling, etc.)
│   ├── sources_review/        # Academic literature review and research digest tools
│   ├── data/                  # Local data storage (gitignored)
│   │   └── {user_sub}/        # User-isolated data partition
│   │       └── {YYYY-MM-DD}/  # Walk date directory
│   │           └── {walk_id}/ # UUID-named walk directory (contains .fit, .csv, .gpx, meta.json)
│   └── tests/                 # Pytest test suite
└── frontend/
    ├── package.json           # React 19, Vite, Leaflet, Recharts
    ├── vite.config.js         # dev proxy: /api → http://localhost:8000
    └── src/
        ├── App.jsx            # Main app shell, navigation, Google OAuth state
        └── components/        # UI components (AnalysisView, DynamicWalkMap, GarminSyncModal, CarelinkSyncModal, SettingsView, SystemLogsSection, etc.)
```

### Key API Routes

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/walks` | List all walks for authenticated user (newest-first) |
| POST | `/api/walks/upload` | Multipart: `date` (YYYY-MM-DD), `name`, `files[]` (.fit, .csv, .gpx) |
| DELETE | `/api/walks/{walk_id}` | Remove a walk folder, metadata, and analytics cache |
| GET | `/api/walks/{walk_id}/analysis` | Full computed analysis payload (cached with ETag) |
| POST | `/api/walks/{walk_id}/rename` | Rename a walk (updates meta.json and analytics DB) |
| POST | `/api/walks/{walk_id}/rate` | Rate a walk 1–5 stars |
| POST | `/api/walks/{walk_id}/share-links` | Generate public share link for a walk |
| GET | `/api/walks/{walk_id}/share-links` | List active share links for walk |
| DELETE | `/api/share-links/{token}` | Revoke a public share link |
| GET | `/api/shared/{token}/analysis` | Public read-only analysis endpoint (no login required) |
| GET/POST | `/api/settings` | Get/update user diabetic profile and treatment parameters |
| POST | `/api/garmin/login` | Authenticate Garmin Connect (supports 2FA / MFA verification) |
| POST | `/api/garmin/sync` | Sync and import walking FIT files from Garmin Connect |
| POST | `/api/carelink/upload-token` | Upload CareLink authentication token file |
| POST | `/api/carelink/sync` | Automatically sync CGM and pump events from CareLink |
| GET/DELETE | `/api/system/logs` | Query or clear diagnostic and application event logs |

### File Storage & Isolation
- User-isolated storage: `backend/data/{user_sub}/{YYYY-MM-DD}/{walk_id}/`.
- Canonical ID is a UUID. Backward-compatible lookups match legacy date-slugs via `meta.json` `legacy_id`.

## Data Formats

### Carelink CSV (Medtronic MiniMed 780G)
- 6 header rows to skip: `pd.read_csv(skiprows=6)` or custom line stripper
- Filter rows where `Date` matches `\d{4}/\d{2}/\d{2}`
- Datetime: `Date + ' ' + Time`, format `%Y/%m/%d %H:%M:%S`
- Glucose values are in **mmol/L**
- Detects bolus volumes, carb entries, basal rates, and SmartGuard suspend states (`PLGM_PREDICTED_LOW_SG`, `NORMAL_PUMPING`, etc.)

### Garmin FIT Files
- Use the **custom binary parser** in `backend/main.py`.
- Records use compressed timestamps; seed `last_timestamp` from field 253 in non-compressed messages, then apply 5-bit rollover logic for compressed headers.
- Field mapping: `0`=lat, `1`=lon (sint32 semicircles → degrees via `* 180/2^31`), `3`=heart_rate (uint8), `5`=distance (uint32, cm→m via `/100`), `253`=timestamp (uint32, seconds since 1989-12-31).

### GPX Files
- XML planned or recorded route parsed via `xml.etree.ElementTree`.

## Clinical Context

| Parameter | Value |
|-----------|-------|
| Target glucose range | 4.0–8.0 mmol/L |
| Hypo threshold | ≤ 3.9 mmol/L |
| Hyper threshold | ≥ 12.9 mmol/L |
| Insulin:carb ratio (default) | 5 g/U |
| Sensitivity factor (default) | 2 mmol/L per U |
| DIA (Insulin duration) | ~4 hours (peak action ~75 min for rapid analogs) |

## Workflow, Branching & Scope Discipline (Keep Work Discrete)
- **Branch-per-feature**: Always work on a dedicated, short-lived branch for each task/feature (e.g. `feat/...`, `fix/...`). Never accumulate unrelated changes directly on `main`.
- **Discrete, focused PRs**: Keep pull requests small and tightly scoped to prevent enormous, unreviewable diffs.
- **Strictly avoid feature creep**: Implement only what is explicitly requested for the task at hand. Do not bundle opportunistic refactoring or tangential features into the active branch. Capture additional ideas in `features.md` instead.

## Development & Security Guidelines
- Always verify all unit tests pass with `uv run pytest -v` before committing.
- Authentication uses Google OAuth with bearer tokens. For local development/testing without Google OAuth, set `WALKIES_DEV_BYPASS_AUTH=1`.
- Never commit user credentials, OAuth client secrets, or private medical data files to Git.
