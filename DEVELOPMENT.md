# Developer Guide

This document covers local development setup, environment configuration, database management, testing, and API architecture for Walkies.

---

## 1. Technology Stack & Architecture

- **Backend**: Python 3.11+ / FastAPI, Uvicorn, SQLite / PostgreSQL (SQLAlchemy), Pandas
- **Frontend**: React 19, Vite, Leaflet, Recharts
- **Package Managers**: `uv` (Python), `npm` (JavaScript)
- **Task Runner**: `task` ([Taskfile.dev](https://taskfile.dev/))
- **Containerization**: Docker & Docker Compose

### Project Layout

```text
walkies/
├── backend/
│   ├── main.py                # FastAPI application: auth, routers, API endpoints, upload handlers
│   ├── pyproject.toml         # Python project configuration and dependencies (uv)
│   ├── uv.lock                # uv lockfile
│   ├── Dockerfile             # Container definition for backend
│   ├── analytics/             # Modular analytics engine & persistence
│   │   ├── db.py              # SQLite/PostgreSQL database engine, migrations, system logs, settings
│   │   ├── migrate.py         # UUID migration & DB schema upgrades
│   │   ├── service.py         # Cache management & analytics recomputation orchestration
│   │   └── models/            # Analytics computation models (stress, risk, decouple, etc.)
│   ├── sources_review/        # Academic literature review and research digest tools
│   ├── data/                  # Local data storage (gitignored)
│   │   └── {user_sub}/        # User-isolated data partition
│   │       └── {YYYY-MM-DD}/  # Walk date directory
│   │           └── {walk_id}/ # UUID-named walk directory (contains .fit, .csv, .gpx, meta.json)
│   └── tests/                 # Pytest test suite
├── frontend/
│   ├── package.json           # Node dependencies (React 19, Vite, Leaflet, Recharts)
│   ├── vite.config.js         # Vite config with API reverse proxy (/api -> :8000)
│   ├── index.html             # Single-page application entry point
│   └── src/
│       ├── main.jsx           # React root renderer
│       ├── App.jsx            # Main dashboard, tab navigation, auth state management
│       └── components/        # UI components (AnalysisView, DynamicWalkMap, GarminSyncModal, CarelinkSyncModal, etc.)
├── .env.example               # Environment variable template
├── Taskfile.yml               # Unified development task runner commands
└── docker-compose.yml         # Local multi-container development stack
```

---

## 2. Installation & Prerequisites

### Prerequisites

1. **Python 3.11+**
   - Check with `python --version`. Download from [python.org](https://www.python.org/downloads/) if needed.
2. **Node.js (LTS)**
   - Check with `node --version`. Download from [nodejs.org](https://nodejs.org/).
3. **uv** (Python package & environment manager)
   - macOS / Linux: `curl -LsSf https://astral.sh/uv/install.sh | sh`
   - Windows: `winget install astral-sh.uv`
   - Or refer to [uv installation docs](https://docs.astral.sh/uv/getting-started/installation/).
4. **task** (Task runner)
   - macOS: `brew install go-task`
   - Windows: `winget install Task.Task`
   - Linux: `sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b ~/.local/bin`
   - Or refer to [Taskfile installation docs](https://taskfile.dev/installation/).

---

### Setup

Clone the repository and install all frontend and backend dependencies in one step:

```bash
git clone <repo-url>
cd walkies
task install
```

This runs `uv sync` in `backend/` and `npm install` in `frontend/`.

---

## 3. Running the Application

### Development Server

```bash
task dev
# or simply:
task
```

This starts:
- **FastAPI backend** on `http://localhost:8000` (with live reload)
- **Vite frontend** on `http://localhost:5173` (with HMR)

Open `http://localhost:5173` in your browser. The Vite dev proxy automatically forwards `/api` requests to `:8000`, so no manual CORS setup is needed.

### Running Individual Services

```bash
task backend    # Run FastAPI backend server only
task frontend   # Run Vite frontend dev server only
```

---

## 4. Environment & Authentication Configuration

Walkies uses Google OAuth sign-in and backend ID token verification. For local development, configuration is managed via a single `.env` file in the project root (automatically loaded for both backend and frontend tasks).

### Creating `.env`

Copy the template from `.env.example`:

```bash
cp .env.example .env
```

### Key Configuration Variables

| Variable | Description | Default / Example |
|---|---|---|
| `WALKIES_DEV_BYPASS_AUTH` | When set to `1`, bypasses Google OAuth verification and creates a local dev user session. | `1` (for local dev) |
| `GOOGLE_CLIENT_ID` | Backend Google OAuth 2.0 Client ID for validating tokens. | `your_client_id.apps.googleusercontent.com` |
| `VITE_GOOGLE_CLIENT_ID` | Frontend Google OAuth 2.0 Client ID for Google Identity Services login button. | `your_client_id.apps.googleusercontent.com` |
| `ANALYTICS_DB_URL` | Database connection URL for analytics caching and user metadata. | `sqlite:///./backend/analytics.sqlite3` |
| `DATA_DIR` | Local root directory for uploaded walk files (.fit, .csv, .gpx). | `backend/data` |
| `GCS_BUCKET_NAME` | (Optional) Cloud Storage bucket for file storage in GCP environments. | Empty / unused locally |

---

## 5. Database Backend & Migrations

By default, Walkies uses a local SQLite database (`backend/analytics.sqlite3`). In production or multi-container environments, PostgreSQL is supported.

```bash
# SQLite (default)
ANALYTICS_DB_URL=sqlite:///./backend/analytics.sqlite3

# PostgreSQL
ANALYTICS_DB_URL=postgresql://user:password@localhost:5432/walkies
```

### Running Migrations

To migrate existing analytics, settings, and share links into a new database or upgrade schema versions:

```bash
task analytics:migrate
```

Typical PostgreSQL migration rollout:
1. Set `ANALYTICS_DB_URL` in `.env` to the target PostgreSQL instance.
2. Run `task analytics:migrate`.
3. Start the backend against the new database.

---

## 6. Docker & Containerized Development

To run the complete application stack (FastAPI backend, Nginx frontend, PostgreSQL 16) with Docker Compose:

1. Configure `.env` with your OAuth settings:
   ```bash
   GOOGLE_CLIENT_ID=your_google_oauth_client_id.apps.googleusercontent.com
   VITE_GOOGLE_CLIENT_ID=your_google_oauth_client_id.apps.googleusercontent.com
   WALKIES_DEV_BYPASS_AUTH=0
   ```

2. Start the compose stack:
   ```bash
   task docker:up
   # or: docker compose up --build
   ```

The compose stack runs:
- Backend FastAPI on the internal Docker network
- Frontend served by Nginx on `http://localhost:5173`
- PostgreSQL 16 on `localhost:5432` with volume persistence

To stop the containers:
```bash
task docker:down
# or: docker compose down
```

---

## 7. Testing & Quality Assurance

### Backend Tests

Pytest test suite is located in `backend/tests/`. Always run tests using `uv`:

```bash
# Run the entire test suite
uv run pytest -v

# Run a specific test file
uv run pytest tests/test_rate_walk.py -v
```

### Frontend Build Verification

To verify JSX syntax and Vite asset bundling:

```bash
task build
# or: npm --prefix frontend run build
```

---

## 8. Available Task Commands

| Command | Description |
|---|---|
| `task` / `task dev` | Start backend (:8000) and frontend (:5173) in parallel |
| `task backend` | Start FastAPI backend with live reload |
| `task frontend` | Start Vite frontend dev server |
| `task install` | Install all dependencies (`uv sync` + `npm install`) |
| `task build` | Build production frontend bundle into `frontend/dist/` |
| `task test` | Run backend test suite via `pytest` |
| `task analytics:migrate` | Run database schema and analytics data migrations |
| `task docker:up` | Build and run Docker Compose stack |
| `task docker:down` | Stop and remove Docker Compose containers |

---

## 9. API Reference

| Method | Endpoint | Description | Auth Required |
|---|---|---|---|
| `GET` | `/api/walks` | List all walks for authenticated user (newest first) | Yes |
| `POST` | `/api/walks/upload` | Upload walk files (.fit, .csv, .gpx) with date & name | Yes |
| `DELETE` | `/api/walks/{walk_id}` | Delete a walk, metadata, and cached analytics | Yes |
| `GET` | `/api/walks/{walk_id}/analysis` | Full computed analysis payload (cached with ETag) | Yes |
| `POST` | `/api/walks/{walk_id}/rename` | Rename a walk (updates meta.json and analytics DB) | Yes |
| `POST` | `/api/walks/{walk_id}/rate` | Rate a walk 1–5 stars | Yes |
| `POST` | `/api/walks/{walk_id}/share-links` | Generate a public share token for a walk | Yes |
| `GET` | `/api/walks/{walk_id}/share-links` | List active public share links for a walk | Yes |
| `DELETE` | `/api/share-links/{token}` | Revoke a public share token | Yes |
| `GET` | `/api/shared/{token}/analysis` | Public read-only analysis endpoint | No |
| `GET` / `POST` | `/api/settings` | Get or update user diabetic profile & treatment parameters | Yes |
| `POST` | `/api/garmin/login` | Authenticate Garmin Connect account (with MFA support) | Yes |
| `POST` | `/api/garmin/sync` | Sync and import walking FIT files from Garmin Connect | Yes |
| `POST` | `/api/carelink/upload-token` | Upload CareLink authentication token file | Yes |
| `POST` | `/api/carelink/sync` | Sync CGM and pump events from CareLink Cloud | Yes |
| `GET` / `DELETE` | `/api/system/logs` | Query or clear diagnostic and application event logs | Yes |
