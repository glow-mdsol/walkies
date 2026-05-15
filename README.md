# Walkies

Walkies is a small web app for collecting walk files and generating analysis views that line up activity data with glucose and insulin data.

The current workflow is built around:

- Garmin FIT files for activity, route, heart rate, distance, and altitude
- CareLink CSV exports for glucose, basal rate, and bolus events
- Optional GPX files for route context

The app stores uploaded files by walk date and serves a single analysis view that combines:

- map rendering for the walk route
- stacked distance-aligned charts for altitude, heart rate, blood glucose, basal rate, and bolus events
- summary metrics for the walk window

## Screenshots

### Walk List

![Walkies walk list](img/walks.png)

The landing page lists uploaded walks and gives quick access to analysis views and file management.

### Analysis Overview

![Walkies analysis overview](img/analysis.png)

The main analysis page aligns activity, glucose, insulin, and environmental data against route distance.

### Glucose And Insulin

![Walkies glucose and insulin chart](img/alt_insulin.png)

This chart combines glucose readings, basal delivery, bolus events, and estimated active bolus insulin so changes in glucose can be interpreted against insulin exposure during the walk.

### Stress And Decoupling

![Walkies stress chart](img/stress.png)

This view compares observed heart rate with expected heart rate and highlights residual strain across the route.

### Map View

![Walkies map view](img/map.png)

The map view shows the route coloured by heart-rate intensity, with hourly markers to show progress.

## Insulin Profile

Walkies includes a persistent insulin profile so the bolus decay curve can be tailored to the insulin you actually use rather than relying on a fixed generic duration.

### Profile Setup

![Walkies insulin profile setup](img/insulin_prof.png)

The setup screen stores the insulin name, onset, early action, peak window, and total duration range in the browser. This only needs to be configured once unless you want to change the model.

### Profile Applied In Analysis

![Walkies insulin profile in analysis](img/insulin_profsel.png)

The selected insulin profile is then shown in the Glucose and Insulin panel and used to drive the active insulin curve drawn alongside basal and bolus data.

## Stack

- Backend: FastAPI
- Frontend: React + Vite
- Python environment: `uv`
- Task runner: `task`

## Project Layout

```text
walkies/
├── backend/
│   ├── main.py
│   ├── pyproject.toml
│   └── data/
├── frontend/
│   ├── package.json
│   └── src/
├── Taskfile.yml
└── README.md
```

## Installation

### Prerequisites

**Python 3.11+**

Check your version with `python --version`. Download from [python.org](https://www.python.org/downloads/) if needed.

**Node.js (LTS)**

Check with `node --version`. Download from [nodejs.org](https://nodejs.org/).

**uv** (Python package manager)

```bash
# macOS / Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows
winget install astral-sh.uv
```

Or see the [uv installation docs](https://docs.astral.sh/uv/getting-started/installation/).

**task** (task runner)

```bash
# macOS
brew install go-task

# Windows
winget install Task.Task

# Linux
sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b ~/.local/bin
```

Or see the [Taskfile installation docs](https://taskfile.dev/installation/).

---

### Setup

Clone the repo and install all dependencies in one step:

```bash
git clone <repo-url>
cd walkies
task install
```

This runs `uv sync` in `backend/` and `npm install` in `frontend/`.

### Running

```bash
task
```

This starts:

- FastAPI on `http://localhost:8000`
- Vite on `http://localhost:5173`

Open `http://localhost:5173` in your browser. The Vite dev proxy forwards `/api` to the backend, so no CORS configuration is needed.

### Docker

If you want to run Walkies in containers, set the Google OAuth client ID in the project root `.env` file:

```bash
GOOGLE_CLIENT_ID=your_google_oauth_client_id.apps.googleusercontent.com
VITE_GOOGLE_CLIENT_ID=your_google_oauth_client_id.apps.googleusercontent.com
WALKIES_DEV_BYPASS_AUTH=0
```

Then start the compose stack:

```bash
task docker:up
# or: docker compose up --build
```

That starts:

- backend FastAPI on the internal Docker network
- frontend served by Nginx on `http://localhost:5173`
- Postgres 16 for analytics metadata

The compose stack keeps uploaded walk files and the Postgres data directory in Docker volumes. If you want to override the database connection, set `ANALYTICS_DB_URL` in `.env` before starting the stack.

### Authentication Configuration

Walkies now uses Google sign-in and backend token verification.

Frontend (`frontend/.env.local`):

```bash
VITE_GOOGLE_CLIENT_ID=your_google_oauth_client_id.apps.googleusercontent.com
```

Backend (`backend/.env` or process env):

```bash
GOOGLE_CLIENT_ID=your_google_oauth_client_id.apps.googleusercontent.com
```

Analytics DB backend (optional, defaults to local sqlite file):

```bash
# Default behavior (implicit)
ANALYTICS_DB_URL=sqlite:///./backend/analytics.sqlite3

# PostgreSQL
ANALYTICS_DB_URL=postgresql://user:password@host:5432/walkies
```

To migrate existing analytics and share-link metadata from the legacy local sqlite file into the configured backend:

```bash
task analytics:migrate
```

Typical rollout flow:

1. Set `ANALYTICS_DB_URL` to the target Postgres instance.
2. Run `task analytics:migrate` once.
3. Start the backend against the new database.

Optional dev-only bypass (never enable in production):

```bash
WALKIES_DEV_BYPASS_AUTH=1
```

## Available Tasks

```bash
task           # start backend + frontend
task dev       # same as above
task backend   # backend only
task frontend  # frontend only
task install   # install backend + frontend dependencies
task build     # build the frontend
```

## Data Inputs

### Garmin FIT

Used for walk activity records including:

- timestamps
- GPS coordinates
- heart rate
- cumulative distance
- enhanced altitude

### CareLink CSV

Used for:

- sensor glucose / BG readings
- basal rate
- bolus volume delivered

### GPX

Optional route file used when a FIT track is unavailable.

## API Endpoints

- `GET /api/walks` - list stored walks
- `POST /api/walks/upload` - upload files for a walk date
- `DELETE /api/walks/{date}` - delete a walk
- `GET /api/walks/{date}/analysis` - render the analysis page
- `POST /api/walks/parse-fit-date` - extract a date from a FIT file

## Notes

- Walk files are stored under `backend/data/YYYY-MM-DD/`.
- The analysis page is currently generated server-side as standalone HTML.
- FIT parsing is implemented directly in the backend rather than through an external FIT library.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.