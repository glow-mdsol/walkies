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

The landing page provides a secure access screen. Unauthenticated visitors can sign in using Google Login or enter a shared route code to view a shared analysis. Once authenticated, the page displays the list of uploaded walks, letting you view individual analysis files, route comparisons, and manage files.

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

### Profile Setup & Templates

![Walkies insulin profile setup](img/insulin_prof.png)

The setup screen lets you customize your insulin configuration. For fast setup, the app provides standard predefined configurations for common fast-acting and short-acting insulins:
- **Fiasp (Ultra-Rapid)**
- **NovoRapid (Rapid)**
- **Humalog (Rapid)**
- **Apidra (Rapid)**
- **Actrapid (Short-Acting / Regular)**
- **Humulin S (Short-Acting / Regular)**

Selecting one of these templates automatically configures standard clinical values for onset time, early action time, peak action window, and total duration. If you adjust any slider or modify the name, the profile automatically switches to **Custom (Enter manually)** so you can fine-tune parameters to your body's kinetics.

### Profile Applied In Analysis

![Walkies insulin profile in analysis](img/insulin_profsel.png)

The selected insulin profile is shown as an active badge in the Glucose and Insulin panel and drives the pharmacokinetic active insulin on board (IOB) decay curve drawn alongside basal and bolus data.

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

Walkies uses Google sign-in and backend token verification. For local development, these variables are configured in a single `.env` file in the project root (which is automatically loaded for both backend and frontend dev tasks):

Root `.env` configuration:

```bash
GOOGLE_CLIENT_ID=your_google_oauth_client_id.apps.googleusercontent.com
VITE_GOOGLE_CLIENT_ID=your_google_oauth_client_id.apps.googleusercontent.com

# Optional dev-only bypass (never enable in production)
WALKIES_DEV_BYPASS_AUTH=1
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

## Production Deployment (Google Cloud Platform)

This project can be deployed to Google Cloud Platform (GCP) entirely within their free tier boundaries (using Google Cloud Run, Firebase Hosting, Google Cloud Storage, and a free Neon.tech serverless PostgreSQL database).

### 1. Prerequisites
- Google Cloud SDK (`gcloud` CLI) installed and authenticated on your local machine (`gcloud auth login`).
- A GCP Project created (note the Project ID).
- A free database account at [Neon](https://neon.tech/) to get a PostgreSQL connection string.

### 2. Provision Resources
We provide a helper script to configure the GCP services. Run it locally using your authenticated terminal:
```bash
chmod +x deploy.sh
./deploy.sh <YOUR_PROJECT_ID> <YOUR_REGION>
```
*Note: If no region is provided, it defaults to `us-central1`.*

This script will:
- Enable the necessary GCP APIs.
- Create an Artifact Registry repository (`walkies-repo`) for Docker images.
- Create a Cloud Storage bucket (`gs://walkies-data-<project-id>`) for durable FIT and CSV file storage.
- Create a deployment Service Account (`walkies-deployer`) with permissions.
- Generate a local credentials key file (`walkies-deployer-key.json`) for GitHub Actions.

### 3. Initialize Firebase Hosting
Go to the [Firebase Console](https://console.firebase.google.com/), select your existing GCP project, go to **Hosting**, and click **Get Started** to initialize the hosting project.

### 4. Configure GitHub Secrets
Go to your GitHub repository **Settings > Secrets and variables > Actions** and add the following secrets:
- `GCP_PROJECT_ID`: Your GCP project ID.
- `GCP_SA_KEY`: The entire contents of the generated `walkies-deployer-key.json` file.
- `GCP_REGION`: The region you selected (e.g., `us-central1`).
- `GOOGLE_CLIENT_ID`: Your Google OAuth 2.0 Client ID.
- `ANALYTICS_DB_URL`: The Neon serverless PostgreSQL connection string.

*After saving the `GCP_SA_KEY` to GitHub, safely delete `walkies-deployer-key.json` from your local machine. Do not commit this file.*

### 5. Deploy
Push the codebase to the `main` branch of your GitHub repository. The Actions workflow in `.github/workflows/deploy.yml` will trigger, build, and deploy the application.

### 6. Post-Deployment Steps
- **OAuth Redirect URIs:** Add your Firebase Hosting URL (e.g., `https://<project-id>.web.app`) to your Google Cloud Console OAuth credentials under "Authorized JavaScript Origins" and "Authorized Redirect URIs" so sign-in works in production.
- **Run Migrations:** Run the migration script against production:
  ```bash
  # In your local terminal, temporarily override ANALYTICS_DB_URL:
  export ANALYTICS_DB_URL="postgresql://user:password@host/dbname?sslmode=require"
  task analytics:migrate
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