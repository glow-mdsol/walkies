# Contributing to Walkies

Thank you for your interest in contributing to **Walkies**! Walkies is an open-source telemetry and analytics platform correlating Garmin workouts and 24-hour intraday activity epochs (`15-minute` buckets) with multi-provider CGM and insulin delivery data across all diabetic regimens.

This document outlines our engineering principles, clinical & scientific standards, development workflow, and pull request requirements.

---

## 1. Core Principles & Project Constitution

All contributions must adhere to the [Walkies Constitution](.specify/memory/constitution.md):

1. **Evidence-Based Clinical & Scientific Foundation**:
   - Every new physiological, metabolic, or glycemic model (e.g., Time in Range, hypoglycemia risk, post-exercise recovery windows, active insulin decay, or aerobic decoupling) **must** be grounded in peer-reviewed clinical literature and international consensus guidelines (such as ATTD consensus statements, Riddell et al. 2017, Battelino et al. 2019, Zaharieva et al. 2020, Moser et al. 2020, Scott et al. 2021).
   - Cite relevant DOIs or PubMed IDs (`PMID`) in specifications (`research.md`) and code docstrings (`backend/analytics/models/`).
   - Never present heuristic signals as medical diagnoses; maintain clear non-diagnostic disclaimers in the UI.
2. **Canonical Units & Multi-Regimen Support**:
   - **Canonical Storage**: Backend persistence (`glycemic_events`, `walk_analytics`, `daily_analytics`, `user_settings`) always stores and computes blood glucose in **`mmol/L`** (`1 mmol/L = 18.0182 mg/dL`) and timestamps in **UTC ISO-8601**.
   - **Display Unit Agnosticism**: All frontend views, charts, tooltips, and settings inputs must respect the user's selected `glucose_unit` (`mmol/L` or `mg/dL`) via `frontend/src/utils/glucoseUnits.js`.
   - **Generalized Regimen Support**: Features must degrade gracefully across Closed-Loop AID Pump (`diabetic_closed_loop`), Standard Pump (`diabetic_pump`), Multiple Daily Injections (`diabetic_mdi`), Non-Insulin / Lifestyle (`diabetic_lifestyle`), Non-Diabetic CGM (`non_diabetic_cgm`), and General Fitness (`non_diabetic`) profiles.
3. **Strict User Data Isolation & Privacy**:
   - Every database query and filesystem path (`backend/data/{user_sub}/`) must be scoped by the authenticated `user_sub`.
   - Public share links (`share_links`) must remain read-only, revocable, and scoped strictly to the shared walk, date, or date range.
   - **Never commit real patient/user health files (`.fit`, `.csv`, `.gpx`), CareLink/Garmin/Dexcom tokens, or OAuth secrets to Git.**
4. **Deterministic Analytics & Cache Versioning**:
   - Analytics models must be deterministic functions of their input telemetry and user settings.
   - Whenever an analytics output schema or calculation changes, increment `ANALYTICS_VERSION` in `backend/analytics/db.py` so stale cached rows recompute automatically.

---

## 2. Development Setup

### Prerequisites
- **Python 3.11+** managed via [`uv`](https://docs.astral.sh/uv/)
- **Node.js (LTS)** & `npm`
- **Task** ([Taskfile.dev](https://taskfile.dev/), optional unified runner)

### Quick Start
```bash
# Install backend and frontend dependencies
uv sync --directory backend
npm --prefix frontend install

# Start backend (:8000) and frontend (:5173) for local development
task dev
```
For full setup details, environment variables, and Docker Compose instructions, see [DEVELOPMENT.md](DEVELOPMENT.md).

---

## 3. Workflow, Branching & Scope Discipline

1. **Branch-per-Feature**:
   - Always create a dedicated, short-lived branch from `main` for each feature, bugfix, or chore (e.g., `feat/dexcom-share-sync`, `fix/fit-timestamp-rollover`, `docs/update-api-ref`).
   - Do not commit unrelated changes directly to `main`.
2. **Strict Scope Boundaries (No Feature Creep)**:
   - Keep pull requests small, focused, and atomic.
   - Do not bundle opportunistic refactors or unrelated UI changes into a feature PR. If you identify a separate improvement, document it in `features.md` or open a separate issue/branch.
3. **Spec-Driven Development (Spec Kit)**:
   - For major features, specifications, literature reviews, data models, and task checklists live under `specs/<feature-branch>/` (`spec.md`, `research.md`, `plan.md`, `data-model.md`, `tasks.md`).

---

## 4. Testing & Quality Gates

Before opening a Pull Request, **all** of the following checks must pass locally:

### 1. Backend Test Suite (`pytest`)
All new endpoints, normalizers, and analytics calculations must include unit/contract tests in `backend/tests/`.
- **Mock External Services**: Never make live network calls to Garmin Connect, Medtronic CareLink, Dexcom Share, or Google OAuth during tests. Always mock external clients (see `test_garmin_sync.py`, `test_carelink_sync.py`, and `test_multi_provider_ingest.py`).
- Run the full suite from the repository root:
  ```bash
  uv run --directory backend pytest -v
  ```

### 2. Frontend Production Build (`vite build`)
Verify that all React components and JSX bundles compile cleanly without warnings or syntax errors:
```bash
npm --prefix frontend run build
```

---

## 5. Submitting a Pull Request

1. Ensure `uv run --directory backend pytest -v` and `npm --prefix frontend run build` succeed.
2. Update relevant documentation (`README.md`, `DEVELOPMENT.md`, `Analysis.md`, `AGENTS.md`, `CLAUDE.md`) if API endpoints, database tables, or clinical models were added or modified.
3. Open a Pull Request with a clear summary of:
   - The problem solved or feature added
   - Any clinical literature or consensus guidelines referenced
   - Verification steps and test coverage added
