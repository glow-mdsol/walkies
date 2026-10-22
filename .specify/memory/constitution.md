<!--
Sync Impact Report
- Version change: 1.2.0 → 1.3.0
- Modified principles:
  - V. Discrete Scope & Branch Discipline → V. Discrete Scope, Branch & Pull Request Discipline (NON-NEGOTIABLE)
- Modified sections:
  - Development Workflow & Quality Gates (added mandatory Pull Request Gate prohibiting direct commits or local fast-forward merges into `main` for feature updates)
- Added sections: None
- Removed sections: None
- Follow-up TODOs: None
-->
# Walkies Constitution

## Core Principles

### I. Clinical & Metabolic Accuracy
All physiological, glycemic, and pharmacokinetic calculations—across both discrete recorded walks
and generalized daily/epochal activity tracking (steps per epoch, daily activity type breakdowns,
and ambient movement)—MUST adhere to validated clinical parameters and user-configured treatment
profiles. Default glycemic thresholds (Target Range: 4.0–8.0 mmol/L, Hypoglycemia: <= 3.9 mmol/L,
Hyperglycemia: >= 12.9 mmol/L) and insulin action models (configurable DIA and peak timing across
rapid and short-acting analogs) MUST be preserved. Missing sensor data, calibration gaps, or pump
suspend states (`PLGM_PREDICTED_LOW_SG`, `NORMAL_PUMPING`, manual suspend) MUST be explicitly
handled without silent interpolation that could distort rate-of-change, daily glycemic load, or
hypoglycemia risk indicators.

*Rationale*: Walkies correlates both structured exercise and generalized daily physical activity
with Type 1 and generalized diabetes management; inaccurate metabolic modeling or silent data
corruption compromises the reliability of retrospective clinical insights.

### II. Data Privacy, Security & Tenant Isolation
User medical data (CGM traces, pump events), biometric and daily wellness telemetry (Garmin FIT/GPX
files, epochal steps, activity summaries), and cloud authentication tokens MUST be strictly isolated
per user (`{DATA_DIR}/{user_sub}/...` and tenant-scoped database records). Sensitive credentials,
OAuth secrets, session tokens, and Personally Identifiable Information (PII) MUST NEVER be committed
to version control or exposed in logs. Public sharing links MUST enforce scoped, read-only access
to the designated analysis and MUST be revocable by the owner at any time without leaking directory
structures or adjacent user records.

*Rationale*: Health and biometric records are highly sensitive personal data requiring strict
access control and zero cross-tenant leakage.

### III. Deterministic & Versioned Analytics
All analytics—including per-walk metrics (distance alignment, heart rate decoupling, glucose rate
of change, Time In Range, SmartGuard state detection, Active Insulin-on-Board curves) and
generalized aggregate metrics (steps walked per epoch, daily activity type breakdowns correlated
with glycemic factors)—MUST be computed deterministically from source telemetry and user settings.
Cached analytics stored in SQLite/PostgreSQL MUST be governed by `ANALYTICS_VERSION` and source
data hashes; any schema change, model refinement, or underlying data modification MUST
automatically invalidate stale caches and trigger recomputation while serving valid `ETag` and
`Cache-Control` headers.

*Rationale*: Reproducible analytics ensure consistent chart rendering across per-activity and
daily aggregate views while avoiding redundant re-parsing of raw telemetry extracts.

### IV. Test-Verified Quality & External Mocking (NON-NEGOTIABLE)
Every backend feature, parser update, provider sync adapter, or analytics model change MUST be
accompanied by automated `pytest` unit or integration tests executed via `uv run pytest -v`.
External upstream services (Garmin Connect, Medtronic CareLink, Dexcom, Tandem, Google OAuth, and
external literature APIs) MUST ALWAYS be mocked in automated test suites. Frontend modifications
MUST pass production asset compilation (`npm --prefix frontend run build`) without syntax or
bundling errors prior to merge.

*Rationale*: Automated verification and strict mocking prevent regressions in binary/CSV/API
parsers and guarantee fast, offline-capable, deterministic CI/CD pipelines.

### V. Discrete Scope, Branch & Pull Request Discipline (NON-NEGOTIABLE)
All development MUST occur on dedicated, short-lived feature or fix branches (`feat/...`,
`fix/...`, `refactor/...`, or numbered Spec Kit feature branches). Feature updates, bug fixes, and
refactors MUST ALWAYS be submitted and integrated into `main` via a Pull Request (PR) rather than
committing directly to `main` or merging locally without a PR. Pull requests and implementation
tasks MUST maintain strict scope boundaries and avoid feature creep or unrequested opportunistic
refactoring. Unrelated enhancements discovered during implementation MUST be recorded in
`features.md` rather than bundled into the active branch.

*Rationale*: Enforcing Pull Requests for all feature updates guarantees reviewable diffs, CI gate
verification, and an auditable history before changes reach `main`.

### VI. Direct Provider Integration & Multi-Modal Telemetry (USP)
Walkies' core Unique Selling Proposition (USP) is frictionless, direct integration with wearable
activity platforms (Garmin Connect) and continuous glucose monitor (CGM) / insulin delivery
providers (Medtronic CareLink, Dexcom, Tandem, and generalized diabetic data sources). Provider
integrations MUST normalize heterogeneous upstream telemetry—spanning high-frequency GPS/FIT workout
streams, epochal step counts, daily activity classifications, CGM glucose traces, and insulin
delivery logs—into unified, provider-agnostic internal domain models so that analytics operate
consistently regardless of the user's specific device ecosystem or diabetes therapy regimen (pump
or MDI).

*Rationale*: Eliminating manual file exports through direct cloud-to-cloud synchronization and
normalizing multi-vendor telemetry enables holistic, day-to-day glycemic and activity correlation
for a broad diabetic user base.

### VII. Evidence-Based Design & Scientific Literature Review (NON-NEGOTIABLE)
Every physiological, glycemic, pharmacokinetic, or exertion analysis introduced or modified in
Walkies MUST be grounded in a thorough, documented review of peer-reviewed scientific and clinical
literature (utilizing repository research digests in `sources/` / `backend/sources_review/` and
current academic databases such as PubMed, Europe PMC, and OpenAlex). Before implementing any new
analytical model or correlation metric, Phase 0 research (`research.md`) MUST cite the current
clinical consensus, physiological mechanisms, and empirical thresholds supporting the formulation,
and explicitly document any heuristic approximations or non-diagnostic caveats presented to the
user.

*Rationale*: Speculative or unverified math in diabetes and exercise physiology can mislead users;
anchoring all analyses in peer-reviewed evidence ensures Walkies reflects current clinical science.

## Clinical & Technical Constraints

- **Backend Stack**: Python managed exclusively via `uv`, FastAPI, and SQLite/PostgreSQL persistence
  (`backend/analytics/db.py`).
- **Frontend Stack**: React 19, Vite, Leaflet for interactive route mapping, and Recharts for
  multi-panel physiological and aggregate daily visualizations.
- **Telemetry & Parsing Standards**: Garmin `.fit` files and wellness epochs MUST preserve accurate
  timestamp alignment (including 5-bit compressed timestamp rollover for binary FIT streams); CGM
  and pump extracts (CareLink `.csv`, Dexcom, Tandem, etc.) MUST normalize glucose readings to
  `mmol/L` for canonical backend storage (while supporting user-configurable `mmol/L` or `mg/dL`
  display units) and preserve event metadata across closed-loop pump, standard pump, MDI, and
  non-insulin regimens.
- **Literature & Evidence Tooling**: Research reviews MUST leverage `backend/sources_review` and
  indexed clinical literature (ATTD International Consensus on Time in Range, EASD/ISPAD/ADA
  exercise management guidelines) to validate metrics and thresholds.
- **Cost & Portability**: Dependencies MUST favor open-source libraries and containerized deployment
  (Docker / Google Cloud Run) with minimal infrastructure overhead.
- **Observability**: Application events, provider sync statuses, and parsing errors MUST be
  captured via `WalkiesDBLogHandler` (`system_logs` table) to provide transparent diagnostics in
  the UI.

## Development Workflow & Quality Gates

1. **Literature Review Gate**: Before designing or coding new physiological or glycemic models,
   conduct and document a literature review in `research.md` (with citations/DOIs/PMIDs) verifying
   alignment with current clinical consensus.
2. **Branching**: Create a dedicated feature or fix branch from `main` before starting any
   implementation work; never commit feature work directly to `main`.
3. **Local Verification**: Use `WALKIES_DEV_BYPASS_AUTH=1` for local development testing when OAuth
   is not required.
4. **Test Gate**: Run `uv run pytest -v` from `backend/` and ensure 100% of the test suite passes.
5. **Build Gate**: Run `npm --prefix frontend run build` to verify frontend compilation when UI
   components or styles are touched.
6. **Documentation Alignment**: Keep `AGENTS.md`, `CLAUDE.md`, `README.md`, `CONTRIBUTING.md`, and
   `Analysis.md` synchronized when introducing new API endpoints, provider integrations, database
   tables, or clinical metrics.
7. **Pull Request Gate**: Push the feature branch and open a Pull Request (PR) targeting `main` for
   all feature updates, bug fixes, and refactors; do not bypass the PR workflow with direct local
   merges into `main`.

## Governance

This Constitution supersedes ad-hoc practices across the Walkies repository. All specifications,
implementation plans, task breakdowns, and code reviews MUST verify compliance with the principles
defined herein.

- **Amendment Procedure**: Any modification to these principles or constraints requires updating
  `.specify/memory/constitution.md`, documenting the rationale in the Sync Impact Report, and
  verifying alignment with `AGENTS.md` and `CLAUDE.md`.
- **Versioning Policy**: This document follows Semantic Versioning (`MAJOR.MINOR.PATCH`):
  - **MAJOR**: Backward-incompatible governance changes or removal/redefinition of Core Principles.
  - **MINOR**: Addition of new principles, sections, or materially expanded clinical/technical
    guidance.
  - **PATCH**: Wording clarifications, formatting refinements, or non-semantic fixes.
- **Compliance Review**: Every implementation plan generated via `/speckit-plan` MUST evaluate its
  Constitution Check gates against these seven Core Principles before Phase 0 research and after
  Phase 1 design. Runtime development guidance is maintained in `AGENTS.md` and `CLAUDE.md`.

**Version**: 1.3.0 | **Ratified**: 2026-10-22 | **Last Amended**: 2026-10-22
