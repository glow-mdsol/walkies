---

description: "Task list for Generalized Diabetic Support, Epochal Garmin Ingestion, and Multi-Provider CGM Correlation"
---

# Tasks: Generalized Diabetic Support, Epochal Garmin Ingestion, and Multi-Provider CGM Correlation

**Input**: Design documents from `/specs/001-generalized-diabetic-telemetry/`

**Prerequisites**: [plan.md](file:///C:/Users/grlow.LAPTOP-FRGT971S/Documents/Projects/walkies/specs/001-generalized-diabetic-telemetry/plan.md) (required), [spec.md](file:///C:/Users/grlow.LAPTOP-FRGT971S/Documents/Projects/walkies/specs/001-generalized-diabetic-telemetry/spec.md) (required for user stories), [research.md](file:///C:/Users/grlow.LAPTOP-FRGT971S/Documents/Projects/walkies/specs/001-generalized-diabetic-telemetry/research.md), [data-model.md](file:///C:/Users/grlow.LAPTOP-FRGT971S/Documents/Projects/walkies/specs/001-generalized-diabetic-telemetry/data-model.md), [contracts/api-contracts.md](file:///C:/Users/grlow.LAPTOP-FRGT971S/Documents/Projects/walkies/specs/001-generalized-diabetic-telemetry/contracts/api-contracts.md)

**Tests**: Automated `pytest` tasks with mocked external providers are included per Constitution Principle IV (`Test-Verified Quality & External Mocking (NON-NEGOTIABLE)`).

**Organization**: Tasks are grouped by user story (`US1`, `US2`, `US3`) to enable independent implementation, verification, and incremental delivery.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (`[US1]`, `[US2]`, `[US3]`)
- Exact file paths and verbatim data-model constraints are included in task descriptions

## Path Conventions

- **Web app**: `backend/` (FastAPI + `analytics/` + `telemetry/` + `tests/`), `frontend/src/` (React + `components/` + `utils/`)

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Initialize modular package structure and shared glucose unit conversion utilities

- [X] T001 Create telemetry package directory and exports in `backend/telemetry/__init__.py`
- [X] T002 [P] Create canonical glucose unit conversion and formatting utility (`1 mmol/L = 18.0182 mg/dL`, `mmol/L` default, `mg/dL` rounding, rate-of-change and ISF helpers) in `frontend/src/utils/glucoseUnits.js`
- [X] T003 [P] Create backend glucose unit and timestamp normalization helpers (`mg/dL` to canonical `mmol/L` via `round(mgdl / 18.0182, 2)`, UTC ISO-8601 + `local_date` + `tz_offset_mins`) in `backend/telemetry/common.py`

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Database schema migrations, persistence CRUD helpers, and analytics version bump that block all user stories

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [X] T004 Bump `ANALYTICS_VERSION` to `"2026-10-22-v3"` and add idempotent table creation and `ALTER TABLE` migrations in `backend/analytics/db.py` for:
  - `user_settings`: `user_type` (`'diabetic_closed_loop'`, `'diabetic_pump'`, `'diabetic_mdi'`, `'diabetic_lifestyle'`, `'non_diabetic_cgm'`, `'non_diabetic'`), `insulin_type` (`'ClosedLoopPump'`, `'Pump'`, `'MDI'`, `'None'`), `glucose_unit` (`TEXT NOT NULL DEFAULT 'mmol/L'`, enum `'mmol/L' | 'mg/dL'`), `target_low_mmol` (`REAL NOT NULL DEFAULT 4.0`, `2.5 <= val < target_high_mmol`), `target_high_mmol` (`REAL NOT NULL DEFAULT 8.0`, `target_low_mmol < val <= 15.0`), `hypo_threshold_mmol` (`REAL NOT NULL DEFAULT 3.9`, `<= target_low_mmol`), `hyper_threshold_mmol` (`REAL NOT NULL DEFAULT 12.9`, `>= target_high_mmol`), `cgm_provider` (`'carelink' | 'dexcom' | 'tandem' | 'none'`), `dexcom_username` (`TEXT NULL`), `dexcom_region` (`TEXT DEFAULT 'ous'`, `'us' | 'ous'`), `last_garmin_sync_at` (`TEXT NULL`), `last_cgm_sync_at` (`TEXT NULL`)
  - `activity_epochs`: `PRIMARY KEY (user_sub, epoch_start_utc)`, `epoch_end_utc` (`TEXT NOT NULL`), `local_date` (`TEXT NOT NULL`), `tz_offset_mins` (`INTEGER NOT NULL DEFAULT 0`), `steps` (`INTEGER NOT NULL DEFAULT 0`, `>= 0`), `activity_category` (`TEXT NOT NULL DEFAULT 'sedentary'`, enum `'sedentary' | 'walking' | 'running' | 'cycling' | 'highly_active' | 'sleeping' | 'other'`), `intensity_level` (`'sedentary' | 'light' | 'moderate' | 'vigorous'`), `distance_m`, `active_kcal`, `mean_hr`, `max_hr`, `linked_walk_id` (`TEXT NULL`), `source_provider` (`TEXT NOT NULL DEFAULT 'garmin'`), `ingested_at` (`TEXT NOT NULL`), plus index `idx_activity_epochs_user_date`
  - `daily_wellness_summaries`: `PRIMARY KEY (user_sub, local_date)`, `total_steps` (`INTEGER NOT NULL DEFAULT 0`), `total_distance_m`, `moderate_intensity_mins`, `vigorous_intensity_mins`, `resting_hr`, `min_hr`, `max_hr`, `active_kcal`, `raw_summary_json`, `updated_at` (`TEXT NOT NULL`)
  - `glycemic_events`: `PRIMARY KEY (user_sub, timestamp_utc, event_type)`, `event_type` (`'cgm' | 'bolus' | 'basal' | 'carb' | 'pump_state'`), `local_date` (`TEXT NOT NULL`), `tz_offset_mins` (`INTEGER NOT NULL DEFAULT 0`), `bg_mmol` (`REAL NULL`, `1.0 <= bg_mmol <= 35.0` for `'cgm'`), `trend_arrow`, `bolus_units` (`> 0` for `'bolus'`), `basal_rate_u_hr` (`>= 0` for `'basal'`), `carb_grams` (`> 0` for `'carb'`), `pump_state` (`'NORMAL_PUMPING' | 'PLGM_PREDICTED_LOW_SG' | 'AUTO_SUSPEND' | 'MANUAL_SUSPEND' | 'CONTROL_IQ_EXERCISE' | 'MANUAL_MODE'`), `source_provider` (`'carelink' | 'dexcom' | 'tandem' | 'manual'`), `ingested_at` (`TEXT NOT NULL`), plus index `idx_glycemic_events_user_date`
  - `daily_analytics`: `PRIMARY KEY (user_sub, local_date)`, `source_hash` (`TEXT NOT NULL`), `analytics_version` (`TEXT NOT NULL`), `computed_at` (`TEXT NOT NULL`), `total_steps`, `active_minutes`, `tir_pct` (`0.0–100.0`), `tbr_pct` (`0.0–100.0`), `tar_pct` (`0.0–100.0`), `avg_bg_mmol`, `bg_cv_pct`, `hypos_count` (`INTEGER NOT NULL DEFAULT 0`), `bolus_total_units`, `carb_total_grams`, `sensor_coverage_pct`, `payload_json` (`TEXT NOT NULL`)
  - `share_links`: `share_type` (`TEXT NOT NULL DEFAULT 'walk'`, enum `'walk' | 'daily' | 'range'`), `target_start_date` (`TEXT NULL`), `target_end_date` (`TEXT NULL`)
- [X] T005 Implement database CRUD and batch upsert functions (`upsert_activity_epochs`, `get_activity_epochs_for_date`, `upsert_daily_wellness_summary`, `get_daily_wellness_summary`, `upsert_glycemic_events`, `get_glycemic_events_for_window`, `upsert_daily_analytics`, `get_cached_daily_analytics`, `list_daily_analytics_range`, and updated `delete_user_data`) in `backend/analytics/db.py`
- [X] T006 Export new database and daily analytics helpers in `backend/analytics/__init__.py`

**Checkpoint**: Database schema and persistence layer ready — user story phases can now proceed

---

## Phase 3: User Story 1 - Daily Epochal Activity & Glycemic Correlation View (Priority: P1) 🎯 MVP

**Goal**: Ingest Garmin 15-minute intraday epochs and daily wellness summaries, correlate them with 24-hour and multi-day CGM/insulin streams (including `>15m` sensor gap detection, workout deduplication via `linked_walk_id`, and 1–2h post-activity lag metrics), and expose the Daily & Multi-Day Aggregate Dashboard with public share link support.

**Independent Test**: Sync or seed 24 hours of Garmin 15-minute epochs alongside 24-hour CGM readings and verify `/api/daily/{date}/analysis` and `/api/daily/range` return aligned 15-minute step counts, activity category breakdowns, Time In Range, sensor gap annotations (`>15 min`), zero step double-counting on days with recorded walks, and working daily/range share links.

### Tests for User Story 1

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [X] T007 [P] [US1] Write unit and contract tests for 24-hour epoch-glycemic correlation, `>15m` sensor gap exclusion, ATTD sustained hypo episode counting (`>=15m` below threshold), 1–2h post-activity lag changes, workout step deduplication (`linked_walk_id`), and `/api/daily/*` endpoints in `backend/tests/test_daily_correlation.py`

### Implementation for User Story 1

- [X] T008 [P] [US1] Implement Garmin 15-minute intraday epoch and daily summary fetcher/normalizer (`get_steps_data`, `get_stats`, `get_heart_rates`, 7-day default lookback, up to 30-day date range, and `linked_walk_id` tagging against recorded walks) in `backend/telemetry/garmin_epochs.py`
- [X] T009 [P] [US1] Implement deterministic 24-hour and multi-day epoch-to-glycemic correlation model (15-min epoch alignment, `>15m` sensor gap detection, `%TIR`/`%TBR`/`%TAR`/`%CV`, sustained `>=15m` hypo episode detection, per-activity-category duration/steps/1h-post-BG-change breakdown, and active vs. sedentary day comparison) in `backend/analytics/models/daily_correlation.py`
- [X] T010 [US1] Implement `daily_analytics` source hashing, cache validation, recomputation orchestration, and `ETag` payload generation in `backend/analytics/service.py` (depends on T005, T009)
- [X] T011 [US1] Extend `POST /api/garmin/sync` to accept optional `start_date`, `end_date` (default 7 days, max 30 days), `include_epochs`, and `include_walks`, persisting `activity_epochs` and `daily_wellness_summaries` and refreshing `daily_analytics` in `backend/main.py`
- [X] T012 [US1] Implement `GET /api/daily/{date}/analysis`, `GET /api/daily/range`, `POST /api/daily/share-links`, `GET /api/daily/share-links`, and extend `GET /api/shared/{token}/analysis` for `'daily'` and `'range'` share types in `backend/main.py`
- [X] T013 [P] [US1] Create `DailyAggregateView.jsx` component (24-hour 15-minute step & activity category chart overlaid with CGM trace and IOB, grey `>15m` sensor gap bands, linked walk badges, activity category glycemic table, 7/14/30-day trend view, and Share Summary modal) in `frontend/src/components/DailyAggregateView.jsx`
- [X] T014 [US1] Update `frontend/src/App.jsx` and `frontend/src/components/WalkList.jsx` to add top-level navigation to the Daily & Aggregate Dashboard, support `?share=<token>` routing for daily/range shares, and add a 7-day / custom 30-day date range selector for Garmin sync

**Checkpoint**: User Story 1 is independently functional and testable as the MVP

---

## Phase 4: User Story 2 - Direct Multi-Provider CGM & Insulin Telemetry Ingestion (Priority: P2)

**Goal**: Enable direct cloud synchronization for Medtronic CareLink and Dexcom Share (7-day default / up to 30-day backfill) and unified CSV file import for Medtronic CareLink, Dexcom Clarity, and Tandem t:connect into `glycemic_events`, automatically refreshing both `daily_analytics` and overlapping `walk_analytics` for pump and CGM-only users.

**Independent Test**: Connect or mock Dexcom Share and CareLink cloud syncs and upload CareLink, Dexcom Clarity (`mg/dL` and `mmol/L`), and Tandem CSV extracts via `POST /api/telemetry/import`; verify deduplicated `mmol/L` records in `glycemic_events` and clean rendering for CGM-only (non-pump) profiles.

### Tests for User Story 2

- [X] T015 [P] [US2] Write mocked unit and endpoint tests for Dexcom Share login/sync (`us` and `ous` regions), CareLink 24-hour `glycemic_events` sync, multi-provider CSV auto-detection and parsing (CareLink, Dexcom Clarity, Tandem t:connect), `mg/dL` to `mmol/L` conversion, and idempotent timestamp deduplication in `backend/tests/test_multi_provider_ingest.py`

### Implementation for User Story 2

- [X] T016 [P] [US2] Implement CareLink 24-hour and multi-day normalizer (extracting full-day `sgs`, `markers`, basal rates, boluses, carbs, and SmartGuard states from both CareLink API payloads and CareLink CSV files into `glycemic_events`) in `backend/telemetry/carelink_normalizer.py`
- [X] T017 [P] [US2] Implement Dexcom Share direct cloud client (`httpx` client supporting `'us'` and `'ous'` endpoints, session token persistence in `{DATA_DIR}/{user_sub}/.dexcom`) and Dexcom Clarity CSV parser (`EGV`, carb, and insulin records with `mg/dL`/`mmol/L` auto-detection) in `backend/telemetry/dexcom_client.py`
- [X] T018 [P] [US2] Implement Tandem t:connect CSV parser (extracting CGM `EGV` readings, basal rate changes, boluses, carbs, and Control-IQ exercise/suspend states into `glycemic_events`) in `backend/telemetry/tandem_parser.py`
- [X] T019 [US2] Implement `POST /api/dexcom/login`, `POST /api/dexcom/sync`, `POST /api/telemetry/import` (auto-detecting CareLink, Dexcom, and Tandem CSVs), and update `POST /api/carelink/sync` in `backend/main.py` to populate `glycemic_events`, refresh `daily_analytics`, and update overlapping `walk_analytics`
- [X] T020 [P] [US2] Create `DexcomSyncModal.jsx` and multi-provider CSV import UI in `frontend/src/components/DexcomSyncModal.jsx` and update `frontend/src/components/UploadForm.jsx` to allow importing standalone CGM/pump CSVs into daily and walk timelines

**Checkpoint**: User Stories 1 and 2 work independently and together across Garmin, CareLink, Dexcom, and Tandem

---

## Phase 5: User Story 3 - Generalized Diabetic Regimen & Profile Configuration (Priority: P3)

**Goal**: Allow users to configure their diabetes therapy regimen (`Closed-Loop Pump`, `Standard Pump`, `Multiple Daily Injections / MDI`, or `Non-Insulin / Lifestyle`), custom glycemic thresholds, and preferred glucose display unit (`mmol/L` or `mg/dL`) in Settings, dynamically adapting all charts, inputs, and analysis cards.

**Independent Test**: Toggle `glucose_unit` between `'mmol/L'` and `'mg/dL'` and switch `user_type` between `'diabetic_closed_loop'`, `'diabetic_mdi'`, and `'diabetic_lifestyle'` in Settings; verify that all threshold inputs, walk charts, daily charts, and stat cards dynamically convert units and hide inapplicable closed-loop pump cards for MDI/non-insulin users.

### Tests for User Story 3

- [X] T021 [P] [US3] Write unit and contract tests for generalized regimen settings validation, canonical `mmol/L` storage with `mg/dL` input normalization, threshold-triggered `daily_analytics` cache invalidation, and regimen-specific payload adaptation in `backend/tests/test_glucose_units_settings.py`

### Implementation for User Story 3

- [X] T022 [US3] Update `get_user_settings`, `upsert_user_settings`, and `GET/POST /api/user/settings` (and `/api/settings`) in `backend/analytics/db.py` and `backend/main.py` to validate and persist `glucose_unit` (`'mmol/L' | 'mg/dL'`), `user_type` (`'diabetic_closed_loop' | 'diabetic_pump' | 'diabetic_mdi' | 'diabetic_lifestyle' | 'non_diabetic_cgm' | 'non_diabetic'`), custom `target_low_mmol`, `target_high_mmol`, `hypo_threshold_mmol`, `hyper_threshold_mmol`, `cgm_provider`, and `dexcom_username`/`dexcom_region`
- [X] T023 [P] [US3] Update `frontend/src/components/UserSettings.jsx` to add the `mmol/L` vs `mg/dL` unit selector (dynamically converting Target Range, Hypo/Hyper thresholds, Correction Threshold, and ISF inputs), generalized therapy regimen selector (`Closed-Loop Pump`, `Standard Pump`, `MDI`, `Non-Insulin / Lifestyle`), CGM provider selector, and Dexcom connection controls
- [X] T024 [US3] Update `frontend/src/components/WalkAnalysisView.jsx`, `frontend/src/components/WalkList.jsx`, and `frontend/src/components/DailyAggregateView.jsx` to format all glucose axes, tooltips, deltas, and slopes using `frontend/src/utils/glucoseUnits.js` and conditionally hide SmartGuard / closed-loop automation panels when the user's regimen is `MDI` or `Non-Insulin / Lifestyle`

**Checkpoint**: All three user stories are complete and independently testable

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Documentation synchronization, full test suite verification, and production build validation

- [X] T025 [P] Update `AGENTS.md`, `CLAUDE.md`, and `Analysis.md` to document the new `/api/daily/*`, `/api/dexcom/*`, and `/api/telemetry/import` routes, `backend/telemetry/` architecture, and generalized diabetic regimen / unit support
- [X] T026 Run full backend test suite (`uv run --directory backend pytest -v`) and frontend production build (`npm --prefix frontend run build`) per `specs/001-generalized-diabetic-telemetry/quickstart.md`

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies — can start immediately (`T001`–`T003`).
- **Foundational (Phase 2)**: Depends on Setup (`T001`–`T003`) — **BLOCKS** all user stories (`T004`–`T006`).
- **User Story 1 (Phase 3 - P1 MVP)**: Depends on Foundational (`T004`–`T006`).
- **User Story 2 (Phase 4 - P2)**: Depends on Foundational (`T004`–`T006`); integrates cleanly with `daily_analytics` from US1.
- **User Story 3 (Phase 5 - P3)**: Depends on Setup (`T002`, `T003`) and Foundational (`T004`–`T006`); adapts UI components from US1 and existing walk views.
- **Polish (Phase 6)**: Depends on completion of Phases 3–5 (`T025`–`T026`).

### User Story Dependencies

- **User Story 1 (P1)**: Independent after Phase 2 Foundational.
- **User Story 2 (P2)**: Independent at the backend ingestion/model layer after Phase 2; enriches US1 daily views with multi-provider cloud and CSV streams.
- **User Story 3 (P3)**: Independent at the settings/unit layer after Phase 2; enhances US1 and walk views with dynamic `mmol/L` ↔ `mg/dL` rendering and regimen-aware cards.

### Parallel Opportunities

- **Phase 1**: `T002` (`frontend/src/utils/glucoseUnits.js`) and `T003` (`backend/telemetry/common.py`) can run in parallel.
- **Phase 3 (US1)**: `T007` (tests), `T008` (`garmin_epochs.py`), `T009` (`daily_correlation.py`), and `T013` (`DailyAggregateView.jsx`) operate on distinct files and can be developed in parallel before wiring endpoints in `T011`/`T012`.
- **Phase 4 (US2)**: `T015` (tests), `T016` (`carelink_normalizer.py`), `T017` (`dexcom_client.py`), `T018` (`tandem_parser.py`), and `T020` (`DexcomSyncModal.jsx`) can all run in parallel.
- **Phase 5 (US3)**: `T021` (tests) and `T023` (`UserSettings.jsx`) can run in parallel with `T022`.

---

## Parallel Example: User Story 1 & User Story 2

```bash
# Parallel stream for User Story 1 core modules:
Task T008: "Implement Garmin 15-minute intraday epoch fetcher in backend/telemetry/garmin_epochs.py"
Task T009: "Implement 24-hour epoch-glycemic correlation model in backend/analytics/models/daily_correlation.py"
Task T013: "Create DailyAggregateView.jsx in frontend/src/components/DailyAggregateView.jsx"

# Parallel stream for User Story 2 provider adapters:
Task T016: "Implement CareLink 24-hour normalizer in backend/telemetry/carelink_normalizer.py"
Task T017: "Implement Dexcom Share client and Clarity CSV parser in backend/telemetry/dexcom_client.py"
Task T018: "Implement Tandem t:connect CSV parser in backend/telemetry/tandem_parser.py"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete **Phase 1: Setup** (`T001`–`T003`)
2. Complete **Phase 2: Foundational** (`T004`–`T006`)
3. Complete **Phase 3: User Story 1** (`T007`–`T014`)
4. **STOP and VALIDATE**: Run `uv run --directory backend pytest tests/test_daily_correlation.py -v` and verify the Daily & Aggregate Dashboard with Garmin 15-minute epochs and CGM correlation.

### Incremental Delivery

1. **Foundation** (`T001`–`T006`): Schema, CRUD, and unit helpers ready.
2. **MVP — US1** (`T007`–`T014`): Daily 15-minute Garmin epochs + 24h/multi-day glycemic correlation dashboard + daily/range share links.
3. **Multi-Provider — US2** (`T015`–`T020`): Direct Dexcom Share sync, 24h CareLink sync, and unified CareLink/Dexcom/Tandem CSV import.
4. **Generalized Regimens & Units — US3** (`T021`–`T024`): Dynamic `mmol/L` ↔ `mg/dL` switching and MDI / Non-Insulin / Closed-Loop profile adaptation.
5. **Polish & Verification** (`T025`–`T026`): Documentation sync, full `pytest` suite, and Vite production build gate.
