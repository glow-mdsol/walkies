# Feature Specification: Generalized Diabetic Support, Epochal Garmin Ingestion, and Multi-Provider CGM Correlation

**Feature Branch**: `001-generalized-diabetic-telemetry`

**Created**: 2026-10-22

**Status**: Draft

**Input**: User description: "Define the feature specification for generalized diabetic support, epochal step/activity ingestion from Garmin, and multi-provider CGM correlation."

## Clarifications

### Session 2026-10-22

- Q: Glucose measurement unit preference in Settings → A: Users can choose between `mmol/L` (default) and `mg/dL` in Settings; all charts, summary cards, rate-of-change metrics, and clinical profile inputs (target range, hypo/hyper thresholds, and Insulin Sensitivity Factor) dynamically display and accept input in the user's selected unit while canonical storage remains `mmol/L`.
- Q: Which CGM and insulin providers should support direct cloud account connection in this release versus CSV/file import only? (FR-002) → A: Direct cloud sync for Garmin Connect, Medtronic CareLink, and Dexcom; CSV/file import supported for Medtronic CareLink, Dexcom, and Tandem.
- Q: How should a user's historical daily data be populated when they sync Garmin and their CGM provider? (FR-001) → A: Default to syncing the last 7 days on standard sync, with a user-selectable date range picker (up to 30 days per batch) for historical backfill.
- Q: Should public sharing links also be supported for daily and multi-day aggregate views, or remain restricted to individual walks? (FR-006) → A: Support revocable read-only share links for daily and date-range aggregate views in addition to individual walks.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Daily Epochal Activity & Glycemic Correlation View (Priority: P1)

As a person with diabetes using a Garmin wearable and a Continuous Glucose Monitor (CGM), I want to view my full-day and multi-day physical movement (steps per time epoch and daily breakdowns of activity types) overlaid and correlated against my continuous glucose readings and glycemic indicators so that I can understand how everyday movement—not just dedicated GPS hikes—affects my blood glucose stability.

**Why this priority**: This delivers the foundational value proposition of expanding Walkies from a single-activity hike analyzer into a generalized daily diabetic lifestyle and movement correlation platform.

**Independent Test**: Can be independently tested by syncing or loading a 24-hour period of epochal step/activity telemetry alongside 24-hour CGM readings and verifying that the daily timeline displays aligned step counts per epoch, activity type breakdowns, Time In Range (TIR), and post-movement glycemic response metrics.

**Acceptance Scenarios**:

1. **Given** an authenticated user with synced Garmin daily wellness data and CGM data for a selected date, **When** the user opens the Daily Activity & Glycemic Overview, **Then** the system displays a time-aligned chart pairing epochal step counts (e.g., 15-minute intervals) and categorized activity windows against continuous glucose readings (`mmol/L`) and target range bands (`4.0–8.0 mmol/L`).
2. **Given** a selected day or date range with multiple activity types (e.g., walking, running, cycling, sedentary, ambient movement), **When** the user views the daily breakdown summary, **Then** the system displays total duration, step volume, and corresponding glycemic metrics (average glucose change, Time In Range, and hypo/hyper excursions) attributable to each activity category and post-activity recovery window.
3. **Given** a time window where CGM readings are absent for more than 15 minutes (sensor warmup or signal loss), **When** the daily correlation view is rendered, **Then** the system visually marks the sensor gap as a "no data" band and excludes the gap from glucose rate-of-change calculations while still displaying Garmin epochal step counts.

---

### User Story 2 - Direct Multi-Provider CGM & Insulin Telemetry Ingestion (Priority: P2)

As a person with diabetes using any supported CGM or insulin delivery ecosystem (such as Medtronic CareLink, Dexcom, Tandem, or CGM-only without a pump), I want to connect my provider account directly or import provider data so that my glucose traces, carbohydrate logs, and insulin deliveries are automatically normalized and available for correlation without manual formatting.

**Why this priority**: Direct integration with Garmin and diverse CGM/pump providers is the core Unique Selling Proposition (USP) that opens the platform to the wider diabetic community beyond MiniMed 780G users.

**Independent Test**: Can be tested independently by configuring a non-Medtronic CGM source (e.g., Dexcom or CGM-only profile), triggering a sync or file import, and verifying that glucose readings are normalized to `mmol/L` and rendered seamlessly in both daily epoch views and individual walk views without requiring pump-specific fields.

**Acceptance Scenarios**:

1. **Given** a user configuring their data sources, **When** they connect or import data from a supported CGM/pump provider (Medtronic CareLink, Dexcom, or Tandem), **Then** the system ingests and normalizes all glucose timestamps and values into canonical `mmol/L` records while preserving provider-specific events (bolus, basal, auto-suspend, or carb entries) where available.
2. **Given** a user who uses a CGM without an automated insulin pump (e.g., Multiple Daily Injections / MDI or lifestyle/oral management), **When** they view daily or per-walk analyses, **Then** the system cleanly renders glucose and activity correlations without failing on missing basal or closed-loop pump states, and allows optional manual logging of insulin doses or carbohydrate intake.
3. **Given** overlapping timestamps from repeated sync operations for the same user and date, **When** new telemetry is ingested from Garmin or a CGM provider, **Then** the system deduplicates records deterministically and refreshes cached analytics automatically.

---

### User Story 3 - Generalized Diabetic Regimen & Profile Configuration (Priority: P3)

As a person with Type 1, Type 2, LADA, or gestational diabetes, I want to configure my specific therapy regimen (Closed-Loop Pump, Open-Loop Pump, Multiple Daily Injections, or No Exogenous Insulin), target glucose ranges, and insulin kinetics so that risk warnings, Time In Range metrics, and active insulin curves match my personal clinical treatment plan.

**Why this priority**: Generalized diabetic support requires accommodating users who do not use automated insulin pumps or who have customized clinical targets, ensuring insights are clinically relevant to each individual.

**Independent Test**: Can be tested independently by switching a user's profile between "Closed-Loop Pump", "MDI (Pens/Syringes)", and "CGM-Only (No Insulin)" and verifying that the dashboard adapts its metrics, hides inapplicable pump automation cards, and recalculates Time In Range using the user's configured thresholds.

**Acceptance Scenarios**:

1. **Given** a user in Settings, **When** they select their diabetes classification, therapy regimen (Closed-Loop Pump, Standard Pump, MDI, or Non-Insulin/Lifestyle), preferred glucose unit (`mmol/L` or `mg/dL`), and save their target glycemic thresholds, **Then** all daily and activity analyses recalculate and display Time In Range, hypoglycemia risk, hyperglycemia exposure, and glucose axes in the chosen unit according to those settings.
2. **Given** a user on an MDI or non-insulin regimen, **When** they view any analysis screen, **Then** pump-specific SmartGuard/auto-suspend indicators are omitted or replaced with regimen-appropriate indicators (such as active bolus decay from logged pen injections or pure glycemic-exertion response).

---

### Edge Cases

- **Timezone and DST Transitions**: How does the system align Garmin epochal timestamps (often stored in UTC alongside local offset metadata) with CGM provider timestamps (often exported in local wall-clock time) when a user travels across timezones or crosses a Daylight Saving Time boundary? The system MUST normalize all telemetry to UTC with preserved local timezone offsets so epochs and CGM readings never shift out of alignment.
- **Unrecorded vs. Recorded Workouts**: What happens when a user completes a recorded GPS walk (FIT file) during a day that also has Garmin 15-minute epochal step summaries? The system MUST prevent double-counting steps/exertion in daily totals while linking the high-resolution GPS activity directly into the corresponding time window on the daily timeline.
- **Partial Provider Outages or Rate Limits**: How does the system handle a sync when Garmin Connect succeeds but the CGM provider sync fails (or vice versa)? The system MUST persist and display the successfully synced stream, clearly report the partial sync status and timestamp to the user, and allow independent retry of the failed provider.
- **Extreme Sensor Noise or Compression Lows**: Rapid, transient single-point drops followed by immediate recovery (common overnight) MUST be distinguishable from sustained exertion-induced hypoglycemia when computing daily glycemic load and activity correlation.
- **Missing Insulin/Carb Data on CGM-Only Extracts**: When a provider feed (such as a basic CGM stream) contains only glucose readings and no insulin or carb records, all glucose-to-step and glucose-to-activity correlations MUST still compute and display accurately without errors.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST ingest daily epochal wellness telemetry from Garmin Connect—including step counts per time epoch (e.g., 15-minute intervals), intensity minutes, resting/ambient heart rate summaries, and categorized daily activity segments (walking, running, cycling, sedentary, sleep/rest)—defaulting to a 7-day lookback on standard sync and supporting a user-selectable date range (up to 30 days per batch) for historical backfill.
- **FR-002**: System MUST support multi-provider CGM and insulin data ingestion via direct cloud synchronization for Garmin Connect, Medtronic CareLink, and Dexcom, as well as CSV/file import for Medtronic CareLink, Dexcom, and Tandem, normalizing all incoming glucose measurements to canonical `mmol/L` (with user-selectable display in `mmol/L` or `mg/dL`) on a unified timeline.
- **FR-003**: System MUST correlate epochal step counts and daily activity type breakdowns with concurrent and lagging glycemic metrics (glucose rate of change during and 1–2 hours post-activity, Time In Range `4.0–8.0 mmol/L` by default, Time Below Range `<= 3.9 mmol/L`, and Time Above Range `>= 12.9 mmol/L`).
- **FR-004**: Users MUST be able to configure a generalized diabetic profile in Settings specifying their preferred glucose unit (`mmol/L` or `mg/dL`), therapy mode (Automated Closed-Loop Pump, Standard Pump, Multiple Daily Injections / MDI, or Non-Insulin / Lifestyle), personalized target glucose thresholds, and optional basal/bolus kinetic parameters, with all charts, statistics, and profile input fields dynamically converting and rendering in the selected unit.
- **FR-005**: System MUST dynamically adapt analysis views based on the user's therapy mode, displaying pump automation states (e.g., auto-suspend / predicted low suspend) for closed-loop users while presenting clean glycemic-movement correlation views for MDI and non-insulin users.
- **FR-006**: System MUST provide a Daily & Multi-Day Aggregate Dashboard where users can inspect a 24-hour epochal timeline (steps, activity categories, CGM trace, and insulin/carb events) as well as multi-day trend summaries comparing glycemic stability on active days versus sedentary days, and generate revocable read-only public share links for selected daily or date-range summaries.
- **FR-007**: System MUST link discrete recorded GPS activities (walks/hikes) into the daily epochal timeline without duplicating daily step totals or exertion metrics.
- **FR-008**: System MUST detect and visually annotate CGM sensor gaps exceeding 15 minutes during both daily epoch views and discrete activity views, excluding missing intervals from rate-of-change and average glucose calculations.
- **FR-009**: System MUST deduplicate overlapping telemetry records across repeated direct cloud syncs or manual file uploads and automatically invalidate and recompute cached daily and per-walk analytics when underlying data or user clinical settings change.
- **FR-010**: System MUST isolate all synced provider credentials, tokens, and health telemetry per user account, never expose credentials in logs or shared views, and allow users to revoke provider connections or delete their synced data at any time.

### Key Entities *(include if feature involves data)*

- **Diabetic Profile**: Represents a user's clinical configuration, including preferred glucose display unit (`mmol/L` or `mg/dL`), diabetes classification, therapy regimen (Closed-Loop Pump, Open-Loop Pump, MDI, Non-Insulin), target glucose range (stored canonically in `mmol/L`), hypo/hyper thresholds, insulin sensitivity factor, carb ratio, and active insulin kinetic profile.
- **Telemetry Provider Connection**: Represents a user's authenticated link or import configuration for an external wearable or glycemic platform (Garmin Connect, Medtronic CareLink, Dexcom, Tandem), tracking sync status, last sync timestamp, and isolated session credentials.
- **Activity Epoch**: Represents a standardized time slice of daily wearable wellness data (e.g., 15-minute window) containing start/end timestamps, step count, dominant activity category, distance, active/total calories, and mean/max heart rate.
- **Daily Activity Summary**: Represents the 24-hour aggregation of movement for a user on a given date, breaking down total steps, duration, and exertion by activity type and linking to any high-resolution recorded workouts on that date.
- **Normalized Glycemic & Therapy Event**: Represents a provider-agnostic time-series record of continuous glucose readings (`mmol/L`), basal delivery rates or states, bolus doses, and carbohydrate intakes aligned to UTC and user local time.
- **Epoch-Glycemic Correlation Summary**: Represents the computed deterministic analytics for a day or date range, capturing Time In Range, glycemic variability, glucose drop rates per 1,000 steps or per activity category, and post-activity hypo/hyper incidences.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Users can connect their Garmin and CGM accounts (or upload provider extracts) and view a complete, time-aligned 24-hour epochal step and glucose correlation timeline in under 2 minutes from initiating sync.
- **SC-002**: 100% of supported CGM provider feeds (Medtronic CareLink and Dexcom, with or without insulin pump records) normalize into the unified daily and per-walk analysis views without missing-field errors for non-pump (MDI or lifestyle) users.
- **SC-003**: Daily step totals and activity category durations match the upstream wearable provider's daily summary within 1% tolerance, with zero double-counting on days containing recorded GPS walks.
- **SC-004**: Users can identify how different daily activity types and step intensities affect their glucose (via per-category glycemic change and Time In Range breakdowns) across single-day and 7/14/30-day aggregate views in a single dashboard view.
- **SC-005**: Subsequent loads of previously synced daily or multi-day correlation views render in under 1 second when no underlying telemetry or clinical profile settings have changed.

## Assumptions

- **Epoch Granularity**: Garmin Connect provides intraday wellness epochs (typically 15-minute intervals for steps and activity classification) alongside daily summaries; when intraday epochs are unavailable for a legacy date, the system gracefully falls back to daily summary totals and recorded workout windows.
- **CGM Sampling Frequency**: CGM providers typically supply readings every 5 minutes; alignment with 15-minute activity epochs aggregates or pairs the corresponding 5-minute CGM readings within and immediately following each epoch window.
- **Canonical Glucose Unit**: Internal calculations and storage standardize on `mmol/L` (consistent with existing Walkies clinical rules), converting `mg/dL` inputs (`mg/dL / 18.0182`) upon ingestion while allowing display unit preferences.
- **Direct Cloud Sync & File Fallback**: Direct cloud integration is provided for Garmin Connect, Medtronic CareLink, and Dexcom, supplemented by CSV/file upload support for Medtronic CareLink, Dexcom, and Tandem so users can import historical extracts across all supported platforms.
