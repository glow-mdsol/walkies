# Phase 0 Research & Scientific Literature Review: Generalized Diabetic Support, Epochal Garmin Ingestion, and Multi-Provider CGM Correlation

**Feature**: `001-generalized-diabetic-telemetry`  
**Date**: 2026-10-22  
**Constitution Gate**: Principle VII (Evidence-Based Design & Scientific Literature Review)

---

## 0. Scientific Literature Review & Clinical Evidence Base

In compliance with **Constitution Principle VII**, the analytical models, time windows, and glycemic metrics for generalized daily/epochal activity correlation are grounded in the repository's curated evidence library ([references.md](file:///C:/Users/grlow.LAPTOP-FRGT971S/Documents/Projects/walkies/references.md), `sources/DIGEST.md`, `sources/requirements.md`) and supplementary open-access literature retrieved via Europe PMC.

### 0.1 Free-Living Physical Activity vs. Structured Exercise in Diabetes
- **Evidence**:
  - **Campbell et al. (2017)** (*Lancet Diabetes Endocrinol*, [doi:10.1016/S2213-8587(17)30169-9](https://doi.org/10.1016/S2213-8587(17)30169-9), PMID: `28533137`) & **Riddell et al. (2017) Authors' Reply** ([doi:10.1016/S2213-8587(17)30168-7](https://doi.org/10.1016/S2213-8587(17)30168-7), PMID: `28533138`): Emphasize that structured sport/exercise guidelines translate poorly to unstructured daily living and ambient physical activity (e.g., housework, dog walking, errands), from which the majority of people with diabetes derive daily glycemic benefit and risk exposure.
  - **DiPietro et al. (2013)** (*Diabetes Care*, [doi:10.2337/dc13-0084](https://doi.org/10.2337/dc13-0084), PMID: `23761134`): Demonstrated that three 15-minute bouts of moderate post-meal walking significantly improve 24-hour glycemic control and blunt postprandial excursions—directly validating **15-minute epoch granularity** (`FR-001`, `FR-003`) as a physiologically meaningful unit of analysis.
  - **Loh et al. (2020)** (*Sports Med*, [doi:10.1007/s40279-019-01183-w](https://doi.org/10.1007/s40279-019-01183-w), PMID: `31552570`, [PMC6985064](https://europepmc.org/articles/PMC6985064)): Meta-analysis of 37 trials showing that interrupting prolonged sitting (`sedentary` epochs) with short physical activity breaks significantly attenuates glucose (`SMD -0.54`, `p < 0.00001`), with an advantage for frequent activity breaks over a single continuous bout when energy-matched.
  - **Karstoft et al. (2013)** (*Diabetes Care*, [doi:10.2337/dc12-0658](https://doi.org/10.2337/dc12-0658), PMID: `23002086`, [PMC3554285](https://europepmc.org/articles/PMC3554285)): Randomized controlled trial demonstrating that free-living interval-walking (alternating low and high intensity) tracked via accelerometry and heart-rate monitoring reduces mean and maximum 24-hour CGM glucose levels in Type 2 diabetes more effectively than continuous moderate walking of equal energy expenditure.
- **Analytical Design Implication**:
  - Daily correlation (`daily_correlation.py`) MUST evaluate both **total step volume** and **epochal pattern/intensity** (sedentary duration vs. active 15-minute bouts, and continuous vs. intermittent intensity across T1D and T2D profiles).

### 0.2 Standardized 24-Hour & Multi-Day CGM Consensus Metrics
- **Evidence**:
  - **Battelino et al. (2019) — ATTD International Consensus on Time in Range** (*Diabetes Care*, [doi:10.2337/dci19-0028](https://doi.org/10.2337/dci19-0028), PMID: `31177185`): Establishes standard CGM metrics: Time in Range (TIR, `3.9–10.0 mmol/L` general consensus; Walkies default `4.0–8.0 mmol/L` tight/exercise target, user-configurable in Settings), Time Below Range (TBR Level 1 `<= 3.9 mmol/L` [70 mg/dL], Level 2 `< 3.0 mmol/L` [54 mg/dL]), Time Above Range (TAR), Coefficient of Variation (`%CV = SD / mean * 100`, target `<= 36%`), and sensor data completeness thresholds (`>= 70%` sensor coverage over 14 days).
  - **Moser et al. (2020) — EASD/ISPAD/ADA Position Statement on CGM in Exercise** (*Diabetologia*, [doi:10.1007/s00125-020-05263-9](https://doi.org/10.1007/s00125-020-05263-9), PMID: `33047169`): Defines CGM rate-of-change arrow semantics, physiological interstitial lag (`5–15 min` during rapid exertion-induced glucose shifts), and regimen-specific glycemic action thresholds across pump and MDI users.
- **Analytical Design Implication**:
  - `daily_analytics` computes `%TIR`, `%TBR`, `%TAR`, `avg_bg_mmol`, `%CV` (`bg_cv_pct`), and `sensor_coverage_pct`.
  - To differentiate true clinical hypoglycemic episodes from single-point sensor noise or compression lows (`Edge Cases` in `spec.md`), a hypoglycemic event requires **sustained readings `<= hypo_threshold_mmol` for `>= 15 minutes`** (at least 3 consecutive 5-minute CGM readings), matching ATTD consensus definition of a CGM hypoglycemic event.

### 0.3 Exercise Type, Intensity & Post-Activity Lag Windows (1–2h Early vs. 6–12h Late/Nocturnal)
- **Evidence**:
  - **Riddell et al. (2017)** (*Lancet Diabetes Endocrinol*, [doi:10.1016/S2213-8587(17)30014-1](https://doi.org/10.1016/S2213-8587(17)30014-1), PMID: `28126459`) & **Tonoli et al. (2012)** (*Sports Med*, [doi:10.1007/BF03262312](https://doi.org/10.1007/BF03262312), PMID: `23134339`): Continuous aerobic movement (`walking`, `running`, `cycling`) increases insulin-independent GLUT4 translocation and peripheral glucose uptake, driving an acute drop in glucose, whereas brief anaerobic/high-intensity activity can transiently elevate glucose via catecholamine counter-regulation.
  - **Parent et al. (2024)** (*Diabetes Res Clin Pract*, [doi:10.1016/j.diabres.2024.111631](https://doi.org/10.1016/j.diabres.2024.111631), PMID: `38513989`): Shows intermittent high-intensity exercise (IHE) attenuates exercise-induced glucose drop by `1.47 mmol/L` compared to continuous moderate exercise (CME).
  - **Scott et al. (2021)** (*Lancet Diabetes Endocrinol*, [doi:10.1016/S2213-8587(21)00054-1](https://doi.org/10.1016/S2213-8587(21)00054-1), PMID: `33864810`), **Zaharieva et al. (2020)** (*Can J Diabetes*, [doi:10.1016/j.jcjd.2020.06.003](https://doi.org/10.1016/j.jcjd.2020.06.003), PMID: `33011134`), & **Hovorka et al. (2014)** (*Diabetes Care*, [doi:10.2337/dc13-2644](https://doi.org/10.2337/dc13-2644), PMID: `24757227`, [PMC3994941](https://europepmc.org/articles/PMC3994941)): Distinguish the **early post-activity recovery window (`0–2 hours`)**, where rapid glycogen resynthesis and elevated insulin sensitivity trigger acute drops (`< 8.0 mmol/L` carb feeding trigger), from the **late/nocturnal recovery window (`6–12 hours` / `23:00–07:00`)**, where biphasic delayed hypoglycemia occurs.
- **Analytical Design Implication**:
  - In `activity_category_breakdown`, each active epoch cluster is evaluated alongside both **concurrent glucose change** and **1-hour / 2-hour lagging post-activity glycemic change** (`mean_bg_change_1h_post_mmol` and `hypo_events_during_or_2h_post`), plus nocturnal (`00:00–06:00`) vs. daytime TIR comparison on active vs. sedentary days.

---

## 1. Garmin Connect Daily Wellness & Intraday Epoch Ingestion

- **Decision**: Use the existing `garminconnect` (`>=0.3.11`) Python library's daily summary and intraday step/wellness methods (`client.get_steps_data(cdate)`, `client.get_stats(cdate)`, `client.get_heart_rates(cdate)`, and `client.get_activities_by_date(start_date, end_date)`) to ingest 15-minute intraday step epochs, daily activity category breakdowns, and recorded workouts over a default 7-day window or user-selected date range (up to 30 days per batch).
- **Rationale**:
  - `garminconnect` is already installed in `backend/pyproject.toml` and authenticated via persistent Garth session tokens in `{DATA_DIR}/{user_sub}/.garminconnect`.
  - `get_steps_data(YYYY-MM-DD)` returns Garmin's 15-minute epoch array (`startGMT`, `endGMT`, `steps`, `primaryActivityLevel`, `activityLevelConstant`), which directly provides both step count per 15-minute epoch and activity state classification (`sedentary`, `walking`, `running`, `highlyActive`, `sleeping`).
  - `get_stats(YYYY-MM-DD)` provides daily totals (`totalSteps`, `moderateIntensityMinutes`, `vigorousIntensityMinutes`, `restingHeartRate`, `activeKilocalories`, and distance/duration breakdowns) to cross-validate epoch sums within the 1% tolerance required by `SC-003`.
- **Alternatives Considered**:
  - *Garmin Health Enterprise API (Webhook push)*: Requires commercial partner approval and public webhook infrastructure; rejected in favor of `garminconnect` pull sync to keep costs zero and support local/self-hosted deployments.
  - *Parsing only recorded `.fit` workout files*: Misses ambient/lifestyle steps and unrecorded daily movement, which is the core focus of `FR-001`.

---

## 2. Direct Multi-Provider CGM & Insulin Ingestion (CareLink, Dexcom, Tandem)

- **Decision**: Implement a unified `TelemetryProviderAdapter` interface in `backend/telemetry/` with concrete adapters for:
  1. **Medtronic CareLink**: Direct cloud sync via existing `carelink_client2.py` (`getRecentData()` / historical connect payloads) + CSV parser supporting full 24-hour/multi-day ingestion (instead of discarding readings outside recorded FIT walk bounds).
  2. **Dexcom**: Direct cloud sync via Dexcom Share REST API (`httpx` client against `share2.dexcom.com` / `shareous1.dexcom.com` with session token caching in `{DATA_DIR}/{user_sub}/.dexcom`) + Dexcom Clarity CSV import parser (`EGV` rows, `mg/dL` or `mmol/L` auto-detection, carb/insulin event rows).
  3. **Tandem**: CSV import parser for Tandem t:connect exports (CGM `EGV` readings, basal delivery changes, bolus events, and Control-IQ automated suspend/basal states).
- **Rationale**:
  - Currently, `sync_carelink_data` in `backend/main.py` only writes CGM readings that fall within `[walk_start - 30m, walk_end + 60m]` of an already-imported Garmin walk. Persisting normalized 24-hour CGM and insulin streams into a tenant-scoped database table (`glycemic_events`) decouples daily glycemic tracking from recorded GPS walks while also automatically feeding per-walk overlays.
  - Implementing the Dexcom Share client directly with `httpx` (already in `pyproject.toml`) avoids fragile third-party wrapper dependencies, supports US and OUS (international) endpoints, and is trivially mockable in `pytest`.
- **Alternatives Considered**:
  - *Nightscout-only bridge*: Forces users to host and maintain a separate Nightscout instance; rejected because direct Garmin + CGM provider integration is Walkies' core USP (Constitution Principle VI).
  - *Dexcom Official Developer OAuth API*: Restricted to registered commercial partners with delayed sandbox/production approval; Dexcom Share API + Clarity CSV import gives immediate direct cloud access for individual users.

---

## 3. Canonical `mmol/L` Storage & Dynamic `mmol/L` / `mg/dL` Unit Conversion

- **Decision**: Store all glucose values, rate-of-change slopes, and user clinical thresholds canonically in `mmol/L` in the backend database and analytics cache, while persisting `glucose_unit` (`'mmol/L'` default or `'mg/dL'`), `target_low_mmol` (`4.0`), `target_high_mmol` (`8.0`), `hypo_threshold_mmol` (`3.9`), and `hyper_threshold_mmol` (`12.9`) in `user_settings`. Provide a shared conversion utility (`1 mmol/L = 18.0182 mg/dL`) in both backend ingestion and a frontend formatting hook/module (`frontend/src/utils/glucoseUnits.js`).
- **Rationale**:
  - Preserves 100% backward compatibility with existing cached `walk_analytics` and clinical models (`backend/analytics/models/`) as mandated by Constitution Principle I.
  - Performing display conversion at the UI boundary (and input conversion when saving `UserSettings`) allows instant toggling between `mmol/L` and `mg/dL` without invalidating or recomputing the underlying `mmol/L` analytics cache.
  - Ingestion parsers automatically detect source units (e.g., Dexcom Share and CareLink API return `mg/dL` integers; CareLink UK/EU CSVs use `mmol/L` floats; US CSVs use `mg/dL`) and convert to canonical `mmol/L` (`round(mgdl / 18.0182, 2)`).
- **Alternatives Considered**:
  - *Storing values in user's preferred unit in DB*: Would corrupt data or require full database rewrites whenever a user toggles their unit setting, and would break cross-module clinical formulas.

---

## 4. Workout vs. Epoch Deduplication & Timezone Alignment

- **Decision**: Normalize all epoch and CGM timestamps to UTC (`ISO-8601` with `Z`) alongside a `local_date` (`YYYY-MM-DD`) and `tz_offset_mins` derived from the provider payload. When linking a recorded GPS `.fit` walk into a day's 15-minute Garmin epochs:
  - Daily total steps use Garmin's authoritative daily summary (`get_stats`) or the sum of 15-minute intraday epochs (which already include steps taken during recorded walks on the watch) — never adding `.fit` distance/steps on top of intraday epochs.
  - Overlapping 15-minute epochs during a recorded `.fit` window are tagged with `linked_walk_id` so the UI highlights the recorded walk on the 24-hour timeline and allows one-click drill-down without double-counting steps or active minutes.
- **Rationale**:
  - Garmin wearables record steps into the 15-minute wellness epoch stream simultaneously while recording a GPS activity. Summing `.fit` steps + wellness epochs would double-count workout steps, violating `SC-003`.
  - Storing both `timestamp_utc` and `local_date` resolves DST and travel timezone shifts (`Edge Cases` in `spec.md`).
- **Alternatives Considered**:
  - *Subtracting FIT duration from daily epochs*: Error-prone when a walk starts/ends mid-epoch (e.g., 10:07–11:22); tagging the overlapping epochs with `linked_walk_id` preserves exact 15-minute step histograms while linking the high-resolution workout.

---

## 5. Deterministic Caching & Share Links for Daily/Range Summaries

- **Decision**:
  - Add a `daily_analytics` table keyed by `(user_sub, local_date)` storing `source_hash` (computed from the day's epoch rows + glycemic event rows + user clinical profile hash + `ANALYTICS_VERSION`), `computed_at`, summary columns (`total_steps`, `tir_pct`, `tbr_pct`, `tar_pct`, `avg_bg_mmol`, `bg_cv_pct`, `active_minutes`), and `payload_json` (the full 24-hour aligned epoch/CGM series, sensor gap bands, and per-activity-type glycemic breakdown).
  - Extend `share_links` with `share_type` (`'walk'` default, `'daily'`, `'range'`), `target_start_date`, and `target_end_date` (keeping `walk_id` populated with a synthetic key for daily/range shares) so revocable read-only share tokens work seamlessly across individual walks, single-day timelines, and 7/14/30-day aggregate views (`FR-006`).
- **Rationale**:
  - Mirrors the proven `walk_analytics` deterministic caching pattern (Constitution Principle III), guaranteeing `<1s` subsequent loads (`SC-005`) and supporting HTTP `ETag` validation.
  - Extending `share_links` reuses existing token generation, expiration, revocation, and ownership checks (`Constitution Principle II`).
- **Alternatives Considered**:
  - *Computing 7/14/30-day correlations on the fly from raw CSV files*: Too slow and lacks unified multi-day storage when CareLink/Dexcom data is synced via API rather than attached to a single walk folder.
