# Phase 1 Data Model: Generalized Diabetic Support, Epochal Garmin Ingestion, and Multi-Provider CGM Correlation

**Feature**: `001-generalized-diabetic-telemetry`  
**Date**: 2026-10-22  
**Storage Engine**: SQLite (`backend/analytics.sqlite3`) and PostgreSQL (`ANALYTICS_DB_URL`) via `backend/analytics/db.py` adapter.

---

## 1. Entity Definitions & Schema Extensions

### 1.1 `user_settings` (Extended Diabetic Profile & Provider Config)

Extends the existing `user_settings` table via idempotent `ALTER TABLE` migrations in `init_analytics_db()`.

| Field | Type | Nullable | Default | Description & Validation Rules |
|-------|------|----------|---------|--------------------------------|
| `user_sub` | `TEXT` | PK | — | Authenticated Google OAuth user subject |
| `user_type` | `TEXT` | No | `'diabetic_pump'` | `'diabetic_closed_loop'`, `'diabetic_pump'`, `'diabetic_mdi'`, `'diabetic_lifestyle'`, `'non_diabetic_cgm'`, `'non_diabetic'` |
| `insulin_type` | `TEXT` | No | `'None'` | `'ClosedLoopPump'`, `'Pump'`, `'MDI'`, `'None'` |
| `glucose_unit` | `TEXT` | No | `'mmol/L'` | Display & input unit preference: `'mmol/L'` or `'mg/dL'` |
| `target_low_mmol` | `REAL` | No | `4.0` | Lower bound of Target Range in canonical `mmol/L` (`2.5 <= val < target_high_mmol`) |
| `target_high_mmol` | `REAL` | No | `8.0` | Upper bound of Target Range in canonical `mmol/L` (`target_low_mmol < val <= 15.0`) |
| `hypo_threshold_mmol` | `REAL` | No | `3.9` | Hypoglycemia threshold in canonical `mmol/L` (`<= target_low_mmol`) |
| `hyper_threshold_mmol` | `REAL` | No | `12.9` | Hyperglycemia threshold in canonical `mmol/L` (`>= target_high_mmol`) |
| `pump_brand_model` | `TEXT` | Yes | `NULL` | e.g., `'MiniMed 780G'`, `'Tandem t:slim X2'`, `'Omnipod 5'` |
| `cgm_provider` | `TEXT` | Yes | `'carelink'` | Active CGM source preference: `'carelink'`, `'dexcom'`, `'tandem'`, `'none'` |
| `basal_rates_json` | `TEXT` | Yes | `NULL` | JSON object of basal rates (`U/hr` or daily long-acting dose for MDI) |
| `carb_ratios_json` | `TEXT` | Yes | `NULL` | JSON object of insulin-to-carb ratios (`g/U`) |
| `insulin_sensitivity_factor` | `REAL` | Yes | `2.0` | Canonical ISF in `mmol/L per U` (converted to/from `mg/dL per U` in UI when `glucose_unit == 'mg/dL'`) |
| `correction_threshold` | `REAL` | Yes | `8.0` | Canonical correction target in `mmol/L` |
| `exercise_correction_factor` | `REAL` | Yes | `1.5` | Exercise sensitivity multiplier |
| `iob_calculation_method` | `TEXT` | Yes | `'exponential'` | `'exponential'` or `'linear'` |
| `insulin_profile_json` | `TEXT` | Yes | `NULL` | JSON pharmacokinetic parameters (`name`, `onsetMins`, `peakMinHours`, `durationMaxHours`, etc.) |
| `garmin_email` | `TEXT` | Yes | `NULL` | Connected Garmin Connect account identifier |
| `carelink_email` | `TEXT` | Yes | `NULL` | Connected Medtronic CareLink account identifier |
| `dexcom_username` | `TEXT` | Yes | `NULL` | Connected Dexcom Share account username |
| `dexcom_region` | `TEXT` | Yes | `'ous'` | Dexcom Share region (`'us'` or `'ous'`) |
| `last_garmin_sync_at` | `TEXT` | Yes | `NULL` | ISO-8601 UTC timestamp of last successful Garmin sync |
| `last_cgm_sync_at` | `TEXT` | Yes | `NULL` | ISO-8601 UTC timestamp of last successful CGM cloud sync |
| `updated_at` | `TEXT` | No | — | ISO-8601 UTC timestamp of last profile update |

---

### 1.2 `activity_epochs` (Garmin Intraday Movement Epochs)

Stores normalized 15-minute (or provider-native) intraday movement slices per user.

| Field | Type | Nullable | Default | Description & Validation Rules |
|-------|------|----------|---------|--------------------------------|
| `user_sub` | `TEXT` | PK (1) | — | Owner user subject |
| `epoch_start_utc` | `TEXT` | PK (2) | — | ISO-8601 UTC start timestamp (e.g., `'2026-10-21T08:00:00Z'`) |
| `epoch_end_utc` | `TEXT` | No | — | ISO-8601 UTC end timestamp (e.g., `'2026-10-21T08:15:00Z'`) |
| `local_date` | `TEXT` | No | — | User local calendar date (`YYYY-MM-DD`) for fast daily grouping |
| `tz_offset_mins` | `INTEGER` | No | `0` | Offset in minutes from UTC at `epoch_start_utc` |
| `steps` | `INTEGER` | No | `0` | Step count recorded during this epoch (`>= 0`) |
| `activity_category` | `TEXT` | No | `'sedentary'` | Normalized category: `'sedentary'`, `'walking'`, `'running'`, `'cycling'`, `'highly_active'`, `'sleeping'`, `'other'` |
| `intensity_level` | `TEXT` | Yes | `'sedentary'` | Raw or mapped intensity level (`'sedentary'`, `'light'`, `'moderate'`, `'vigorous'`) |
| `distance_m` | `REAL` | Yes | `0.0` | Estimated distance in meters during epoch |
| `active_kcal` | `REAL` | Yes | `0.0` | Active kilocalories burned in epoch |
| `mean_hr` | `REAL` | Yes | `NULL` | Average heart rate (bpm) during epoch if available |
| `max_hr` | `REAL` | Yes | `NULL` | Max heart rate (bpm) during epoch if available |
| `linked_walk_id` | `TEXT` | Yes | `NULL` | UUID of recorded walk in `walk_analytics` overlapping this epoch (prevents double-counting) |
| `source_provider` | `TEXT` | No | `'garmin'` | Source wearable provider (`'garmin'`) |
| `ingested_at` | `TEXT` | No | — | ISO-8601 UTC ingestion timestamp |

**Indexes**:
- `PRIMARY KEY (user_sub, epoch_start_utc)`
- `idx_activity_epochs_user_date ON activity_epochs(user_sub, local_date, epoch_start_utc)`

---

### 1.3 `daily_wellness_summaries` (Authoritative Daily Wearable Totals)

Stores daily summary statistics from Garmin (`get_stats`) to cross-check epoch totals and capture daily resting HR and intensity minutes.

| Field | Type | Nullable | Default | Description & Validation Rules |
|-------|------|----------|---------|--------------------------------|
| `user_sub` | `TEXT` | PK (1) | — | Owner user subject |
| `local_date` | `TEXT` | PK (2) | — | Calendar date (`YYYY-MM-DD`) |
| `total_steps` | `INTEGER` | No | `0` | Authoritative daily step count |
| `total_distance_m` | `REAL` | Yes | `0.0` | Total daily distance in meters |
| `moderate_intensity_mins` | `INTEGER` | Yes | `0` | Daily moderate intensity minutes |
| `vigorous_intensity_mins` | `INTEGER` | Yes | `0` | Daily vigorous intensity minutes |
| `resting_hr` | `REAL` | Yes | `NULL` | Daily resting heart rate (bpm) |
| `min_hr` | `REAL` | Yes | `NULL` | Daily minimum heart rate (bpm) |
| `max_hr` | `REAL` | Yes | `NULL` | Daily maximum heart rate (bpm) |
| `active_kcal` | `REAL` | Yes | `0.0` | Total active kilocalories |
| `raw_summary_json` | `TEXT` | Yes | `NULL` | Compact JSON of additional provider breakdown fields |
| `updated_at` | `TEXT` | No | — | ISO-8601 UTC update timestamp |

---

### 1.4 `glycemic_events` (Normalized Multi-Provider CGM & Therapy Stream)

Stores deduplicated, provider-agnostic continuous glucose readings, insulin deliveries, pump states, and carbohydrate entries across 24-hour/multi-day timelines.

| Field | Type | Nullable | Default | Description & Validation Rules |
|-------|------|----------|---------|--------------------------------|
| `user_sub` | `TEXT` | PK (1) | — | Owner user subject |
| `timestamp_utc` | `TEXT` | PK (2) | — | ISO-8601 UTC event timestamp |
| `event_type` | `TEXT` | PK (3) | — | `'cgm'`, `'bolus'`, `'basal'`, `'carb'`, `'pump_state'` |
| `local_date` | `TEXT` | No | — | User local calendar date (`YYYY-MM-DD`) |
| `tz_offset_mins` | `INTEGER` | No | `0` | Offset in minutes from UTC |
| `bg_mmol` | `REAL` | Yes | `NULL` | Canonical sensor glucose in `mmol/L` (`1.0 <= bg_mmol <= 35.0` for `'cgm'` events) |
| `trend_arrow` | `TEXT` | Yes | `NULL` | Normalized trend rate/arrow (`'FLAT'`, `'SINGLE_UP'`, `'SINGLE_DOWN'`, `'DOUBLE_DOWN'`, etc.) |
| `bolus_units` | `REAL` | Yes | `NULL` | Delivered rapid/short insulin units (`> 0` for `'bolus'` events) |
| `basal_rate_u_hr` | `REAL` | Yes | `NULL` | Active basal delivery rate in `U/hr` (`>= 0` for `'basal'` events) |
| `carb_grams` | `REAL` | Yes | `NULL` | Carbohydrates consumed in grams (`> 0` for `'carb'` events) |
| `pump_state` | `TEXT` | Yes | `NULL` | Normalized state: `'NORMAL_PUMPING'`, `'PLGM_PREDICTED_LOW_SG'`, `'AUTO_SUSPEND'`, `'MANUAL_SUSPEND'`, `'CONTROL_IQ_EXERCISE'`, `'MANUAL_MODE'` |
| `source_provider` | `TEXT` | No | — | `'carelink'`, `'dexcom'`, `'tandem'`, `'manual'` |
| `ingested_at` | `TEXT` | No | — | ISO-8601 UTC ingestion timestamp |

**Indexes**:
- `PRIMARY KEY (user_sub, timestamp_utc, event_type)`
- `idx_glycemic_events_user_date ON glycemic_events(user_sub, local_date, timestamp_utc)`

---

### 1.5 `daily_analytics` (Deterministic Cached Daily & Epoch-Glycemic Correlation)

Caches computed 24-hour correlation metrics and chart series per user and date.

| Field | Type | Nullable | Default | Description & Validation Rules |
|-------|------|----------|---------|--------------------------------|
| `user_sub` | `TEXT` | PK (1) | — | Owner user subject |
| `local_date` | `TEXT` | PK (2) | — | Calendar date (`YYYY-MM-DD`) |
| `source_hash` | `TEXT` | No | — | SHA-256 digest of day's epochs + glycemic events + user clinical thresholds + `ANALYTICS_VERSION` |
| `analytics_version` | `TEXT` | No | — | Matches `ANALYTICS_VERSION` |
| `computed_at` | `TEXT` | No | — | ISO-8601 UTC computation timestamp |
| `total_steps` | `INTEGER` | No | `0` | Deduplicated total daily steps |
| `active_minutes` | `INTEGER` | No | `0` | Total non-sedentary / active minutes |
| `tir_pct` | `REAL` | Yes | `NULL` | Percentage of CGM readings within `[target_low_mmol, target_high_mmol]` (`0.0–100.0`) |
| `tbr_pct` | `REAL` | Yes | `NULL` | Percentage of CGM readings `<= hypo_threshold_mmol` (`0.0–100.0`) |
| `tar_pct` | `REAL` | Yes | `NULL` | Percentage of CGM readings `>= hyper_threshold_mmol` (`0.0–100.0`) |
| `avg_bg_mmol` | `REAL` | Yes | `NULL` | Daily mean sensor glucose in `mmol/L` (excluding sensor gaps `>15m`) |
| `bg_cv_pct` | `REAL` | Yes | `NULL` | Glycemic coefficient of variation (`SD / mean * 100`) |
| `hypos_count` | `INTEGER` | No | `0` | Count of distinct hypoglycemic episodes (`>= 15m` below threshold, distinguished from single-point compression noise) |
| `bolus_total_units` | `REAL` | Yes | `0.0` | Total daily bolus insulin delivered (`U`) |
| `carb_total_grams` | `REAL` | Yes | `0.0` | Total daily carbohydrates logged (`g`) |
| `sensor_coverage_pct` | `REAL` | Yes | `0.0` | Percentage of the 24-hour day covered by valid CGM readings (without `>15m` gaps) |
| `payload_json` | `TEXT` | No | — | Full serialized payload: 15-min aligned epoch + BG + IOB series, sensor gap intervals, linked walks, and per-activity-category glycemic breakdown |

---

### 1.6 `share_links` (Extended for Daily & Date-Range Shares)

Extends the existing `share_links` table via idempotent `ALTER TABLE` statements so share tokens support individual walks, single-day views, and multi-day aggregate ranges.

| Field | Type | Nullable | Default | Description & Validation Rules |
|-------|------|----------|---------|--------------------------------|
| `token` | `TEXT` | PK | — | Cryptographically random URL-safe token |
| `owner_sub` | `TEXT` | No | — | Owner user subject |
| `walk_id` | `TEXT` | No | `''` | Walk UUID for `'walk'` shares; synthetic key (`'daily:YYYY-MM-DD'` or `'range:YYYY-MM-DD:YYYY-MM-DD'`) for daily/range shares to preserve NOT NULL compatibility |
| `share_type` | `TEXT` | No | `'walk'` | `'walk'`, `'daily'`, or `'range'` |
| `target_start_date` | `TEXT` | Yes | `NULL` | Start date (`YYYY-MM-DD`) for `'daily'` or `'range'` shares |
| `target_end_date` | `TEXT` | Yes | `NULL` | End date (`YYYY-MM-DD`) for `'daily'` or `'range'` shares |
| `created_at` | `TEXT` | No | — | ISO-8601 UTC creation timestamp |
| `expires_at` | `TEXT` | Yes | `NULL` | Optional ISO-8601 UTC expiration timestamp |
| `revoked_at` | `TEXT` | Yes | `NULL` | ISO-8601 UTC revocation timestamp |

---

## 2. Entity Relationships & Data Flow

```text
[Garmin Connect API] ──► activity_epochs (15-min slices) ──┐
                     ──► daily_wellness_summaries          ├──► Daily & Range Analytics Engine ──► daily_analytics (cached)
                     ──► walk_analytics (recorded .fit) ───┤                                              │
                                                           │                                              ▼
[CareLink / Dexcom / Tandem] ──► glycemic_events (mmol/L) ─┘                                    share_links ('daily' | 'range' | 'walk')
                                        ▲
[user_settings (Regimen, Units, Targets)]┘
```

## 3. State Transitions & Cache Invalidation Rules

1. **Telemetry Ingestion (`sync` or `import`)**:
   - New epochs or glycemic events are upserted idempotently via primary key conflict resolution (`ON CONFLICT DO UPDATE`).
   - Any `local_date` touched by an ingestion batch has its `daily_analytics` row recomputed (or marked stale via `source_hash` mismatch).
   - Any existing recorded walk (`walk_analytics`) on a touched `local_date` is linked to overlapping `activity_epochs` via `linked_walk_id` and has its glucose overlay refreshed if new `glycemic_events` arrived for its time window.
2. **Profile / Threshold Update (`POST /api/user/settings`)**:
   - Changing `target_low_mmol`, `target_high_mmol`, `hypo_threshold_mmol`, `hyper_threshold_mmol`, `user_type`, or `insulin_profile` alters the user clinical config hash included in `daily_analytics.source_hash`, automatically triggering deterministic recomputation on next read.
   - Changing `glucose_unit` (`'mmol/L'` ↔ `'mg/dL'`) updates the display preference immediately without invalidating canonical `mmol/L` cached analytics.
3. **Account Deletion (`delete_user_data`)**:
   - Atomically deletes all rows for `user_sub` across `user_settings`, `activity_epochs`, `daily_wellness_summaries`, `glycemic_events`, `daily_analytics`, `share_links`, `system_logs`, `walk_sources`, and `walk_analytics`, plus `{DATA_DIR}/{user_sub}/` (including `.garminconnect`, `.carelink`, and `.dexcom` token stores).
