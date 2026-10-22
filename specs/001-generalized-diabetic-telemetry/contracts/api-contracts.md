# API & UI Contracts: Generalized Diabetic Support, Epochal Garmin Ingestion, and Multi-Provider CGM Correlation

**Feature**: `001-generalized-diabetic-telemetry`  
**Date**: 2026-10-22  
**Base URL**: `/api` (proxied from Vite `:5173` to FastAPI `:8000`)  
**Authentication**: `Authorization: Bearer <google_id_token>` (except public `/api/shared/{token}/*` endpoints).

---

## 1. User Clinical Settings & Unit Preference Contract

### `GET /api/user/settings` & `POST /api/user/settings`

Retrieves or updates the user's generalized diabetic profile, preferred glucose display unit (`mmol/L` or `mg/dL`), clinical thresholds (stored canonically in `mmol/L`), and connected provider metadata.

**Request / Response Payload (`application/json`)**:
```json
{
  "user_type": "diabetic_closed_loop",
  "insulin_type": "ClosedLoopPump",
  "glucose_unit": "mmol/L",
  "target_low_mmol": 4.0,
  "target_high_mmol": 8.0,
  "hypo_threshold_mmol": 3.9,
  "hyper_threshold_mmol": 12.9,
  "pump_brand_model": "MiniMed 780G",
  "cgm_provider": "carelink",
  "basal_rates": { "default": 0.8, "exercise": 0.5 },
  "carb_ratios": { "default": 5.0 },
  "insulin_sensitivity_factor": 2.0,
  "correction_threshold": 8.0,
  "exercise_correction_factor": 1.5,
  "iob_calculation_method": "exponential",
  "insulin_profile": {
    "name": "NovoRapid",
    "onsetMins": 15,
    "earlyActionMins": 30,
    "peakMinHours": 1.0,
    "peakMaxHours": 2.0,
    "durationMinHours": 3.0,
    "durationMaxHours": 5.0
  },
  "garmin_email": "user@example.com",
  "carelink_email": "carelink_user",
  "dexcom_username": "dexcom_user",
  "dexcom_region": "ous",
  "last_garmin_sync_at": "2026-10-22T00:10:00Z",
  "last_cgm_sync_at": "2026-10-22T00:12:00Z"
}
```
*Note*: Backward-compatible alias `/api/settings` continues to point to the same handler. When `glucose_unit == "mg/dL"`, the frontend converts displayed and edited threshold/ISF values using `1 mmol/L = 18.0182 mg/dL` while sending canonical `mmol/L` fields to the backend (or passing `input_unit: "mg/dL"` for server-side normalization).

---

## 2. Multi-Provider Cloud Sync & File Ingestion Contracts

### 2.1 Garmin Epochal & Activity Sync: `POST /api/garmin/sync`

Extended to accept an optional date window (defaulting to the last 7 days, maximum 30 days per batch) and sync both recorded GPS walks and 15-minute daily wellness epochs.

**Request Body (optional JSON)**:
```json
{
  "start_date": "2026-10-15",
  "end_date": "2026-10-22",
  "include_epochs": true,
  "include_walks": true
}
```

**Response (`200 OK`)**:
```json
{
  "status": "ok",
  "start_date": "2026-10-15",
  "end_date": "2026-10-22",
  "days_synced": 8,
  "epochs_upserted": 768,
  "imported": [
    { "id": "84e01c88-...", "name": "Morning Hike", "date": "2026-10-21" }
  ]
}
```

### 2.2 Dexcom Direct Cloud Connection & Sync

- **`POST /api/dexcom/login`**: Authenticates with Dexcom Share (`username`, `password`, `region`: `'us' | 'ous'`), validates connection, stores isolated session credentials in `{DATA_DIR}/{user_sub}/.dexcom`, and updates `dexcom_username` in `user_settings`.
- **`POST /api/dexcom/sync`**: Pulls CGM readings for the requested lookback window (default 7 days, up to 30 days where available via provider history), normalizes to canonical `mmol/L` in `glycemic_events`, updates overlapping `walk_analytics`, and refreshes `daily_analytics`.
- **`POST /api/carelink/sync`**: Extended with optional `{"start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}` (default 7 days, max 30 days) to persist full 24-hour `glycemic_events` in addition to updating individual walk overlays.

### 2.3 Unified Multi-Provider CSV / File Import: `POST /api/telemetry/import`

Allows importing historical CSV extracts from **Medtronic CareLink**, **Dexcom Clarity**, or **Tandem t:connect** into the normalized `glycemic_events` store.

**Multipart Form Fields**:
- `provider`: `'auto' | 'carelink' | 'dexcom' | 'tandem'` (default `'auto'` detects format from headers)
- `file`: `.csv` file upload
- `tz_offset_mins`: Optional integer timezone offset in minutes for local wall-clock CSV timestamps (default `0`)

**Response (`200 OK`)**:
```json
{
  "status": "ok",
  "detected_provider": "dexcom",
  "source_unit": "mg/dL",
  "events_ingested": 2016,
  "date_range": { "start_date": "2026-10-08", "end_date": "2026-10-21" },
  "days_updated": 14,
  "walks_updated": 3
}
```

---

## 3. Daily & Multi-Day Aggregate Correlation Contracts

### 3.1 Single-Day Epochal & Glycemic Analysis: `GET /api/daily/{date}/analysis`

Returns the cached, deterministic 24-hour epochal movement and glycemic correlation payload for `date` (`YYYY-MM-DD`). Supports `ETag` / `If-None-Match` and `Cache-Control: private, max-age=0, must-revalidate`.

**Response (`200 OK`)**:
```json
{
  "date": "2026-10-21",
  "analytics_version": "2026-10-22-v3",
  "computed_at": "2026-10-22T00:15:00Z",
  "regimen": {
    "user_type": "diabetic_closed_loop",
    "insulin_type": "ClosedLoopPump",
    "glucose_unit": "mmol/L",
    "target_low_mmol": 4.0,
    "target_high_mmol": 8.0,
    "hypo_threshold_mmol": 3.9,
    "hyper_threshold_mmol": 12.9
  },
  "summary": {
    "total_steps": 11420,
    "active_minutes": 95,
    "resting_hr": 58,
    "tir_pct": 84.2,
    "tbr_pct": 2.1,
    "tar_pct": 13.7,
    "avg_bg_mmol": 6.4,
    "bg_cv_pct": 24.5,
    "hypos_count": 1,
    "bolus_total_units": 18.5,
    "carb_total_grams": 110.0,
    "sensor_coverage_pct": 97.9
  },
  "epochs": [
    {
      "epoch_start_utc": "2026-10-21T08:00:00Z",
      "epoch_end_utc": "2026-10-21T08:15:00Z",
      "local_time": "09:00",
      "steps": 1450,
      "activity_category": "walking",
      "intensity_level": "moderate",
      "mean_hr": 112,
      "avg_bg_mmol": 6.8,
      "bg_delta_mmol": -0.4,
      "iob_units": 1.25,
      "linked_walk_id": "84e01c88-..."
    }
  ],
  "cgm_series": [
    { "timestamp_utc": "2026-10-21T08:00:00Z", "local_time": "09:00", "bg_mmol": 7.0, "trend": "FLAT" }
  ],
  "sensor_gaps": [
    { "start_utc": "2026-10-21T14:10:00Z", "end_utc": "2026-10-21T14:45:00Z", "duration_mins": 35 }
  ],
  "therapy_events": {
    "boluses": [{ "timestamp_utc": "2026-10-21T07:30:00Z", "units": 4.0 }],
    "carbs": [{ "timestamp_utc": "2026-10-21T07:30:00Z", "grams": 20.0 }],
    "pump_states": [{ "timestamp_utc": "2026-10-21T08:20:00Z", "state": "PLGM_PREDICTED_LOW_SG" }]
  },
  "activity_category_breakdown": [
    {
      "category": "walking",
      "duration_mins": 75,
      "steps": 8200,
      "avg_bg_mmol": 6.1,
      "mean_bg_change_1h_post_mmol": -1.1,
      "tir_pct": 91.0,
      "hypo_events_during_or_2h_post": 0
    },
    {
      "category": "sedentary",
      "duration_mins": 840,
      "steps": 620,
      "avg_bg_mmol": 6.9,
      "mean_bg_change_1h_post_mmol": 0.2,
      "tir_pct": 80.5,
      "hypo_events_during_or_2h_post": 1
    }
  ],
  "linked_walks": [
    { "id": "84e01c88-...", "name": "Morning Hike", "start_time": "2026-10-21T08:00:00Z", "distance_km": 6.4, "tir_pct": 92.0 }
  ]
}
```

### 3.2 Multi-Day Range Summary: `GET /api/daily/range?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`

Returns aggregate statistics and daily breakdown rows across a 7, 14, or 30-day window (up to 90 days max), comparing active vs. sedentary days and summarizing glycemic response per activity type.

**Response (`200 OK`)**:
```json
{
  "start_date": "2026-10-15",
  "end_date": "2026-10-21",
  "days_count": 7,
  "aggregate": {
    "total_steps": 68400,
    "avg_daily_steps": 9771,
    "tir_pct": 82.4,
    "tbr_pct": 1.8,
    "tar_pct": 15.8,
    "avg_bg_mmol": 6.6,
    "bg_drop_per_1000_steps_mmol": -0.18,
    "active_days_tir_pct": 86.5,
    "sedentary_days_tir_pct": 74.2
  },
  "activity_category_totals": [
    { "category": "walking", "total_mins": 410, "total_steps": 48200, "tir_pct": 88.0, "mean_bg_delta_mmol": -0.9 }
  ],
  "days": [
    {
      "date": "2026-10-21",
      "total_steps": 11420,
      "active_minutes": 95,
      "tir_pct": 84.2,
      "tbr_pct": 2.1,
      "tar_pct": 13.7,
      "avg_bg_mmol": 6.4,
      "linked_walks_count": 1
    }
  ]
}
```

---

## 4. Public Share Links for Daily & Range Views

- **`POST /api/daily/share-links`**:
  - Request: `{"share_type": "daily" | "range", "start_date": "2026-10-21", "end_date": "2026-10-21", "expires_in_days": 30}`
  - Response: `{"token": "...", "share_type": "daily", "start_date": "2026-10-21", "end_date": "2026-10-21", "created_at": "..."}`
- **`GET /api/daily/share-links?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`**: Lists active share links for the specified daily/range view.
- **`GET /api/shared/{token}/analysis`**:
  - Extended to inspect `share_links.share_type`.
  - If `'walk'`, returns the existing walk analysis payload.
  - If `'daily'`, returns the read-only `GET /api/daily/{date}/analysis` payload (sanitized of user email/credentials).
  - If `'range'`, returns the read-only `GET /api/daily/range` payload for `[target_start_date, target_end_date]`.
- **`DELETE /api/share-links/{token}`**: Existing endpoint revokes any share token owned by the authenticated user regardless of `share_type`.
