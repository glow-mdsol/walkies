# Analysis Catalogue

This file lists analysis sets the app can do based on available data.

Legend:
- [x] Implemented now
- [ ] Planned / not implemented yet

## FIT / GPX only (activity-first)

### Session summary
- [x] Distance, duration, average HR
- [x] Activity timeline (distance-aligned HR + altitude)

### Route and map
- [x] GPS route map with start/finish markers
- [x] Route coloring by HR zone
- [x] Hour markers on map

### Weather context (from route/time)
- [x] Temperature and apparent temperature along route
- [x] Wind speed along route
- [x] Headwind profile (distance-aligned)
- [x] Wind direction exposure (wind rose)
- [x] Weather stress score + band
- [x] Headwind exposure percentage

### Stress / HR decoupling (FIT/GPX + optional weather)
- [x] Heuristic model of expected HR from exertion proxy (speed + uphill grade + heat load)
- [x] Distance-aligned chart: observed HR vs expected HR + HR residual
- [x] Possible non-exertional HR elevation score + elevated-minutes estimate
- [x] Caveat in report labeling this as non-diagnostic (review signal, not anxiety diagnosis)
- [x] In-report interpretation note (what this signal can and cannot mean)
- [x] User sensitivity controls (residual smoothing + elevated threshold)
- [x] Multi-session trend chart (decoupling score + elevated minutes)

### Potential additions
- [ ] Pace/speed breakdown by segment
- [ ] Elevation gain/loss and grade analysis
- [ ] Compare two walks (normalized distance/time)

## Daily & Multi-Day Intraday Epoch Correlation (Garmin + Multi-Provider CGM)

### 24-Hour Epochal Activity & Glycemic Correlation (`/api/daily/{date}/analysis`)
- [x] 15-minute intraday step count, distance, active/moderately-active/highly-active seconds, mean HR, and primary activity category (`sedentary`, `walking`, `running`, `cycling`, `generic`)
- [x] Workout deduplication (`linked_walk_id`) so recorded FIT walks are annotated without double-counting steps
- [x] 24-hour CGM alignment (`mean_bg_mmol`, `bg_delta_mmol`, `iob_units`) across 15-minute buckets
- [x] Sensor gap detection (`> 15 min` missing CGM interval) with visual gap bands and exclusion from interpolation
- [x] Consensus glycemic summary: Time in Range (`%TIR`), Time Below Range (`%TBR`), Time Above Range (`%TAR`), `%CV`, and sustained hypoglycemia episodes (`>= 15 min` below threshold, ATTD 2019 consensus)
- [x] Per-activity-category duration, step totals, during-activity BG delta, and 1-hour / 2-hour post-activity lag glucose changes
- [x] Public revocable read-only share links for single-day (`daily`) and multi-day (`range`) summaries

### Multi-Day Aggregate Comparison (`/api/daily/range`)
- [x] 7-, 14-, and 30-day rolling trend charts (daily steps, active minutes, `%TIR`, `%TBR`, `%TAR`, mean BG)
- [x] Active vs. sedentary day comparison (`active_days_avg_tir_pct` vs `sedentary_days_avg_tir_pct`)

## Multi-Provider CGM & Insulin Telemetry (`/api/dexcom/*`, `/api/carelink/*`, `/api/telemetry/import`)

- [x] Direct cloud sync for Medtronic CareLink (24-hour CGM, basal, bolus, carbs, SmartGuard states)
- [x] Direct cloud sync for Dexcom Share (`us` and `ous` regions, 7-day default lookback and up to 30-day custom range)
- [x] Unified CSV auto-detection and ingestion (`POST /api/telemetry/import`) for Medtronic CareLink, Dexcom Clarity (`mmol/L` and `mg/dL`), and Tandem t:connect (CGM `EGV`, basal, bolus, carbs, Control-IQ states)
- [x] Automatic synchronization of imported 24h `glycemic_events` into overlapping walk analyses

## FIT + CGM / Insulin (Per-Walk Diabetes Analysis)

### Core diabetes metrics
- [x] BG delta during walk
- [x] BG slope during walk (including first-30-min vs rest-of-walk split)
- [x] Time in range (customizable target range, default 4.0–8.0 mmol/L / 72–144 mg/dL)
- [x] Hypo count during walk
- [x] Bolus total and effort-adjusted Insulin-on-Board (IOB) decay during walk

### Combined timeline
- [x] Distance-aligned multi-axis timeline with:
  - [x] HR
  - [x] Altitude
  - [x] BG (`mmol/L` or `mg/dL`)
  - [x] Basal rate
  - [x] Bolus events
- [x] Walk-window shading and phase markers

### Training insight panels & Exercise Risk Profile
- [x] Phase glucose response (pre / during / post)
- [x] HR-zone vs glucose stability
- [x] Evidence-based Exercise Risk Profile, Recovery Monitor (0–12h nocturnal window), and Next Walk Preparation (Riddell et al. 2017; Zaharieva et al. 2020; Scott et al. 2021)

## Generalized Regimens, Audience Modes & Units

- [x] User-selectable glucose display unit (`mmol/L` or `mg/dL`) across all charts, stat cards, slopes, and settings inputs (canonical `mmol/L` backend storage)
- [x] Generalized therapy regimen profiles (`Closed-Loop Pump`, `Standard Pump`, `Multiple Daily Injections / MDI`, `Non-Insulin / Lifestyle`, `Non-Diabetic with CGM`, `General Fitness`)
- [x] Dynamic UI adaptation (hiding closed-loop/SmartGuard panels for MDI or non-insulin users, hiding insulin panels for CGM-only users)
- [x] Auto-hide diabetes-only reports if BG data is missing
- [x] BG data badge (detected / not present)
