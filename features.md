# Walkies — Feature Backlog

This file is intended to be read by an LLM working on this project. Each feature includes enough context to implement it without requiring clarification. Read CLAUDE.md first for data formats, clinical parameters, and architecture overview.

---

## Per-walk analysis enhancements

### DONE - Bolus / insulin event markers on analysis charts
Show bolus deliveries as vertical markers on the dual-axis glucose + HR chart in `WalkAnalysisView`. The Carelink CSV contains bolus records (filter on rows where the `Bolus Volume Delivered (U)` column is non-empty). Render each as a small labelled triangle or dashed vertical line on the glucose axis, with a tooltip showing units delivered. This is high-value because bolus timing relative to activity onset is the main cause of exercise-induced hypos.

### Carb intake markers
Similar to bolus markers above. Carelink CSV may contain carb entries; also consider a manual-entry UI (simple form field in the walk detail view) allowing the user to log carbs consumed during a walk (e.g., a gel taken to pre-empt a hypo). Store in the walk's analytics row as `carb_events_json`.

### Glucose drop rate indicator
Compute the BG rate of change (mmol/L per hour) during the walk window. Show as a stat card: e.g. "−1.4 mmol/L/hr during walk". Flag in red if the rate exceeds 2 mmol/L/hr (rapid drop). Also annotate the glucose trace with a shaded "projected hypo zone" arrow if the last 15-min trend, extrapolated forward 30 min, crosses the 3.9 mmol/L hypo threshold.

### DONE - Elevation + HR + BG overlay chart
A three-panel stacked chart (using Recharts or Plotly): elevation profile, heart rate, and BG — all sharing the same X axis (distance in km). This lets the user visually correlate climbs with BG drops or HR spikes. Data already exists in the FIT parser output (altitude, HR, distance). BG needs to be interpolated to the same distance timestamps.

### DONE - GPS map coloured by glucose level
In the Folium map (or a Leaflet equivalent in the React frontend), colour the GPS polyline segments by BG value at that point in time. Use a green-yellow-red scale: green = in range (4.0-8.0), yellow = mild deviation, red = hypo/hyper. Includes a default no-BGM-data trace when glucose points are unavailable.

### DONE - GPS map coloured by HR zone
Alternative colouring mode for the GPS map: colour by heart rate zone (Zone 1-5 based on max HR of ~175 bpm). Includes a live UI toggle between glucose-colour and HR-colour modes.

### Closed-loop / AUTO mode annotation
The Carelink CSV contains `Suspend` and `Auto Mode` status columns. Annotate the analysis chart with a banner or shaded region showing when the pump was in AUTO mode vs Manual. Also flag "auto suspend" events (pump withheld basal due to predicted low) — these are a key safety signal during exercise.

### Sensor gap detection and annotation
The Guardian 4 CGM has gaps (calibration, sensor warmup, adhesion loss). Detect runs of more than 15 minutes with no glucose reading during the walk window and annotate the chart with a grey "no data" band. Currently missing data may silently break rate-of-change calculations.

### Standalone HTML export
Generate a self-contained `.html` file for a walk (embedding Plotly charts, the Folium map, and summary stats inline) that can be saved and revisited without the app running. Trigger via a "Download report" button in `WalkAnalysisView`. Backend endpoint: `GET /api/walks/{date}/report.html`. Use the `plotly` and `folium` Python libraries already listed in CLAUDE.md preferences.

---

## Cross-walk / aggregate features

### All-walks summary dashboard
A new top-level page (tab or section in the nav) showing aggregate stats across all walks:
- Total distance walked
- Average BG drop during walks (mmol/L)
- Time-in-range during walks vs rest-of-day
- Hypo event count during walks vs baseline
- Average HR by walk
- Simple scatter: distance vs BG drop (does longer = bigger drop?)

### Hypo frequency analysis
Track whether the user goes hypo (≤ 3.9 mmol/L) more often during walks or in the hours after. The "exercise lag" effect on T1D means BG can drop 2–4 hours after activity ends. Pull BG from the Carelink CSV for the full day of each walk and compute hypo events in: (a) walk window, (b) 0–4 hrs post-walk, (c) rest of day. Display as a grouped bar chart.

### Walk tagging and notes
Allow the user to add a short text note to a walk (e.g., "took 15g carbs at km 8", "felt low but sensor was delayed") and assign tags (e.g., `hilly`, `fasted`, `pre-bolus`, `carbs-on-board`). Store in the analytics DB. Surface tags as filter chips on the walk list. This enriches future correlation analysis.

### Walk name editing
Currently walks are identified by date. Add an editable name field (e.g., "Staveley–Kentmere"). Store in the analytics DB. Show the name in the walk list and as the heading in `WalkAnalysisView`. The banner already has a walk name field — wire it up.  We could also have the walk name be imputed from the GPX data (eg Loop from Staveley to Kentmere)

---

## Route comparison enhancements (RouteCompare)

### Normalised BG comparison
On the route comparison chart, show BG as a delta from walk-start BG (i.e., BG − BG[0]) so walks with different starting glucose values are directly comparable. This removes the confound of "started high vs started low" and makes the shape of the BG response visible.

### "Best conditions" ranking
For a route group, rank walks by: lowest BG drop, best time-in-range during walk, lowest max HR per km (efficiency). Surface as a "best walk on this route" highlight to help the user understand what conditions (weather, time of day, pre-walk BG, IOB) lead to better outcomes.

### Planned route (GPX) overlay
When a GPX file is uploaded alongside a FIT file, overlay the planned route on the GPS map as a dashed line and compute deviation from plan (useful for understanding whether the user went off-route and why BG behaved differently).

---

## Clinical / safety features

### Pre-walk BG check reminder
If the user opens a walk upload form and the most recent walk was within 4 hours, show a reminder: "Your last walk ended X hours ago — exercise lag may still be active." Not a hard block, just a contextual nudge.

### DONE - Medtronic SmartGuard
Added SmartGuard-aware analytics and model switching support.
* CareLink parsing now extracts SmartGuard-related suspend states from the `Suspend` column, including `PLGM_PREDICTED_LOW_SG`, generic suspend, and resume (`NORMAL_PUMPING`)
* Analysis payload now includes `smartguard.events` and `smartguard.summary` (event counts and predicted-low suspend count)
* Added SmartGuard summary values in the analysis stats/summary cards
* Added a modal in analysis to switch between Normal basal/bolus model and SmartGuard-adjusted model


### DONE - Insulin on board (IOB) estimate
Using the bolus history from the Carelink CSV and a standard insulin decay model (DIA ≈ 4 hours, peak at 75 min for Humalog), estimate IOB at walk start and annotate the analysis. High IOB at walk start is a major hypo risk factor. Display as a stat card: "Est. IOB at walk start: 1.2 U".  

### DONE - Stress performance on Insulin effect
Added an insulin-stress-effect proxy model that estimates how effort and conditions may amplify insulin effect during active bolus windows. The backend now computes a walk-level multiplier and overlap metrics from HR, uphill grade, apparent temperature, and bolus timing.
* New payload section: `insulinStressEffect` with `stress_multiplier`, `band`, `overlap_minutes`, `weighted_overlap_minutes`, `peak_iob_units_proxy`, and `peak_stress_index`
* UI now shows insulin stress summary values in stats and stress summary cards
* Literature and model rationale captured in `research/stress-insulin-effect.md` with PMID anchors for validation planning

### Weather-based risk flag
Already fetching weather from Open-Meteo. Add a computed flag: high temperature + humidity increases hypo risk (sweat, absorption rate). If temp > 25°C or humidity > 80%, add a "heat advisory" badge to the walk card.

---

## Infrastructure / UX

### Backfill progress indicator
`POST /api/analytics/backfill` currently runs silently. Add a simple polling endpoint (`GET /api/analytics/backfill/status`) or SSE stream that the frontend can use to show a progress bar while analytics are computed for all walks.

### Walk list search and filter
Add a search box and filter controls to the walk list: filter by date range, distance range, tag, or route group. Useful as the walk count grows.

### File type validation on upload
Currently any file can be uploaded. Add frontend + backend validation: accept only `.fit`, `.csv`, `.gpx` files. Warn if no FIT file is uploaded (GPS/HR data will be missing from analysis).

### Dark mode
Add a CSS dark-mode variant using the existing CSS custom properties (`--primary`, `--bg` etc.). Toggle via a button in the header, persisted to `localStorage`.


### Deploy the application
We need to deploy the app so people can use it. This should be something that can be triggered via github actions or similar.  This is a fun app, so the hosting environment should be economical.  
We need to add:
* A landing page 
  * Granting the ability for users to register and login.  We can use google login for this.
* Their uploaded files should be protected from any unauthorized access  
* They should be able to share a walk analysis via a link without requiring login.  
* We should use aggressive caching mechanisms, as the data won't change after upload.  
* We should migrate the analytics db to allow for cloud deployment 
* Let's support docker deployment

Suggested implementation plan:
1) Identity + data isolation
- Google OAuth login in frontend, backend token verification, per-user data isolation for walks/files.
- API authorization must enforce owner-only access for private walk endpoints.

2) Public sharing (no login)
- Add share links backed by durable tokens (`share_links` table with created_at, expires_at optional, revoked_at optional).
- Public endpoint serves read-only analysis payload for a valid token.
- Owner can revoke links at any time.

3) Aggressive caching
- Private immutable analysis responses: send ETag and `Cache-Control: private, max-age=0, must-revalidate`.
- Shared/public analysis pages and payloads: send CDN-friendly `Cache-Control` with `s-maxage` and `stale-while-revalidate`.
- Static frontend assets: long-lived immutable cache headers (`max-age=31536000, immutable`).
- Upload/refresh/recompute actions must invalidate affected cache keys.
- Mutable endpoints (notes/tags/name/share-revoke) must not be long-lived cached.

4) Cloud analytics persistence
- Migrate analytics persistence from local-only SQLite assumptions to managed Postgres.
- Add schema migration support and an idempotent backfill/import command.
- All database config must be env-driven for local/dev/prod parity.

5) CI/CD and economical hosting
- GitHub Actions workflows for frontend and backend deploys.
- Frontend on low-cost static hosting/CDN.
- Backend on low-cost container hosting.
- Managed Postgres tier sized for hobby usage, with backup enabled.

Acceptance criteria:
- Anonymous users only see landing page and valid shared links.
- Authenticated users can access only their own private walks/files.
- Shared link works without login and can be revoked immediately.
- Re-upload or analytics recompute updates the shared/private analysis on next request (cache invalidation verified).
- Deploy from `main` is automated via GitHub Actions with no manual file-copy steps.
- App state survives backend restarts/redeploys (analytics and share-link metadata persist in cloud DB).
