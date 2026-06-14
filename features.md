# Walkies — Feature Backlog

This file is intended to be read by an LLM working on this project. Each feature includes enough context to implement it without requiring clarification. Read CLAUDE.md first for data formats, clinical parameters, and architecture overview.

## General

This is a fun project for me, so let's keep the costs minimal.  Where possible, use open source libraries and free services.  Where possible, use Python libraries and free services.  

I want to be able to share the experience with other people, so it should support running locally or be deployed and run on inexpensive cloud infrastructure.  I would prefer using Google Cloud infrastructure.  That means that the app should run in Docker and containerization should be used for deployment.  

The code should follow the practices described in CLAUDE.md and be well documented.  Ensure that the code is well tested and that there are unit tests for all the major functionality.  The tests should be stored in the `tests` directory and should follow the practices described in CLAUDE.md.

This data can be extremely sensitive, so ensure that all data is stored securely and that there are appropriate measures in place to protect the data from unauthorised access.  ie no data in the code, no data in the git repo etc.  Use environment variables and secrets management to store sensitive data.

Ensure that wherever possible data is protected (encrypted) at rest and in transit.   ie Use encryption for sensitive data at rest and in transit.  Don't store passwords in the code.  Use environment variables.

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

## Data Source Support

### Automated Ingestion
Allow a user to connect to the upstream systems and pull data in automatically.  This should be done in a way that is secure and private.  The user should be able to select which data source to use for a given walk.  The data source should be displayed on the Walk Analysis View when the user is viewing a walk.   It's possible there are existing APIs/libraries for automated ingestion.  Do some research on the best way to implement this.

Make sure that a user's credentials are secure and private.  I would suggest that the user stores their credentials in the app, and only the app has access to them.


### Support Dexcom CSV imports
Support loading glucose data from the DEXCOM sensor extract (as CSV).  It should support as many of the features as possible of the Medtronic CSV imports.  It's important to consider what fields should be extracted for an extract for each manufacturer.  
For example, Dexcom does not include bolus data in its extracts, so it will not be possible to show bolus markers on the charts.  On the other hand, Dexcom does not include auto mode, so it will not be possible to show auto mode annotations on the charts.  


### Support Tandem Pump data formats
Research what features are available to be extracted for Tandem Pumps.  To get feature parity with Medtronic Pump data, it may be necessary to support both Tandem Data and Dexcom in a generic way.  Research the methods for extracting the data, what formats are used and how best to support this.


### Multiple Data Sources
The UI should support a choice for either; No insulin pump data, Medtronic Format (Carelink) and Dexcom Format.  If the Tandem Pump data is available, that should be an option as well.  Each of these data sources should be able to be imported independently, and the UI should allow the user to select which data source to use for a given walk.  The import process should be the same as the current import process for Medtronic data.  (ie upload a CSV file)   

### DONE - Insulin Treatment Dashboard
When a User registers for the app, they should be asked if they use an Insulin Pump and if so, which type.  If they use Medtronic, Dexcom, or Tandem, the app should automatically select the correct data source.  If they use multiple sources, they should be able to select which data source to use for a given walk.  
In addition to selecting the data source, the user should be able to enter their insulin treatment plan.  This should include:
- Insulin Type (MDI or Pump)
- Pump Brand and Model
- Basal Rates (default and exercise)
- Carb Ratios
- Insulin Sensitivity Factor
- Correction Threshold
- Exercise Correction Factor
- Insulin on Board Calculation Method

This information should be stored in the database and used to provide personalized insights and recommendations to the user.  It should also be displayed on the Walk Analysis View when the user is viewing a walk.  The treatment plan should be displayed in a way that is easy to understand and use.  The treatment plan should also be used to calculate the expected BG drop during a walk and compare it to the actual BG drop.  This will allow the user to see if their treatment plan is effective and if they need to make any adjustments.

### Automated extraction of Data Sources
The source data is extracted from data portals (eg Garmin Connect, Carelink).  Investigate whether it is possible to automate the extraction of this data.  Do they make available a API that can be used to extract the data?  Are there libraries that can be used to extract the data?  

It should be possible to have a button on the Dashboard to allow the user to "pull" the latest data from the data sources.  If the data is available, it should be added to the database and the user should be notified that the data has been added.  The data should be added to the database in a way that is easy to understand and use.  The data should also be used to provide personalized insights and recommendations to the user.  The user should be able to select which data source to use for a given walk.  The data source should be displayed on the Walk Analysis View when the user is viewing a walk.   

Ensure that if credentials are required, these are protected to the highest level.

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
