# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## About this project

Geoffrey Low is a Type 1 diabetic (MiniMed 780G MMT-1885 pump + Guardian 4 CGM, mostly in closed-loop AUTO mode) who is an active walker/hiker. This app stores per-walk files and will generate analyses correlating activity data against glucose/insulin data.

## Commands

```bash
task dev       # start both servers with auto-reload (backend :8000, frontend :5173)
task           # alias for task dev
task install   # install all dependencies (uv sync + npm install)
task backend   # backend only
task frontend  # frontend only
task build     # production build of frontend → frontend/dist/
```

The Vite proxy forwards `/api` to `:8000`, so no CORS issues in dev.

## Architecture

```
walkies/
├── backend/
│   ├── main.py          # FastAPI app — all API routes
│   ├── pyproject.toml   # UV/Python deps
│   └── data/            # gitignored; one subfolder per walk date (YYYY-MM-DD)
└── frontend/
    ├── src/
    │   ├── App.jsx                    # root component, fetches walk list
    │   └── components/
    │       ├── UploadForm.jsx         # date picker + file input → POST /api/walks/upload
    │       └── WalkList.jsx           # lists walks with files; DELETE /api/walks/{date}
    └── vite.config.js   # dev proxy: /api → http://localhost:8000
```

### API routes
| Method | Path | Purpose |
|--------|------|---------|
| GET | `/api/walks` | List all walks, sorted newest-first |
| POST | `/api/walks/upload` | Multipart: `date` (YYYY-MM-DD) + `files[]` |
| DELETE | `/api/walks/{date}` | Remove a walk folder and all contents |

### File storage
`backend/data/{YYYY-MM-DD}/` — files land here as-is. The date in the folder name is the walk date chosen at upload time.

## Data formats

### Carelink CSV (Medtronic MiniMed 780G)
- 6 header rows to skip: `pd.read_csv(skiprows=6)`
- Filter rows where `Date` matches `\d{4}/\d{2}/\d{2}`
- Datetime: `Date + ' ' + Time`, format `%Y/%m/%d %H:%M:%S`
- Glucose is in **mmol/L**

### Garmin FIT files
Use a **custom binary parser** (not fitparse — may not be available). Key implementation notes:
- Records use compressed timestamps; seed `last_timestamp` from field 253 in non-compressed messages, then apply 5-bit rollover logic for compressed headers
- Field mapping: `0`=lat, `1`=lon (both sint32 semicircles → degrees via `* 180/2^31`), `3`=heart_rate (uint8), `5`=distance (uint32, cm→m via `/100`), `253`=timestamp (uint32, seconds since 1989-12-31)

### GPX files
Planned route (XML); parse with `xml.etree.ElementTree` or `gpxpy`.

## Clinical context

| Parameter | Value |
|-----------|-------|
| Target glucose range | 4.0–8.0 mmol/L |
| Hypo threshold | ≤ 3.9 mmol/L |
| Hyper threshold | ≥ 12.9 mmol/L |
| Insulin:carb ratio | 5 g/U |
| Sensitivity factor | 2 mmol/L per U |

## Analysis preferences

- Output: **standalone HTML files** (save-and-revisit)
- Libraries: `pandas`, `plotly` (interactive), `folium` (GPS maps), `scipy` (stats)
- Don't re-explain data parsing — go straight to analysis and visualisation
- High-value analyses:
  - Glucose drop rate during exercise and hypo risk prediction
  - Cross-referencing activity intensity (HR, speed, elevation) vs glucose trajectory
  - Interactive Plotly dashboard: glucose + HR dual-axis, bolus event markers, walk period shaded
  - Folium map of GPS track coloured by glucose level or HR zone
  - Summary stats: time in range, hypo events during vs outside activity, glucose drop rate per km
