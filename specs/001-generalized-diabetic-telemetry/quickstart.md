# Quickstart & Validation Guide: Generalized Diabetic Support, Epochal Garmin Ingestion, and Multi-Provider CGM Correlation

**Feature**: `001-generalized-diabetic-telemetry`  
**Date**: 2026-10-22

---

## 1. Prerequisites & Environment Setup

1. **Install Backend & Frontend Dependencies**:
   ```powershell
   # From repository root
   uv sync --directory backend
   npm --prefix frontend install
   ```
2. **Start Local Development Environment (with Auth Bypass for Local Testing)**:
   ```powershell
   # Terminal 1: Start FastAPI backend on :8000
   $env:WALKIES_DEV_BYPASS_AUTH="1"
   uv run --directory backend uvicorn main:app --host 0.0.0.0 --port 8000 --reload

   # Terminal 2: Start Vite frontend on :5173
   npm --prefix frontend run dev
   ```

---

## 2. End-to-End Validation Scenarios

### Scenario 1: Configure Generalized Diabetic Regimen & Glucose Units (`mmol/L` ↔ `mg/dL`)
1. Open `http://localhost:5173` and navigate to **Settings** (`UserSettings`).
2. Select a therapy regimen (`Closed-Loop Pump`, `Standard Pump`, `MDI`, or `Non-Insulin / Lifestyle`) and toggle **Glucose Unit** between `mmol/L` and `mg/dL`.
3. Verify that Target Range (`4.0–8.0 mmol/L` ↔ `72–144 mg/dL`), Hypo (`3.9 mmol/L` ↔ `70 mg/dL`), Hyper (`12.9 mmol/L` ↔ `232 mg/dL`), and ISF fields automatically convert in the form, and that saving persists canonical `mmol/L` values via [API Contracts](./contracts/api-contracts.md#1-user-clinical-settings--unit-preference-contract).
4. Switch to any walk or daily analysis view and confirm that all glucose axes, stat cards, and rate-of-change labels reflect the chosen unit (`mmol/L` or `mg/dL`).

### Scenario 2: Sync or Import Multi-Provider CGM & Garmin Intraday Epochs
1. Trigger a 7-day sync (or custom date range up to 30 days) for Garmin (`POST /api/garmin/sync`) and either CareLink (`POST /api/carelink/sync`), Dexcom (`POST /api/dexcom/sync`), or upload a CareLink / Dexcom Clarity / Tandem CSV via `POST /api/telemetry/import`.
2. Open the **Daily & Aggregate Dashboard** tab for a synced date (`GET /api/daily/{date}/analysis`).
3. Verify:
   - 15-minute step epochs and activity category bars align with the 5-minute CGM trace.
   - Any CGM gap `>15 minutes` is shaded as a grey "No Data" band and excluded from rate-of-change math.
   - Days containing a recorded GPS walk display the linked walk badge on the overlapping epochs without double-counting total daily steps.

### Scenario 3: Multi-Day Aggregate & Public Share Link Verification
1. Switch the Daily Dashboard to the **7 / 14 / 30-Day Aggregate** view (`GET /api/daily/range`).
2. Verify the breakdown of Time In Range and mean glucose change by activity category (`walking`, `running`, `sedentary`, etc.) and Active vs. Sedentary days comparison.
3. Click **Share Summary** (`POST /api/daily/share-links`) to generate a public link, open `?share=<token>` in an unauthenticated browser session, verify read-only rendering, and revoke the token (`DELETE /api/share-links/{token}`).

---

## 3. Automated Test & Build Gates

```powershell
# Run full backend pytest suite (all external Garmin/CareLink/Dexcom calls mocked)
uv run --directory backend pytest -v

# Verify production frontend build
npm --prefix frontend run build
```
