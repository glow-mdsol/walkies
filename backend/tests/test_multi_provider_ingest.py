"""Unit and API contract tests for User Story 2: Direct Multi-Provider CGM & Insulin Telemetry Ingestion."""

import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DB_FILE = Path(__file__).resolve().parent / "test_multi_provider.sqlite3"
os.environ["ANALYTICS_DB_URL"] = f"sqlite:///{TEST_DB_FILE.as_posix()}"
os.environ["WALKIES_DEV_BYPASS_AUTH"] = "1"

from fastapi.testclient import TestClient
import main
from main import app, _require_user
from analytics.db import (
    get_glycemic_events_for_date,
    get_user_settings,
    init_analytics_db,
    upsert_user_settings,
)
import telemetry.dexcom_client as dexcom_mod


@pytest.fixture(autouse=True)
def setup_test_db():
    if TEST_DB_FILE.exists():
        try:
            TEST_DB_FILE.unlink()
        except OSError:
            pass
    init_analytics_db()
    yield
    if TEST_DB_FILE.exists():
        try:
            TEST_DB_FILE.unlink()
        except OSError:
            pass


def test_dexcom_login_and_sync_mocked(monkeypatch, tmp_path):
    """Verify Dexcom Share login and cloud sync ('ous' and 'us') with mocked httpx client."""
    client = TestClient(app)
    user_sub = "dexcom-test-user"
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "dexcom@local",
        "name": "Dexcom User",
    }
    main.DATA_DIR = tmp_path

    class MockResponse:
        def __init__(self, status_code, payload):
            self.status_code = status_code
            self._payload = payload

        def json(self):
            return self._payload

    class MockHttpxClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def post(self, url, json=None, params=None):
            if "AuthenticatePublisherAccount" in url:
                return MockResponse(200, "acct-uuid-1234")
            if "LoginPublisherAccountById" in url:
                return MockResponse(200, "sess-uuid-5678")
            if "ReadPublisherLatestGlucoseValues" in url:
                # 126 mg/dL -> 6.99 mmol/L; 144 mg/dL -> 7.99 mmol/L
                return MockResponse(
                    200,
                    [
                        {"WT": "Date(1792576800000)", "Value": 126, "Trend": "Flat"},
                        {"WT": "Date(1792577100000)", "Value": 144, "Trend": "FortyFiveUp"},
                    ],
                )
            return MockResponse(404, {})

    monkeypatch.setattr(dexcom_mod.httpx, "Client", MockHttpxClient)

    login_res = client.post(
        "/api/dexcom/login",
        json={"username": "dex_hiker", "password": "secret", "region": "ous"},
    )
    assert login_res.status_code == 200
    assert login_res.json()["status"] == "success"

    settings = get_user_settings(user_sub)
    assert settings["dexcom_username"] == "dex_hiker"
    assert settings["dexcom_region"] == "ous"
    assert settings["cgm_provider"] == "dexcom"

    sync_res = client.post(
        "/api/dexcom/sync",
        json={"start_date": "2026-10-21", "end_date": "2026-10-21"},
    )
    assert sync_res.status_code == 200
    sync_data = sync_res.json()
    assert sync_data["events_upserted"] == 2

    events = get_glycemic_events_for_date(user_sub, "2026-10-21")
    assert len(events) == 2
    assert events[0]["bg_mmol"] == 6.99
    assert events[0]["trend_arrow"] == "FLAT"
    assert events[1]["bg_mmol"] == 7.99
    assert events[1]["trend_arrow"] == "FORTY_FIVE_UP"


def test_multi_provider_csv_import_and_idempotent_dedup(tmp_path):
    """Verify CareLink, Dexcom Clarity (mg/dL), and Tandem t:connect CSV auto-detection, conversion, and deduplication."""
    client = TestClient(app)
    user_sub = "csv-import-user"
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "csv@local",
        "name": "CSV User",
    }
    main.DATA_DIR = tmp_path

    # 1. Dexcom Clarity CSV in mg/dL
    dexcom_csv = "\n".join([
        "Index,Timestamp (YYYY-MM-DDThh:mm:ss),Event Type,Event Subtype,Glucose Value (mg/dL),Insulin Value (u),Carb Value (grams)",
        "1,2026-10-21T09:00:00,EGV,,180,,",
        "2,2026-10-21T09:05:00,EGV,,108,,",
        "3,2026-10-21T09:05:00,Insulin,Fast-Acting,,3.5,",
        "4,2026-10-21T09:05:00,Carbs,,, ,25",
    ])
    res_dex = client.post(
        "/api/telemetry/import",
        data={"provider": "auto"},
        files={"file": ("dexcom_clarity.csv", dexcom_csv.encode("utf-8"), "text/csv")},
    )
    assert res_dex.status_code == 200
    data_dex = res_dex.json()
    assert data_dex["detected_provider"] == "dexcom"
    assert data_dex["source_unit"] == "mg/dL"
    assert data_dex["events_ingested"] == 4

    # Re-upload the exact same file to verify idempotent deduplication (zero duplicate rows in DB)
    res_dex_dup = client.post(
        "/api/telemetry/import",
        data={"provider": "auto"},
        files={"file": ("dexcom_clarity.csv", dexcom_csv.encode("utf-8"), "text/csv")},
    )
    assert res_dex_dup.status_code == 200
    events_after_dup = get_glycemic_events_for_date(user_sub, "2026-10-21")
    assert len(events_after_dup) == 4
    cgm_rows = [e for e in events_after_dup if e["event_type"] == "cgm"]
    assert len(cgm_rows) == 2
    assert cgm_rows[0]["bg_mmol"] == 9.99  # 180 / 18.0182 -> 9.99

    # 2. Tandem t:connect CSV
    tandem_csv = "\n".join([
        "EventDateTime,Readings (mg/dL),BolusVolumeDelivered,BasalRate,CarbSize,ControlIQMode",
        "2026-10-20T14:00:00,144,2.0,0.85,15,Exercise",
        "2026-10-20T14:05:00,135,,, ,",
    ])
    res_tan = client.post(
        "/api/telemetry/import",
        data={"provider": "auto"},
        files={"file": ("tandem_export.csv", tandem_csv.encode("utf-8"), "text/csv")},
    )
    assert res_tan.status_code == 200
    data_tan = res_tan.json()
    assert data_tan["detected_provider"] == "tandem"
    events_tan = get_glycemic_events_for_date(user_sub, "2026-10-20")
    assert any(e["event_type"] == "pump_state" and e["pump_state"] == "CONTROL_IQ_EXERCISE" for e in events_tan)

    # 3. CareLink CSV in mmol/L
    carelink_csv = "\n".join([
        "Index,Date,Time,Sensor Glucose (mmol/L),Basal Rate (U/h),Bolus Volume Delivered (U),BWZ Carb Input (grams),Suspend",
        "1,2026/10/19,10:00:00,5.6,0.9,1.5,10,PLGM_PREDICTED_LOW_SG",
    ])
    res_cl = client.post(
        "/api/telemetry/import",
        data={"provider": "auto"},
        files={"file": ("carelink_export.csv", carelink_csv.encode("utf-8"), "text/csv")},
    )
    assert res_cl.status_code == 200
    data_cl = res_cl.json()
    assert data_cl["detected_provider"] == "carelink"
    assert data_cl["source_unit"] == "mmol/L"
    events_cl = get_glycemic_events_for_date(user_sub, "2026-10-19")
    assert len(events_cl) == 5  # cgm + basal + bolus + carb + pump_state
