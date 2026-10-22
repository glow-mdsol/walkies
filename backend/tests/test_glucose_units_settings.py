"""Unit and API tests for User Story 3: Generalized Diabetic Regimen & Glucose Unit Configuration."""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DB_FILE = Path(__file__).resolve().parent / "test_glucose_units_settings.sqlite3"
os.environ["ANALYTICS_DB_URL"] = f"sqlite:///{TEST_DB_FILE.as_posix()}"
os.environ["WALKIES_DEV_BYPASS_AUTH"] = "1"

from fastapi.testclient import TestClient
import main
from main import app, _require_user
from analytics.db import (
    get_user_settings,
    init_analytics_db,
    upsert_activity_epochs,
    upsert_glycemic_events,
    upsert_user_settings,
)


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


def test_generalized_regimens_and_mgdl_input_normalization():
    """Verify generalized diabetic regimens and canonical mmol/L storage from mg/dL inputs."""
    client = TestClient(app)
    user_sub = "us3-regimen-user"
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "us3@local",
        "name": "US3 User",
    }

    # 1. Save MDI regimen with mg/dL unit preference and mg/dL threshold inputs
    res = client.post(
        "/api/user/settings",
        json={
            "user_type": "diabetic_mdi",
            "glucose_unit": "mg/dL",
            "input_unit": "mg/dL",
            "target_low_mmol": 70,          # 70 mg/dL -> 3.88 mmol/L
            "target_high_mmol": 180,        # 180 mg/dL -> 9.99 mmol/L
            "hypo_threshold_mmol": 70,      # 70 mg/dL -> 3.88 mmol/L
            "hyper_threshold_mmol": 250,    # 250 mg/dL -> 13.87 mmol/L
            "insulin_sensitivity_factor": 36,  # 36 mg/dL/U -> 2.0 mmol/L/U
            "correction_threshold": 144,    # 144 mg/dL -> 7.99 mmol/L
            "carb_ratios": {"default": 8.0},
        },
    )
    assert res.status_code == 200
    saved = get_user_settings(user_sub)
    assert saved["user_type"] == "diabetic_mdi"
    assert saved["insulin_type"] == "MDI"
    assert saved["glucose_unit"] == "mg/dL"
    assert saved["target_low_mmol"] == 3.88
    assert saved["target_high_mmol"] == 9.99
    assert saved["hypo_threshold_mmol"] == 3.88
    assert saved["hyper_threshold_mmol"] == 13.87
    assert saved["insulin_sensitivity_factor"] == 2.0
    assert saved["correction_threshold"] == 7.99
    assert saved["basal_rates"] is None

    # 2. Switch to non-insulin / lifestyle diabetic profile
    res2 = client.post(
        "/api/settings",
        json={
            "user_type": "diabetic_lifestyle",
            "glucose_unit": "mmol/L",
            "target_low_mmol": 3.9,
            "target_high_mmol": 7.8,
        },
    )
    assert res2.status_code == 200
    saved2 = get_user_settings(user_sub)
    assert saved2["user_type"] == "diabetic_lifestyle"
    assert saved2["insulin_type"] == "None"
    assert saved2["glucose_unit"] == "mmol/L"
    assert saved2["target_low_mmol"] == 3.9
    assert saved2["target_high_mmol"] == 7.8
    assert saved2["insulin_profile"] is None


def test_threshold_change_invalidates_daily_analytics_cache(tmp_path):
    """Verify changing target_high_mmol in settings automatically invalidates and recomputes daily TIR."""
    client = TestClient(app)
    user_sub = "us3-cache-user"
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "cache@local",
        "name": "Cache User",
    }
    main.DATA_DIR = tmp_path

    upsert_user_settings(
        user_sub,
        {
            "user_type": "diabetic_closed_loop",
            "target_low_mmol": 4.0,
            "target_high_mmol": 8.0,
        },
    )
    upsert_activity_epochs(
        user_sub,
        [
            {
                "epoch_start_utc": "2026-10-21T10:00:00Z",
                "epoch_end_utc": "2026-10-21T10:15:00Z",
                "local_date": "2026-10-21",
                "steps": 1200,
                "activity_category": "walking",
                "intensity_level": "moderate",
            }
        ],
    )
    upsert_glycemic_events(
        user_sub,
        [
            {
                "timestamp_utc": "2026-10-21T10:00:00Z",
                "event_type": "cgm",
                "local_date": "2026-10-21",
                "bg_mmol": 6.0,
            },
            {
                "timestamp_utc": "2026-10-21T10:05:00Z",
                "event_type": "cgm",
                "local_date": "2026-10-21",
                "bg_mmol": 9.0,  # Above 8.0 mmol/L, but within 10.0 mmol/L
            },
        ],
    )

    # Initial analysis with target_high_mmol = 8.0 -> 1 of 2 readings in range (50.0% TIR)
    r1 = client.get("/api/daily/2026-10-21/analysis")
    assert r1.status_code == 200
    assert r1.json()["summary"]["tir_pct"] == 50.0

    # Update target_high_mmol to 10.0 -> cache hash changes -> 2 of 2 readings in range (100.0% TIR)
    client.post(
        "/api/user/settings",
        json={
            "user_type": "diabetic_closed_loop",
            "target_low_mmol": 4.0,
            "target_high_mmol": 10.0,
        },
    )
    r2 = client.get("/api/daily/2026-10-21/analysis")
    assert r2.status_code == 200
    assert r2.json()["summary"]["tir_pct"] == 100.0
