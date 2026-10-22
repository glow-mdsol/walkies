"""Unit and contract tests for User Story 1: Daily Epochal Activity & Glycemic Correlation."""

import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DB_FILE = Path(__file__).resolve().parent / "test_daily_correlation.sqlite3"
os.environ["ANALYTICS_DB_URL"] = f"sqlite:///{TEST_DB_FILE.as_posix()}"
os.environ["WALKIES_DEV_BYPASS_AUTH"] = "1"

from fastapi.testclient import TestClient
import main
from main import app, _require_user
from analytics.db import (
    init_analytics_db,
    upsert_activity_epochs,
    upsert_daily_wellness_summary,
    upsert_glycemic_events,
    upsert_user_settings,
)
from analytics.models.daily_correlation import (
    compute_daily_correlation,
    detect_sensor_gaps,
    detect_sustained_hypo_episodes,
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


def test_sensor_gap_detection_and_sustained_hypo_counting():
    """Verify >15m sensor gaps are flagged and ATTD >=15m sustained hypo episodes are counted accurately."""
    base_dt = datetime(2026, 10, 21, 8, 0, 0, tzinfo=timezone.utc)
    cgm_events = []

    # 08:00 to 09:00: normal 6.5 mmol/L every 5 mins
    for m in range(0, 65, 5):
        ts = (base_dt + timedelta(minutes=m)).isoformat()
        cgm_events.append({
            "timestamp_utc": ts,
            "event_type": "cgm",
            "local_date": "2026-10-21",
            "bg_mmol": 6.5,
        })

    # 09:00 to 09:40: 40-minute sensor gap (>15m) -> next sample at 09:40
    # 09:40, 09:45, 09:50, 09:55: 4 samples (15m span) at 3.5 mmol/L -> 1 sustained hypo episode
    for m in (100, 105, 110, 115):
        ts = (base_dt + timedelta(minutes=m)).isoformat()
        cgm_events.append({
            "timestamp_utc": ts,
            "event_type": "cgm",
            "local_date": "2026-10-21",
            "bg_mmol": 3.5,
        })

    # 10:00 to 10:30: recovery at 5.8 mmol/L
    for m in range(120, 155, 5):
        ts = (base_dt + timedelta(minutes=m)).isoformat()
        cgm_events.append({
            "timestamp_utc": ts,
            "event_type": "cgm",
            "local_date": "2026-10-21",
            "bg_mmol": 5.8,
        })

    # Single 5-min dip at 10:35 (3.7 mmol/L) followed by 5.5 at 10:40 -> NOT a sustained >=15m hypo
    cgm_events.append({
        "timestamp_utc": (base_dt + timedelta(minutes=155)).isoformat(),
        "event_type": "cgm",
        "local_date": "2026-10-21",
        "bg_mmol": 3.7,
    })
    cgm_events.append({
        "timestamp_utc": (base_dt + timedelta(minutes=160)).isoformat(),
        "event_type": "cgm",
        "local_date": "2026-10-21",
        "bg_mmol": 5.5,
    })

    epochs = [
        {
            "epoch_start_utc": "2026-10-21T08:00:00+00:00",
            "epoch_end_utc": "2026-10-21T08:15:00+00:00",
            "local_date": "2026-10-21",
            "steps": 1400,
            "activity_category": "walking",
            "intensity_level": "moderate",
        },
        # Epoch inside the 40-min sensor gap (09:15 to 09:30)
        {
            "epoch_start_utc": "2026-10-21T09:15:00+00:00",
            "epoch_end_utc": "2026-10-21T09:30:00+00:00",
            "local_date": "2026-10-21",
            "steps": 50,
            "activity_category": "sedentary",
            "intensity_level": "sedentary",
        },
    ]

    result = compute_daily_correlation(
        local_date="2026-10-21",
        epochs=epochs,
        glycemic_events=cgm_events,
    )

    assert len(result["sensor_gaps"]) == 1
    assert result["sensor_gaps"][0]["duration_mins"] == 40.0
    # Epoch inside the 40m gap must NOT interpolate BG
    gap_epoch = result["epochs"][1]
    assert gap_epoch["avg_bg_mmol"] is None
    assert gap_epoch["bg_delta_mmol"] is None

    # Only 1 sustained >=15m hypo episode (the single 5-min dip at 10:35 is ignored)
    assert result["summary"]["hypos_count"] == 1
    # Because the hypo at 09:40 occurred within 2 hours after the 08:00-08:15 walking epoch,
    # walking category breakdown attributes it to hypo_events_during_or_2h_post!
    walk_cat = next(c for c in result["activity_category_breakdown"] if c["category"] == "walking")
    assert walk_cat["hypo_events_during_or_2h_post"] == 1


def test_garmin_sync_epochs_and_walk_deduplication(monkeypatch, tmp_path):
    """Verify Garmin sync ingests 15-min epochs, links overlapping walks, and never double-counts steps."""
    client = TestClient(app)
    user_sub = "us1-epoch-user"
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "us1@local",
        "name": "US1 User",
    }
    main.DATA_DIR = tmp_path
    upsert_user_settings(user_sub, {"garmin_email": "us1@garmin.com", "user_type": "diabetic_closed_loop"})

    class MockGarminWithEpochs:
        class ActivityDownloadFormat:
            FIT = "FIT"

        def __init__(self, email, password=None, *args, **kwargs):
            self.email = email

        def login(self, *args, **kwargs):
            pass

        def get_activities(self, start, count):
            return [
                {
                    "activityId": "555111222",
                    "activityName": "Lunchtime Brisk Walk",
                    "activityType": {"typeKey": "walking"},
                    "startTimeLocal": "2026-10-21 12:00:00",
                    "startTimeGMT": "2026-10-21T12:00:00Z",
                }
            ]

        def download_activity(self, activity_id, dl_fmt):
            return b"dummy_fit"

        def get_steps_data(self, d_str):
            if d_str != "2026-10-21":
                return []
            return [
                {
                    "startGMT": "2026-10-21T12:00:00.0",
                    "endGMT": "2026-10-21T12:15:00.0",
                    "steps": 1600,
                    "primaryActivityLevel": "walking",
                },
                {
                    "startGMT": "2026-10-21T12:15:00.0",
                    "endGMT": "2026-10-21T12:30:00.0",
                    "steps": 1500,
                    "primaryActivityLevel": "walking",
                },
                {
                    "startGMT": "2026-10-21T14:00:00.0",
                    "endGMT": "2026-10-21T14:15:00.0",
                    "steps": 40,
                    "primaryActivityLevel": "sedentary",
                },
            ]

        def get_stats(self, d_str):
            if d_str != "2026-10-21":
                return {}
            return {
                "totalSteps": 3140,
                "totalDistanceMeters": 2400.0,
                "moderateIntensityMinutes": 30,
                "vigorousIntensityMinutes": 0,
                "restingHeartRate": 56,
            }

        def get_heart_rates(self, d_str):
            return {
                "restingHeartRate": 56,
                "heartRateValues": [
                    [int(datetime(2026, 10, 21, 12, 5, tzinfo=timezone.utc).timestamp() * 1000), 118],
                    [int(datetime(2026, 10, 21, 12, 20, tzinfo=timezone.utc).timestamp() * 1000), 124],
                ],
            }

    garmin_mod = types.ModuleType("garminconnect")
    garmin_mod.Garmin = MockGarminWithEpochs
    sys.modules["garminconnect"] = garmin_mod

    monkeypatch.setattr(main, "_fit_time_bounds", lambda data: (
        datetime(2026, 10, 21, 12, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 10, 21, 12, 30, 0, tzinfo=timezone.utc),
    ))
    monkeypatch.setattr(main, "_fit_file_identity", lambda data: "fit-id-555111222")
    monkeypatch.setattr(main, "refresh_walk_analytics_if_needed", lambda *args, **kwargs: None)

    # Seed CGM & bolus events for 2026-10-21
    upsert_glycemic_events(
        user_sub,
        [
            {
                "timestamp_utc": "2026-10-21T11:30:00+00:00",
                "event_type": "bolus",
                "local_date": "2026-10-21",
                "bolus_units": 2.0,
                "source_provider": "carelink",
            },
            {
                "timestamp_utc": "2026-10-21T12:00:00+00:00",
                "event_type": "cgm",
                "local_date": "2026-10-21",
                "bg_mmol": 7.8,
                "source_provider": "carelink",
            },
            {
                "timestamp_utc": "2026-10-21T12:15:00+00:00",
                "event_type": "cgm",
                "local_date": "2026-10-21",
                "bg_mmol": 6.9,
                "source_provider": "carelink",
            },
            {
                "timestamp_utc": "2026-10-21T12:30:00+00:00",
                "event_type": "cgm",
                "local_date": "2026-10-21",
                "bg_mmol": 6.1,
                "source_provider": "carelink",
            },
            {
                "timestamp_utc": "2026-10-21T13:15:00+00:00",
                "event_type": "cgm",
                "local_date": "2026-10-21",
                "bg_mmol": 5.8,
                "source_provider": "carelink",
            },
        ],
    )

    sync_res = client.post(
        "/api/garmin/sync",
        json={"start_date": "2026-10-21", "end_date": "2026-10-21"},
    )
    assert sync_res.status_code == 200
    sync_data = sync_res.json()
    assert sync_data["epochs_upserted"] == 3
    assert len(sync_data["imported"]) == 1
    imported_walk_id = sync_data["imported"][0]["id"]

    # Query single-day analysis
    day_res = client.get("/api/daily/2026-10-21/analysis")
    assert day_res.status_code == 200
    day_payload = day_res.json()

    # Steps are NOT double-counted despite having a recorded walk overlapping 12:00-12:30
    assert day_payload["summary"]["total_steps"] == 3140
    assert day_payload["summary"]["tir_pct"] == 100.0
    assert len(day_payload["epochs"]) == 3
    assert day_payload["epochs"][0]["linked_walk_id"] == imported_walk_id
    assert day_payload["epochs"][0]["mean_hr"] == 118.0
    assert day_payload["epochs"][0]["iob_units"] > 0

    # Query multi-day range analysis
    range_res = client.get("/api/daily/range?start_date=2026-10-20&end_date=2026-10-21")
    assert range_res.status_code == 200
    range_data = range_res.json()
    assert range_data["days_count"] == 2
    assert range_data["aggregate"]["total_steps"] == 3140
    assert range_data["aggregate"]["active_days_tir_pct"] == 100.0

    # Create and verify public daily share link
    share_res = client.post(
        "/api/daily/share-links",
        json={"share_type": "daily", "start_date": "2026-10-21"},
    )
    assert share_res.status_code == 200
    token = share_res.json()["token"]

    pub_res = client.get(f"/api/shared/{token}/analysis")
    assert pub_res.status_code == 200
    pub_data = pub_res.json()
    assert pub_data["shared"] is True
    assert pub_data["share_type"] == "daily"
    assert pub_data["summary"]["total_steps"] == 3140
    # Verify private email is stripped
    assert "garmin_email" not in pub_data.get("user_settings", {})
