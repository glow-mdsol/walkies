import os
import sys
import json
import queue
import threading
import uuid
import pytest
from pathlib import Path
from datetime import datetime, timezone


# Resolve import paths by adding the backend directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DB_FILE = Path(__file__).resolve().parent / "test_analytics_garmin.sqlite3"
os.environ['ANALYTICS_DB_URL'] = f"sqlite:///{TEST_DB_FILE.as_posix()}"
os.environ['WALKIES_DEV_BYPASS_AUTH'] = '1'

from fastapi.testclient import TestClient
import main
from main import app, _require_user, _user_data_dir, active_auth_attempts, GarminAuthState
from analytics.db import init_analytics_db, get_user_settings, upsert_user_settings

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

def test_garmin_login_immediate_success(monkeypatch, tmp_path):
    client = TestClient(app)
    user_sub = "garmin-sync-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "garmin-test@local",
        "name": "Garmin Test User"
    }
    
    main.DATA_DIR = tmp_path
    
    # Mock Garmin client class
    class MockGarmin:
        def __init__(self, email, password=None, *args, **kwargs):
            self.email = email
            self.password = password
            
        def login(self, *args, **kwargs):
            # No MFA triggered, just return success
            pass

    # Use monkeypatch to mock the import of Garmin client in main.py
    # We mock the class imported in garmin_login_worker
    import sys
    import types
    garmin_module = types.ModuleType("garminconnect")
    garmin_module.Garmin = MockGarmin
    sys.modules["garminconnect"] = garmin_module

    # Trigger login request
    response = client.post(
        "/api/garmin/login",
        json={"email": "test@garmin.com", "password": "secure_password"}
    )
    
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    
    # Verify settings updated
    settings = get_user_settings(user_sub)
    assert settings is not None
    assert settings["garmin_email"] == "test@garmin.com"

def test_garmin_login_password_failure(monkeypatch, tmp_path):
    client = TestClient(app)
    user_sub = "garmin-bad-pwd-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "garmin-pwd@local",
        "name": "Garmin Pwd Test User"
    }
    
    main.DATA_DIR = tmp_path
    
    # Mock Garmin client class simulating invalid credentials
    class MockGarminAuthFail:
        def __init__(self, email, password=None, *args, **kwargs):
            self.email = email
            self.password = password
            
        def login(self, *args, **kwargs):
            raise Exception("GarminConnectAuthenticationError: Invalid credentials or password")

    import sys
    import types
    garmin_module = types.ModuleType("garminconnect")
    garmin_module.Garmin = MockGarminAuthFail
    sys.modules["garminconnect"] = garmin_module

    # Trigger login request with wrong password
    response = client.post(
        "/api/garmin/login",
        json={"email": "wrong@garmin.com", "password": "wrong_password"}
    )
    
    # Must NOT return mfa_required, must return 400 with a clear error message
    assert response.status_code == 400
    assert "Invalid Garmin email or password" in response.json()["detail"]
    assert user_sub not in active_auth_attempts

def test_garmin_login_mfa_flow(monkeypatch, tmp_path):
    client = TestClient(app)
    user_sub = "garmin-mfa-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "mfa-test@local",
        "name": "MFA Test User"
    }
    
    main.DATA_DIR = tmp_path
    
    # Mock Garmin client class triggering MFA callback
    class MockGarminMFA:
        def __init__(self, email, password=None, *args, **kwargs):
            self.email = email
            self.password = password
            self.prompt_mfa = kwargs.get("prompt_mfa")
            
        def login(self, *args, **kwargs):
            # Trigger MFA
            if self.prompt_mfa:
                code = self.prompt_mfa()
                if code != "123456":
                    raise Exception("Invalid MFA code")
            else:
                raise Exception("MFA required but no callback provided")

    import sys
    import types
    garmin_module = types.ModuleType("garminconnect")
    garmin_module.Garmin = MockGarminMFA
    sys.modules["garminconnect"] = garmin_module

    # 1. Trigger initial login request
    response = client.post(
        "/api/garmin/login",
        json={"email": "mfa@garmin.com", "password": "secure_password"}
    )
    
    assert response.status_code == 200
    assert response.json()["status"] == "mfa_required"
    
    # Assert auth state is saved in active attempts
    assert user_sub in active_auth_attempts
    
    # 2. Submit invalid MFA code
    response_bad_mfa = client.post(
        "/api/garmin/login",
        json={"email": "mfa@garmin.com", "mfa_code": "000000"}
    )
    assert response_bad_mfa.status_code == 400
    assert "Invalid MFA code" in response_bad_mfa.json()["detail"]
    
    # Trigger login request again since failed attempt cleaned up the auth state
    response_retry = client.post(
        "/api/garmin/login",
        json={"email": "mfa@garmin.com", "password": "secure_password"}
    )
    assert response_retry.json()["status"] == "mfa_required"
    
    # 3. Submit valid MFA code
    response_good_mfa = client.post(
        "/api/garmin/login",
        json={"email": "mfa@garmin.com", "mfa_code": "123456"}
    )
    assert response_good_mfa.status_code == 200
    assert response_good_mfa.json()["status"] == "success"

    # Verify settings updated
    settings = get_user_settings(user_sub)
    assert settings["garmin_email"] == "mfa@garmin.com"

def test_garmin_sync_activities(monkeypatch, tmp_path):
    client = TestClient(app)
    user_sub = "sync-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "sync-test@local",
        "name": "Sync Test User"
    }
    
    main.DATA_DIR = tmp_path
    
    # 1. Set connected Garmin email in user settings
    upsert_user_settings(user_sub, {"garmin_email": "sync@garmin.com"})
    
    # 2. Mock Garmin client activities list
    class MockGarminSync:
        class ActivityDownloadFormat:
            FIT = "FIT"
            
        def __init__(self, email, password=None, *args, **kwargs):
            self.email = email
            
        def login(self, *args, **kwargs):
            pass
            
        def get_activities(self, start, count):
            return [
                {
                    "activityId": "999888777",
                    "activityName": "Morning Walk",
                    "activityType": {"typeKey": "walking"},
                    "startTimeLocal": "2026-08-23 10:15:30",
                    "startTimeGMT": "2026-08-23T09:15:30Z",
                },
                {
                    "activityId": "111222333",
                    "activityName": "Afternoon Run",
                    "activityType": {"typeKey": "running"},  # Should be filtered out
                    "startTimeLocal": "2026-08-23 16:15:30",
                    "startTimeGMT": "2026-08-23T15:15:30Z",
                }
            ]
            
        def download_activity(self, activity_id, dl_fmt):
            return b"dummy_fit_file_bytes"

    import sys
    import types
    garmin_module = types.ModuleType("garminconnect")
    garmin_module.Garmin = MockGarminSync
    sys.modules["garminconnect"] = garmin_module

    # Mock fit helper decoders and analytics refresh
    monkeypatch.setattr(main, "_fit_time_bounds", lambda data: (
        datetime(2026, 8, 23, 10, 15, 30, tzinfo=timezone.utc),
        datetime(2026, 8, 23, 11, 15, 30, tzinfo=timezone.utc)
    ))
    monkeypatch.setattr(main, "_fit_file_identity", lambda data: "mock-fit-identity-999")
    monkeypatch.setattr(main, "refresh_walk_analytics_if_needed", lambda *args, **kwargs: None)

    # Trigger sync
    response = client.post("/api/garmin/sync")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "ok"
    assert len(data["imported"]) == 1
    assert uuid.UUID(data["imported"][0]["id"])

    
    # Assert file written to disk
    user_dir = _user_data_dir(user_sub)
    date_dir = user_dir / "2026-08-23"
    walk_id = data["imported"][0]["id"]
    walk_dir = date_dir / walk_id
    
    assert walk_dir.exists()
    assert (walk_dir / "activity_999888777.fit").exists()
    assert (walk_dir / "meta.json").exists()
    
    # Assert meta.json contents
    meta = json.loads((walk_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["garmin_activity_id"] == "999888777"
    assert meta["name"] == "Morning Walk"


def test_get_garmin_activities(monkeypatch, tmp_path):
    client = TestClient(app)
    user_sub = "list-act-user"

    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "list-act@local",
        "name": "List Activities User"
    }

    main.DATA_DIR = tmp_path
    upsert_user_settings(user_sub, {"garmin_email": "list@garmin.com"})

    class MockGarminActivities:
        def __init__(self, email, password=None, *args, **kwargs):
            self.email = email

        def login(self, *args, **kwargs):
            pass

        def get_activities(self, start, count):
            return [
                {
                    "activityId": "444555666",
                    "activityName": "Coastal Hike",
                    "activityType": {"typeKey": "hiking"},
                    "startTimeLocal": "2026-08-25 09:30:00",
                    "startTimeGMT": "2026-08-25T08:30:00Z",
                    "distance": 6250.0,
                    "duration": 4200.0,
                    "averageHR": 122,
                    "maxHR": 145,
                },
                {
                    "activityId": "777888999",
                    "activityName": "Evening Stroll",
                    "activityType": {"typeKey": "walking"},
                    "startTimeLocal": "2026-08-25 18:00:00",
                    "startTimeGMT": "2026-08-25T17:00:00Z",
                    "distance": 3100.0,
                    "duration": 1800.0,
                    "averageHR": 105,
                    "maxHR": 120,
                }
            ]

    import sys
    import types
    garmin_module = types.ModuleType("garminconnect")
    garmin_module.Garmin = MockGarminActivities
    sys.modules["garminconnect"] = garmin_module

    res = client.get("/api/garmin/activities")
    assert res.status_code == 200
    data = res.json()
    assert len(data["activities"]) == 2
    assert data["activities"][0]["activity_id"] == "444555666"
    assert data["activities"][0]["name"] == "Coastal Hike"
    assert data["activities"][0]["distance_km"] == 6.25
    assert data["activities"][0]["duration_min"] == 70.0
    assert data["activities"][0]["already_imported"] is False


def test_import_selected_garmin_activity(monkeypatch, tmp_path):
    client = TestClient(app)
    user_sub = "import-act-user"

    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "import-act@local",
        "name": "Import Selected User"
    }

    main.DATA_DIR = tmp_path
    upsert_user_settings(user_sub, {"garmin_email": "import@garmin.com"})

    class MockGarminImport:
        class ActivityDownloadFormat:
            FIT = "FIT"

        def __init__(self, email, password=None, *args, **kwargs):
            self.email = email

        def login(self, *args, **kwargs):
            pass

        def download_activity(self, activity_id, dl_fmt):
            return b"dummy_selected_fit_bytes"

    import sys
    import types
    garmin_module = types.ModuleType("garminconnect")
    garmin_module.Garmin = MockGarminImport
    sys.modules["garminconnect"] = garmin_module

    monkeypatch.setattr(main, "_fit_time_bounds", lambda data: (
        datetime(2026, 8, 25, 9, 30, 0, tzinfo=timezone.utc),
        datetime(2026, 8, 25, 10, 40, 0, tzinfo=timezone.utc)
    ))
    monkeypatch.setattr(main, "_fit_file_identity", lambda data: "mock-fit-identity-444")
    monkeypatch.setattr(main, "refresh_walk_analytics_if_needed", lambda *args, **kwargs: None)

    # Post import with custom name and date
    res = client.post(
        "/api/garmin/activities/import",
        data={
            "activity_id": "444555666",
            "name": "My Custom Walk Name",
            "date": "2026-08-25",
        }
    )
    assert res.status_code == 200
    res_data = res.json()
    assert res_data["status"] == "ok"
    assert res_data["walk"]["name"] == "My Custom Walk Name"
    assert res_data["walk"]["date"] == "2026-08-25"

    user_dir = _user_data_dir(user_sub)
    walk_id = res_data["walk"]["id"]
    walk_dir = user_dir / "2026-08-25" / walk_id
    assert walk_dir.exists()
    assert (walk_dir / "activity_444555666.fit").exists()

    meta = json.loads((walk_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["name"] == "My Custom Walk Name"
    assert meta["garmin_activity_id"] == "444555666"

