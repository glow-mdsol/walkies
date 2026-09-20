import os
import sys
import json
import pytest
from pathlib import Path
from datetime import datetime, timezone

# Resolve import paths by adding the backend directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DB_FILE = Path(__file__).resolve().parent / "test_analytics_carelink.sqlite3"
os.environ['ANALYTICS_DB_URL'] = f"sqlite:///{TEST_DB_FILE.as_posix()}"
os.environ['WALKIES_DEV_BYPASS_AUTH'] = '1'

from fastapi.testclient import TestClient
import main
from main import app, _require_user, _user_data_dir
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

def test_carelink_upload_token_success(monkeypatch, tmp_path):
    client = TestClient(app)
    user_sub = "carelink-upload-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "carelink-upload@local",
        "name": "Carelink Upload User"
    }
    
    main.DATA_DIR = tmp_path
    
    # Mock CareLinkClient
    class MockCareLinkClient:
        def __init__(self, tokenFile):
            self.tokenFile = tokenFile
            
        def init(self):
            return True
            
        def _get_access_token_payload(self, token_data):
            return {
                "token_details": {
                    "preferred_username": "connected@carelink.com"
                }
            }

    import sys
    import types
    carelink_module = types.ModuleType("carelink_client2")
    carelink_module.CareLinkClient = MockCareLinkClient
    sys.modules["carelink_client2"] = carelink_module

    # Valid token structure
    token_json = {
        "access_token": "valid.header.payload.signature",
        "refresh_token": "valid_refresh",
        "scope": "openid",
        "client_id": "valid_client_id"
    }
    
    # Send upload request
    response = client.post(
        "/api/carelink/upload-token",
        files={"file": ("logindata.json", json.dumps(token_json), "application/json")}
    )
    
    assert response.status_code == 200
    assert response.json()["status"] == "success"
    assert response.json()["username"] == "connected@carelink.com"
    
    # Verify file saved on disk
    user_dir = _user_data_dir(user_sub)
    token_path = user_dir / ".carelink" / "logindata.json"
    assert token_path.exists()
    
    # Verify settings updated in DB
    settings = get_user_settings(user_sub)
    assert settings is not None
    assert settings["carelink_email"] == "connected@carelink.com"

def test_carelink_sync_success(monkeypatch, tmp_path):
    client = TestClient(app)
    user_sub = "carelink-sync-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "carelink-sync@local",
        "name": "Carelink Sync User"
    }
    
    main.DATA_DIR = tmp_path
    
    # 1. Connected CareLink settings
    upsert_user_settings(user_sub, {"carelink_email": "synced@carelink.com"})
    
    # 2. Setup mock token file
    user_dir = _user_data_dir(user_sub)
    carelink_dir = user_dir / ".carelink"
    carelink_dir.mkdir(parents=True, exist_ok=True)
    token_path = carelink_dir / "logindata.json"
    token_path.write_text('{"dummy": "token"}', encoding="utf-8")
    
    # 3. Setup mock Garmin walk directory on disk
    walk_id = "2026-08-23-150000-afternoon-walk"
    walk_dir = user_dir / "2026-08-23" / walk_id
    walk_dir.mkdir(parents=True, exist_ok=True)
    
    # Write metadata
    meta = {
        "id": walk_id,
        "date": "2026-08-23",
        "name": "Afternoon Walk",
        "start_time": "2026-08-23T15:00:00+00:00",
    }
    (walk_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    
    # Mock CareLinkClient fetching last 24h data
    class MockCareLinkClientSync:
        def __init__(self, tokenFile):
            self.tokenFile = tokenFile
            
        def init(self):
            return True
            
        def getRecentData(self):
            return {
                "patientData": {
                    "sgs": [
                        # valid reading (180 mg/dL -> 10.0 mmol/L)
                        {"sg": 180, "timestamp": "2026-08-23T15:05:00.000Z"},
                        # outside range (ignored)
                        {"sg": 120, "timestamp": "2026-08-23T20:00:00.000Z"}
                    ],
                    "markers": [
                        # insulin injection inside walk bounds
                        {"type": "INSULIN", "amount": 2.5, "timestamp": "2026-08-23T15:10:00.000Z"}
                    ]
                }
            }

    import sys
    import types
    carelink_module = types.ModuleType("carelink_client2")
    carelink_module.CareLinkClient = MockCareLinkClientSync
    sys.modules["carelink_client2"] = carelink_module

    # Mock time bounds, identity, and analytics update pipelines
    monkeypatch.setattr(main, "_fit_time_bounds", lambda data: (
        datetime(2026, 8, 23, 15, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 8, 23, 16, 0, 0, tzinfo=timezone.utc)
    ))
    monkeypatch.setattr(main, "refresh_walk_analytics_if_needed", lambda *args, **kwargs: None)

    # 4. Trigger synchronization
    response = client.post("/api/carelink/sync")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "ok"
    assert len(data["synced_walks"]) == 1
    assert data["synced_walks"][0]["id"] == walk_id
    
    # Assert carelink_sync.csv file created on disk
    csv_path = walk_dir / "carelink_sync.csv"
    assert csv_path.exists()
    
    # Assert CSV contents
    csv_lines = csv_path.read_text(encoding="utf-8-sig").splitlines()
    assert len(csv_lines) == 3 # Header + 1 Glucose row + 1 Insulin row
    assert "Sensor Glucose (mmol/L)" in csv_lines[0]
    
    # First row is Glucose 9.99 (180 mg/dL * 0.0555)
    assert "9.99" in csv_lines[1]
    # Second row is Bolus 2.5 units
    assert "2.5" in csv_lines[2]
