import os
import sys
import json
from pathlib import Path
import pytest

# Resolve import paths by adding the backend directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DB_FILE = Path(__file__).resolve().parent / "test_analytics_rename.sqlite3"
os.environ['ANALYTICS_DB_URL'] = f"sqlite:///{TEST_DB_FILE.as_posix()}"
os.environ['WALKIES_DEV_BYPASS_AUTH'] = '1'

from fastapi.testclient import TestClient
import main
from main import app, _require_user, _user_data_dir
from analytics.db import (
    init_analytics_db,
    upsert_walk_analytics,
    get_cached_analytics_row,
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

def test_rename_walk_api(tmp_path):
    client = TestClient(app)
    user_sub = "rename-test-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "rename-test@local",
        "name": "Rename Test User"
    }

    # Override DATA_DIR to use a temp folder so we don't write to real data/
    main.DATA_DIR = tmp_path
    
    # 1. Create a dummy walk structure
    user_dir = _user_data_dir(user_sub)
    walk_id = "2026-08-23-rename-test"
    walk_dir = user_dir / walk_id
    walk_dir.mkdir()
    
    meta = {
        "id": walk_id,
        "date": "2026-08-23",
        "name": "Original Name",
        "start_time": "2026-08-23T10:00:00+00:00",
    }
    meta_file = walk_dir / "meta.json"
    meta_file.write_text(json.dumps(meta), encoding="utf-8")
    
    # 2. Insert cached row in DB
    upsert_walk_analytics(
        walk_meta=meta,
        source_hash="hash123",
        computed_at="2026-08-23T11:00:00Z",
        metrics={"distance_km": 5.2, "duration_h": 1.1, "avg_hr": 120, "bg_delta": -1.5, "tir_pct": 85},
        summary={"score": 8},
    )
    
    # Verify initial name in DB
    row = get_cached_analytics_row(walk_id)
    assert row is not None
    assert row["name"] == "Original Name"
    
    # 3. Request Rename
    new_name = "Beautiful Kentmere Loop"
    response = client.post(
        f"/api/walks/{walk_id}/rename",
        json={"name": new_name}
    )
    
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "name": new_name}
    
    # 4. Assert disk meta.json is updated
    updated_meta = json.loads(meta_file.read_text(encoding="utf-8"))
    assert updated_meta["name"] == new_name
    
    # 5. Assert database row is updated
    updated_row = get_cached_analytics_row(walk_id)
    assert updated_row is not None
    assert updated_row["name"] == new_name

def test_rename_non_existent_walk():
    client = TestClient(app)
    user_sub = "rename-test-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "rename-test@local",
        "name": "Rename Test User"
    }
    
    response = client.post(
        "/api/walks/non-existent-walk-id/rename",
        json={"name": "New Name"}
    )
    
    assert response.status_code == 404
    assert response.json()["detail"] == "Walk not found"
