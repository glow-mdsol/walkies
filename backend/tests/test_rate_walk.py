import os
import sys
import json
from pathlib import Path
import pytest

# Resolve import paths by adding the backend directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DB_FILE = Path(__file__).resolve().parent / "test_analytics_rate.sqlite3"
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

def test_rate_walk_api(tmp_path):
    client = TestClient(app)
    user_sub = "rate-test-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "rate-test@local",
        "name": "Rate Test User"
    }

    # Override DATA_DIR to use a temp folder so we don't write to real data/
    main.DATA_DIR = tmp_path
    
    # 1. Create a dummy walk structure
    user_dir = _user_data_dir(user_sub)
    walk_id = "2026-08-23-rate-test"
    walk_dir = user_dir / walk_id
    walk_dir.mkdir()
    
    meta = {
        "id": walk_id,
        "date": "2026-08-23",
        "name": "Rename Test Walk",
        "start_time": "2026-08-23T10:00:00+00:00",
    }
    meta_file = walk_dir / "meta.json"
    meta_file.write_text(json.dumps(meta), encoding="utf-8")
    
    # 2. Insert cached row in DB (should default to 3 if not specified)
    upsert_walk_analytics(
        walk_meta=meta,
        source_hash="hash123",
        computed_at="2026-08-23T11:00:00Z",
        metrics={"distance_km": 5.2, "duration_h": 1.1, "avg_hr": 120, "bg_delta": -1.5, "tir_pct": 85},
        summary={"score": 8},
    )
    
    # Verify initial rating is 3 in DB
    row = get_cached_analytics_row(walk_id)
    assert row is not None
    assert row["rating"] == 3
    
    # 3. Request Rating change to 5
    response = client.post(
        f"/api/walks/{walk_id}/rate",
        json={"rating": 5}
    )
    
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "rating": 5}
    
    # 4. Assert disk meta.json is updated
    updated_meta = json.loads(meta_file.read_text(encoding="utf-8"))
    assert updated_meta["rating"] == 5
    
    # 5. Assert database row is updated
    updated_row = get_cached_analytics_row(walk_id)
    assert updated_row is not None
    assert updated_row["rating"] == 5

def test_rate_walk_validation():
    client = TestClient(app)
    user_sub = "rate-test-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "rate-test@local",
        "name": "Rate Test User"
    }

    # Test out of range ratings (e.g. 0, 6)
    for bad_rating in [0, 6, -1, 10]:
        response = client.post(
            "/api/walks/2026-08-23-rate-test/rate",
            json={"rating": bad_rating}
        )
        assert response.status_code == 400
        assert "Rating must be an integer between 1 and 5" in response.json()["detail"]

    # Test bad type ratings (e.g. float, string)
    for bad_type in [4.5, "5", [3], None]:
        response = client.post(
            "/api/walks/2026-08-23-rate-test/rate",
            json={"rating": bad_type}
        )
        assert response.status_code == 400
        assert "Rating must be an integer between 1 and 5" in response.json()["detail"]

def test_rate_non_existent_walk():
    client = TestClient(app)
    user_sub = "rate-test-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "rate-test@local",
        "name": "Rate Test User"
    }
    
    response = client.post(
        "/api/walks/non-existent-walk-id/rate",
        json={"rating": 4}
    )
    
    assert response.status_code == 404
    assert response.json()["detail"] == "Walk not found"
