import os
import sys
import shutil
from pathlib import Path
import pytest

# Resolve import paths by adding the backend directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DB_FILE = Path(__file__).resolve().parent / "test_analytics.sqlite3"
os.environ['ANALYTICS_DB_URL'] = f"sqlite:///{TEST_DB_FILE.as_posix()}"
os.environ['WALKIES_DEV_BYPASS_AUTH'] = '1'

from fastapi.testclient import TestClient
import main
from main import app, _require_user, _user_data_dir
from analytics.db import (
    init_analytics_db,
    get_user_settings,
    upsert_user_settings,
    create_share_link,
    get_share_link,
    upsert_walk_analytics,
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

def test_delete_account_api():
    client = TestClient(app)
    user_sub = "account-delete-user"
    
    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "delete-test@local",
        "name": "Delete Test User"
    }
    
    try:
        # 1. Populate settings
        settings = {
            "user_type": "diabetic_pump",
            "insulin_type": "Pump",
            "pump_brand_model": "Medtronic 780G",
            "basal_rates": {"default": 0.8},
            "carb_ratios": {"default": 5.0},
        }
        upsert_user_settings(user_sub, settings)
        
        # 2. Add some walks and share links
        walk_id = "2026-07-10-delete-walk"
        create_share_link("test-token-123", user_sub, walk_id, "2026-07-10T12:00:00Z")
        
        # Mock walk analytics row
        walk_meta = {
            "id": walk_id,
            "date": "2026-07-10",
            "name": "Test Walk",
            "start_time": "2026-07-10T12:00:00Z"
        }
        metrics = {
            "distance_km": 5.2,
            "duration_h": 1.2,
            "avg_hr": 135.0,
            "bg_delta": -2.5,
            "tir_pct": 90.0,
            "bolus_units": 2.0,
            "weather_stress_score": 15.0,
            "hr_decoupling_score": 2.0
        }
        summary = {}
        upsert_walk_analytics(
            walk_meta=walk_meta,
            source_hash="dummy-hash",
            computed_at="2026-07-10T12:05:00Z",
            metrics=metrics,
            summary=summary
        )
        
        # 3. Setup fake walk directory on disk
        user_dir = _user_data_dir(user_sub)
        walk_dir = user_dir / walk_id
        walk_dir.mkdir(parents=True, exist_ok=True)
        # Create a mock file
        (walk_dir / "activity.fit").write_text("dummy fit contents")
        (walk_dir / "meta.json").write_text('{"id": "2026-07-10-delete-walk", "date": "2026-07-10", "name": "Test Walk"}')
        
        # Verify setups exist
        assert get_user_settings(user_sub) is not None
        assert get_share_link("test-token-123") is not None
        assert user_dir.exists()
        assert walk_dir.exists()
        
        # 4. Perform DELETE request to api
        res = client.delete("/api/user/account")
        assert res.status_code == 200
        assert res.json() == {"status": "ok"}
        
        # 5. Assert all data deleted
        # Settings should be gone
        assert get_user_settings(user_sub) is None
        # Share link should be gone
        assert get_share_link("test-token-123") is None
        # Disk directory should be deleted
        assert not user_dir.exists()
        
    finally:
        app.dependency_overrides.clear()
        # Ensure clean up of mock directories in case assertions failed
        user_dir = _user_data_dir(user_sub)
        if user_dir.exists():
            shutil.rmtree(user_dir, ignore_errors=True)
