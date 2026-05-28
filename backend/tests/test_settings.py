import os
import sys
from pathlib import Path
import pytest
from datetime import datetime

# Resolve import paths by adding the backend directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Set test environment database to file-based SQLite in tests directory
# (SQLite in-memory creates a fresh empty DB on each connection call, so we need a file to persist tables)
TEST_DB_FILE = Path(__file__).resolve().parent / "test_analytics.sqlite3"
os.environ['ANALYTICS_DB_URL'] = f"sqlite:///{TEST_DB_FILE.as_posix()}"
os.environ['WALKIES_DEV_BYPASS_AUTH'] = '1'

from fastapi.testclient import TestClient
from main import app, _require_user
from analytics.db import init_analytics_db, get_user_settings, upsert_user_settings


@pytest.fixture(autouse=True)
def setup_test_db():
    # Remove any stale test db file
    if TEST_DB_FILE.exists():
        try:
            TEST_DB_FILE.unlink()
        except OSError:
            pass
            
    # Force DB initialization
    init_analytics_db()
    yield
    
    # Clean up the test database file
    if TEST_DB_FILE.exists():
        try:
            TEST_DB_FILE.unlink()
        except OSError:
            pass


def test_db_settings_pump():
    user_sub = "test-user-pump"
    settings = {
        "insulin_type": "Pump",
        "pump_brand_model": "Medtronic 780G",
        "basal_rates": {"default": 0.95, "exercise": 0.60},
        "carb_ratios": {"default": 8.5},
        "insulin_sensitivity_factor": 2.2,
        "correction_threshold": 7.5,
        "exercise_correction_factor": 1.3,
        "iob_calculation_method": "exponential",
        "insulin_profile": {"name": "NovoRapid", "onsetMins": 15}
    }
    
    upsert_user_settings(user_sub, settings)
    retrieved = get_user_settings(user_sub)
    
    assert retrieved is not None
    assert retrieved["user_sub"] == user_sub
    assert retrieved["insulin_type"] == "Pump"
    assert retrieved["pump_brand_model"] == "Medtronic 780G"
    assert retrieved["basal_rates"] == {"default": 0.95, "exercise": 0.60}
    assert retrieved["carb_ratios"] == {"default": 8.5}
    assert retrieved["insulin_sensitivity_factor"] == 2.2
    assert retrieved["correction_threshold"] == 7.5
    assert retrieved["exercise_correction_factor"] == 1.3
    assert retrieved["iob_calculation_method"] == "exponential"
    assert retrieved["insulin_profile"] == {"name": "NovoRapid", "onsetMins": 15}
    assert "updated_at" in retrieved
    assert retrieved["updated_at"] is not None


def test_db_settings_mdi():
    user_sub = "test-user-mdi"
    settings = {
        "insulin_type": "MDI",
        "pump_brand_model": None,
        "basal_rates": None,
        "carb_ratios": {"default": 10.0},
        "insulin_sensitivity_factor": 2.0,
        "correction_threshold": 8.0,
        "exercise_correction_factor": 1.0,
        "iob_calculation_method": "linear",
        "insulin_profile": {"name": "Humalog", "onsetMins": 10}
    }
    
    upsert_user_settings(user_sub, settings)
    retrieved = get_user_settings(user_sub)
    
    assert retrieved is not None
    assert retrieved["user_sub"] == user_sub
    assert retrieved["insulin_type"] == "MDI"
    assert retrieved["pump_brand_model"] is None
    assert retrieved["basal_rates"] is None
    assert retrieved["carb_ratios"] == {"default": 10.0}
    assert retrieved["insulin_sensitivity_factor"] == 2.0
    assert retrieved["correction_threshold"] == 8.0
    assert retrieved["exercise_correction_factor"] == 1.0
    assert retrieved["iob_calculation_method"] == "linear"
    assert retrieved["insulin_profile"] == {"name": "Humalog", "onsetMins": 10}


def test_db_settings_none_diabetic():
    user_sub = "test-user-none"
    settings = {
        "insulin_type": "None",
        "pump_brand_model": "Should be wiped",
        "basal_rates": {"ignored": 1.0},
        "carb_ratios": {"ignored": 10.0},
        "insulin_sensitivity_factor": 3.0,
        "correction_threshold": 9.0,
        "exercise_correction_factor": 1.5,
        "iob_calculation_method": "ignored"
    }
    
    upsert_user_settings(user_sub, settings)
    retrieved = get_user_settings(user_sub)
    
    assert retrieved is not None
    assert retrieved["user_sub"] == user_sub
    assert retrieved["insulin_type"] == "None"
    # All other insulin-specific fields must be forced to None/NULL
    assert retrieved["pump_brand_model"] is None
    assert retrieved["basal_rates"] is None
    assert retrieved["carb_ratios"] is None
    assert retrieved["insulin_sensitivity_factor"] is None
    assert retrieved["correction_threshold"] is None
    assert retrieved["exercise_correction_factor"] is None
    assert retrieved["iob_calculation_method"] is None
    assert retrieved["insulin_profile"] is None


def test_api_endpoints():
    client = TestClient(app)
    
    # Mock authentication override to return a predictable test user
    app.dependency_overrides[_require_user] = lambda: {
        "sub": "api-test-user",
        "email": "api-test@local",
        "name": "API Test User"
    }
    
    try:
        # 1. Fetch initial default settings
        res = client.get("/api/user/settings")
        assert res.status_code == 200
        data = res.json()
        assert data["user_sub"] == "api-test-user"
        assert data["insulin_type"] == "None"
        assert data["pump_brand_model"] is None
        assert data["insulin_profile"] is None
        
        # 2. Update settings via POST
        payload = {
            "insulin_type": "Pump",
            "pump_brand_model": "Tandem t:slim X2",
            "basal_rates": {"default": 1.2, "exercise": 0.8},
            "carb_ratios": {"default": 6.0},
            "insulin_sensitivity_factor": 1.8,
            "correction_threshold": 7.0,
            "exercise_correction_factor": 1.2,
            "iob_calculation_method": "exponential",
            "insulin_profile": {"name": "Fiasp", "onsetMins": 5}
        }
        res_post = client.post("/api/user/settings", json=payload)
        assert res_post.status_code == 200
        assert res_post.json() == {"status": "ok"}
        
        # 3. Retrieve updated settings and verify
        res_get = client.get("/api/user/settings")
        assert res_get.status_code == 200
        updated_data = res_get.json()
        assert updated_data["insulin_type"] == "Pump"
        assert updated_data["pump_brand_model"] == "Tandem t:slim X2"
        assert updated_data["basal_rates"] == {"default": 1.2, "exercise": 0.8}
        assert updated_data["carb_ratios"] == {"default": 6.0}
        assert updated_data["insulin_sensitivity_factor"] == 1.8
        assert updated_data["correction_threshold"] == 7.0
        assert updated_data["exercise_correction_factor"] == 1.2
        assert updated_data["iob_calculation_method"] == "exponential"
        assert updated_data["insulin_profile"] == {"name": "Fiasp", "onsetMins": 5}
        
    finally:
        # Clear dependency overrides
        app.dependency_overrides.clear()
