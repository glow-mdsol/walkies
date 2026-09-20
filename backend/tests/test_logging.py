import os
import sys
from pathlib import Path
import pytest

# Resolve import paths by adding the backend directory to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DB_FILE = Path(__file__).resolve().parent / "test_analytics_logging.sqlite3"
os.environ['ANALYTICS_DB_URL'] = f"sqlite:///{TEST_DB_FILE.as_posix()}"
os.environ['WALKIES_DEV_BYPASS_AUTH'] = '1'

from fastapi.testclient import TestClient
from analytics.db import (
    init_analytics_db,
    insert_system_log,
    query_system_logs,
    get_logs_summary,
    clear_system_logs,
    analytics_conn,
)
from analytics.logging_service import classify_log_event, log_system_event
import main
from main import app



@pytest.fixture(autouse=True)
def setup_test_db(monkeypatch, tmp_path):
    monkeypatch.setenv("WALKIES_DEV_BYPASS_AUTH", "1")
    main.DEV_BYPASS_AUTH = True

    test_db_path = tmp_path / "test_analytics.sqlite3"
    monkeypatch.setattr("analytics.db.ANALYTICS_DB_PATH", test_db_path)
    from analytics.adapter import build_analytics_adapter
    monkeypatch.setattr("analytics.db.ANALYTICS_ADAPTER", build_analytics_adapter(test_db_path))

    init_analytics_db()
    clear_system_logs()
    yield


def test_classify_log_event_auth_issues():
    # 401 Unauthorized
    cat, is_auth, is_conn = classify_log_event("HTTP 401 Unauthorized: Invalid session token")
    assert is_auth is True
    assert is_conn is False
    assert cat == "auth"

    # Garmin login / MFA failure
    cat, is_auth, is_conn = classify_log_event("Garmin MFA authentication timed out completing MFA authentication for user sub: u123")
    assert is_auth is True
    assert cat == "auth"

    # CareLink invalid credentials
    cat, is_auth, is_conn = classify_log_event("CareLink session invalid. Please re-authenticate.")
    assert is_auth is True
    assert cat == "auth"


def test_classify_log_event_connection_issues():
    # 502 Bad gateway
    cat, is_auth, is_conn = classify_log_event("Garmin API call failed: 502 Bad Gateway")
    assert is_conn is True
    assert is_auth is False
    assert cat == "connection"

    # Socket / Timeout error
    cat, is_auth, is_conn = classify_log_event("Connection timed out after 30000ms connecting to remote server")
    assert is_conn is True
    assert cat == "connection"

    # Rate limiting 429
    cat, is_auth, is_conn = classify_log_event("HTTP 429 Too Many Requests: Rate limit exceeded")
    assert is_conn is True
    assert cat == "connection"


def test_insert_and_query_system_logs():
    # Insert multiple distinct log entries
    insert_system_log(
        level="ERROR",
        category="auth",
        service="garmin",
        message="Garmin login failed: Invalid credentials",
        user_sub="user_1",
        is_auth_issue=True,
        is_connection_issue=False,
    )
    insert_system_log(
        level="WARNING",
        category="connection",
        service="garmin",
        message="Garmin API timeout: 504 Gateway Timeout",
        user_sub="user_1",
        is_auth_issue=False,
        is_connection_issue=True,
    )
    insert_system_log(
        level="INFO",
        category="sync",
        service="carelink",
        message="CareLink sync completed: 45 readings imported",
        user_sub="user_1",
        is_auth_issue=False,
        is_connection_issue=False,
    )

    # 1. Query all logs
    all_logs, count = query_system_logs(user_sub="user_1")
    assert count == 3
    assert len(all_logs) == 3

    # 2. Filter by auth_only (Authentication Issues)
    auth_logs, auth_count = query_system_logs(user_sub="user_1", auth_only=True)
    assert auth_count == 1
    assert len(auth_logs) == 1
    assert auth_logs[0]["is_auth_issue"] is True
    assert "Invalid credentials" in auth_logs[0]["message"]

    # 3. Filter by connection issues (Connection Issues)
    conn_logs, conn_count = query_system_logs(user_sub="user_1", is_connection=True)
    assert conn_count == 1
    assert len(conn_logs) == 1
    assert conn_logs[0]["is_connection_issue"] is True
    assert "504 Gateway Timeout" in conn_logs[0]["message"]

    # 4. Filter excluding connection issues
    non_conn_logs, non_conn_count = query_system_logs(user_sub="user_1", exclude_connection=True)
    assert non_conn_count == 2

    # 5. Check Summary
    summary = get_logs_summary(user_sub="user_1")
    assert summary["total_logs"] == 3
    assert summary["auth_issues_count"] == 1
    assert summary["connection_issues_count"] == 1
    assert summary["error_count"] == 1
    assert summary["warning_count"] == 1

    # 6. Clear logs
    cleared = clear_system_logs(user_sub="user_1")
    assert cleared == 3
    remaining, rem_count = query_system_logs(user_sub="user_1")
    assert rem_count == 0


def test_system_logs_api_endpoints():
    client = TestClient(app)

    # 1. Post a test log event via API
    resp = client.post(
        "/api/system/logs/test",
        json={
            "level": "ERROR",
            "message": "OAuth token expired for user session (401)",
            "service": "google_auth",
            "details": {"error_code": "TOKEN_EXPIRED"},
        },
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

    # Post another connection event
    resp = client.post(
        "/api/system/logs/test",
        json={
            "level": "WARNING",
            "message": "Connection error: Temporary DNS failure (503)",
            "service": "garmin",
        },
    )
    assert resp.status_code == 200

    # 2. Get logs
    get_resp = client.get("/api/system/logs")
    assert get_resp.status_code == 200
    data = get_resp.json()
    assert data["total"] == 2
    assert len(data["logs"]) == 2
    assert data["summary"]["auth_issues_count"] == 1
    assert data["summary"]["connection_issues_count"] == 1

    # 3. Get logs filtered by auth_only
    auth_resp = client.get("/api/system/logs?auth_only=true")
    assert auth_resp.status_code == 200
    auth_data = auth_resp.json()
    assert auth_data["total"] == 1
    assert "OAuth token expired" in auth_data["logs"][0]["message"]
    assert auth_data["logs"][0]["is_auth_issue"] is True

    # 4. Get logs filtered by connection issues
    conn_resp = client.get("/api/system/logs?is_connection=true")
    assert conn_resp.status_code == 200
    conn_data = conn_resp.json()
    assert conn_data["total"] == 1
    assert "Connection error" in conn_data["logs"][0]["message"]
    assert conn_data["logs"][0]["is_connection_issue"] is True

    # 5. Clear logs
    clear_resp = client.post("/api/system/logs/clear", json={})
    assert clear_resp.status_code == 200
    assert clear_resp.json()["cleared_count"] == 2

    # Verify empty
    get_after = client.get("/api/system/logs")
    assert get_after.json()["total"] == 0
