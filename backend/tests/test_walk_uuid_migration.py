import os
import sys
import json
import uuid
import pytest
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEST_DB_FILE = Path(__file__).resolve().parent / "test_analytics_migration.sqlite3"
os.environ['ANALYTICS_DB_URL'] = f"sqlite:///{TEST_DB_FILE.as_posix()}"
os.environ['WALKIES_DEV_BYPASS_AUTH'] = '1'

from fastapi.testclient import TestClient
import main
from main import app, _require_user, _user_data_dir, _find_walk_dir, _load_walk_meta
from analytics.db import (
    init_analytics_db,
    upsert_walk_analytics,
    upsert_walk_sources,
    create_share_link,
    get_cached_analytics_row,
    get_share_link,
)
from analytics.migrate import migrate_walk_ids_to_uuids, is_valid_uuid


@pytest.fixture(autouse=True)
def setup_test_db():
    if TEST_DB_FILE.exists():
        try:
            TEST_DB_FILE.unlink()
        except Exception:
            pass
    init_analytics_db()
    yield
    if TEST_DB_FILE.exists():
        try:
            TEST_DB_FILE.unlink()
        except Exception:
            pass


def test_is_valid_uuid():
    assert is_valid_uuid(str(uuid.uuid4())) is True
    assert is_valid_uuid("2026-08-23-101530-morning-walk") is False
    assert is_valid_uuid("invalid-uuid-string") is False
    assert is_valid_uuid("") is False
    assert is_valid_uuid(None) is False


def test_new_walk_upload_assigns_uuid(monkeypatch, tmp_path):
    client = TestClient(app)
    user_sub = "uuid-user-1"

    app.dependency_overrides[_require_user] = lambda: {
        "sub": user_sub,
        "email": "uuid1@local",
        "name": "UUID User 1",
    }
    main.DATA_DIR = tmp_path

    # Minimal dummy FIT file header
    header = bytearray(14)
    header[0] = 14
    header[1] = 0x20
    header[8:12] = b'.FIT'
    fit_bytes = bytes(header)

    monkeypatch.setattr(main, "_fit_start_datetime", lambda data: datetime(2026, 8, 25, 8, 0, 0, tzinfo=timezone.utc))
    monkeypatch.setattr(main, "_fit_file_identity", lambda data: "mock-fit-identity-uuid-1")
    monkeypatch.setattr(main, "refresh_walk_analytics_if_needed", lambda *args, **kwargs: None)

    response = client.post(
        "/api/walks/upload",
        data={"date": "2026-08-25", "name": "Morning Stroll"},
        files=[
            ("files", ("test.fit", fit_bytes, "application/octet-stream")),
            ("files", ("carelink.csv", b"dummy_carelink_csv", "text/csv")),
        ],
    )
    assert response.status_code == 200
    data = response.json()
    new_walk_id = data["id"]
    
    # Assert generated ID is a valid UUID
    assert is_valid_uuid(new_walk_id)
    
    user_dir = _user_data_dir(user_sub)
    walk_dir = user_dir / "2026-08-25" / new_walk_id
    assert walk_dir.exists()
    assert (walk_dir / "meta.json").exists()

    meta = json.loads((walk_dir / "meta.json").read_text(encoding="utf-8"))
    assert meta["id"] == new_walk_id
    assert meta["name"] == "Morning Stroll"


def test_migrate_existing_walks_to_uuids(tmp_path):
    main.DATA_DIR = tmp_path
    user_sub = "legacy-user-sub"
    user_dir = _user_data_dir(user_sub)

    # 1. Create legacy slug walks
    legacy_id_1 = "2026-08-20-091500-mountain-hike-abc123"
    legacy_dir_1 = user_dir / "2026-08-20" / legacy_id_1
    legacy_dir_1.mkdir(parents=True)
    (legacy_dir_1 / "activity.fit").write_bytes(b"dummy_fit_1")
    (legacy_dir_1 / "meta.json").write_text(json.dumps({
        "id": legacy_id_1,
        "date": "2026-08-20",
        "name": "Mountain Hike",
        "rating": 4,
    }), encoding="utf-8")

    legacy_id_2 = "2026-08-21-170000-evening-park-xyz789"
    legacy_dir_2 = user_dir / "2026-08-21" / legacy_id_2
    legacy_dir_2.mkdir(parents=True)
    (legacy_dir_2 / "activity.fit").write_bytes(b"dummy_fit_2")
    (legacy_dir_2 / "meta.json").write_text(json.dumps({
        "id": legacy_id_2,
        "date": "2026-08-21",
        "name": "Evening Park",
        "rating": 5,
    }), encoding="utf-8")

    # 2. Insert records in database with legacy IDs
    upsert_walk_sources(legacy_id_1, [{
        "filename": "activity.fit",
        "file_type": "fit",
        "size_bytes": 100,
        "modified_time": "2026-08-20T09:15:00Z",
        "content_hash": "hash-1",
    }])
    upsert_walk_analytics(
        {"id": legacy_id_1, "date": "2026-08-20", "name": "Mountain Hike", "start_time": "2026-08-20T09:15:00Z"},
        source_hash="source-hash-1",
        computed_at="2026-08-20T10:00:00Z",
        metrics={"distance_km": 5.2, "duration_h": 1.2, "avg_hr": 125},
        summary={"title": "Hike summary"},
    )
    create_share_link("share-token-legacy-1", user_sub, legacy_id_1, "2026-08-20T12:00:00Z")

    upsert_walk_analytics(
        {"id": legacy_id_2, "date": "2026-08-21", "name": "Evening Park", "start_time": "2026-08-21T17:00:00Z"},
        source_hash="source-hash-2",
        computed_at="2026-08-21T18:00:00Z",
        metrics={"distance_km": 3.1, "duration_h": 0.6, "avg_hr": 108},
        summary={"title": "Park summary"},
    )

    # 3. Run migration
    migration_result = migrate_walk_ids_to_uuids(tmp_path)
    assert migration_result["migrated_walks"] == 2

    # 4. Assert old directories no longer exist under old names
    assert not legacy_dir_1.exists()
    assert not legacy_dir_2.exists()

    # Find the new directories
    date_dir_1 = user_dir / "2026-08-20"
    date_dir_2 = user_dir / "2026-08-21"
    
    new_subdirs_1 = [d for d in date_dir_1.iterdir() if d.is_dir()]
    assert len(new_subdirs_1) == 1
    new_dir_1 = new_subdirs_1[0]
    new_uuid_1 = new_dir_1.name
    assert is_valid_uuid(new_uuid_1)

    new_subdirs_2 = [d for d in date_dir_2.iterdir() if d.is_dir()]
    assert len(new_subdirs_2) == 1
    new_dir_2 = new_subdirs_2[0]
    new_uuid_2 = new_dir_2.name
    assert is_valid_uuid(new_uuid_2)

    # 5. Check meta.json contains new UUID and legacy ID
    meta_1 = json.loads((new_dir_1 / "meta.json").read_text(encoding="utf-8"))
    assert meta_1["id"] == new_uuid_1
    assert meta_1["legacy_id"] == legacy_id_1
    assert meta_1["name"] == "Mountain Hike"

    meta_2 = json.loads((new_dir_2 / "meta.json").read_text(encoding="utf-8"))
    assert meta_2["id"] == new_uuid_2
    assert meta_2["legacy_id"] == legacy_id_2
    assert meta_2["name"] == "Evening Park"

    # 6. Check database tables were updated with the new UUIDs
    cached_1 = get_cached_analytics_row(new_uuid_1)
    assert cached_1 is not None
    assert cached_1["walk_id"] == new_uuid_1
    assert cached_1["name"] == "Mountain Hike"

    cached_2 = get_cached_analytics_row(new_uuid_2)
    assert cached_2 is not None
    assert cached_2["walk_id"] == new_uuid_2
    assert cached_2["name"] == "Evening Park"

    # Share link updated
    share_row = get_share_link("share-token-legacy-1")
    assert share_row is not None
    assert share_row["walk_id"] == new_uuid_1

    # 7. Check backward-compatible directory lookups
    assert _find_walk_dir(new_uuid_1, user_dir) == new_dir_1
    assert _find_walk_dir(legacy_id_1, user_dir) == new_dir_1

    # 8. Idempotency test: running migration again does nothing
    second_run = migrate_walk_ids_to_uuids(tmp_path)
    assert second_run["migrated_walks"] == 0
