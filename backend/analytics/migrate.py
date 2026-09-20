import argparse
import json
import sqlite3
import uuid
from pathlib import Path

from .db import (
    ANALYTICS_ADAPTER,
    ANALYTICS_DB_PATH,
    init_analytics_db,
    upsert_share_link,
    upsert_walk_analytics,
    upsert_walk_sources,
    upsert_user_settings,
    update_walk_id_references,
)


def is_valid_uuid(val: str) -> bool:
    try:
        uuid.UUID(str(val))
        return True
    except (ValueError, TypeError, AttributeError):
        return False


def _find_leaf_walk_dirs(root: Path) -> list[Path]:
    """Find all walk directories (directories that directly contain walk files or meta.json)."""
    walk_dirs: list[Path] = []
    if not root.exists() or not root.is_dir():
        return walk_dirs

    for item in root.rglob("meta.json"):
        if item.is_file():
            walk_dirs.append(item.parent)

    # Also check directories containing .fit or .csv files without a meta.json
    for fit_file in root.rglob("*.fit"):
        parent = fit_file.parent
        if parent not in walk_dirs:
            walk_dirs.append(parent)

    return sorted(list(set(walk_dirs)), key=lambda p: str(p))


def migrate_walk_ids_to_uuids(data_root: Path | None = None) -> dict:
    """
    Migrates all existing walk directories and database records from legacy slug names
    to UUIDs. Preserves legacy IDs in meta.json.
    """
    if data_root is None:
        data_root = Path(__file__).resolve().parent.parent / "data"

    init_analytics_db()
    leaf_dirs = _find_leaf_walk_dirs(data_root)
    migrations = []

    for walk_dir in leaf_dirs:
        dir_name = walk_dir.name
        meta_file = walk_dir / "meta.json"
        meta = {}
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
            except Exception:
                meta = {}

        current_id = meta.get("id") or dir_name

        # If already a valid UUID and directory name matches, ensure metadata consistency
        if is_valid_uuid(dir_name):
            if meta and meta.get("id") != dir_name:
                meta["id"] = dir_name
                meta_file.write_text(json.dumps(meta), encoding="utf-8")
            continue

        # Non-UUID walk identified: assign new UUID
        new_uuid = str(uuid.uuid4())
        old_id = dir_name
        old_meta_id = meta.get("id") or old_id

        meta["id"] = new_uuid
        meta["legacy_id"] = old_meta_id

        target_dir = walk_dir.parent / new_uuid
        walk_dir.rename(target_dir)

        target_meta_file = target_dir / "meta.json"
        target_meta_file.write_text(json.dumps(meta), encoding="utf-8")

        # Update database references atomically
        update_walk_id_references(old_id, new_uuid)
        if old_meta_id != old_id:
            update_walk_id_references(old_meta_id, new_uuid)

        migrations.append({
            "old_id": old_id,
            "new_id": new_uuid,
            "path": str(target_dir),
        })

    return {
        "migrated_walks": len(migrations),
        "migrations": migrations,
    }


def read_source_rows(source_path: Path) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    conn = sqlite3.connect(source_path)
    conn.row_factory = sqlite3.Row
    try:
        tables = {
            row['name']
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }

        walk_sources = [dict(row) for row in conn.execute(
            "SELECT walk_id, filename, file_type, size_bytes, modified_time, content_hash FROM walk_sources"
        ).fetchall()]

        analytics_columns = {
            row['name']
            for row in conn.execute("PRAGMA table_info(walk_analytics)").fetchall()
        }
        route_fingerprint_select = 'route_fingerprint_json' if 'route_fingerprint_json' in analytics_columns else 'NULL AS route_fingerprint_json'
        route_series_select = 'route_series_json' if 'route_series_json' in analytics_columns else 'NULL AS route_series_json'

        walk_analytics = [dict(row) for row in conn.execute(
            f"""
            SELECT walk_id, walk_date, walk_name, start_time, source_hash, analytics_version, computed_at,
                   distance_km, duration_h, avg_hr, bg_delta, tir_pct, bolus_units,
                   weather_stress_score, hr_decoupling_score, metrics_json, summary_json,
                   {route_fingerprint_select}, {route_series_select}
            FROM walk_analytics
            """
        ).fetchall()]

        if 'share_links' in tables:
            share_links = [dict(row) for row in conn.execute(
                "SELECT token, owner_sub, walk_id, created_at, expires_at, revoked_at FROM share_links"
            ).fetchall()]
        else:
            share_links = []

        if 'user_settings' in tables:
            try:
                user_settings = [dict(row) for row in conn.execute(
                    "SELECT user_sub, user_type, insulin_type, pump_brand_model, basal_rates_json, carb_ratios_json, insulin_sensitivity_factor, correction_threshold, exercise_correction_factor, iob_calculation_method, insulin_profile_json, updated_at FROM user_settings"
                ).fetchall()]
            except Exception:
                user_settings = [dict(row) for row in conn.execute(
                    "SELECT user_sub, insulin_type, pump_brand_model, basal_rates_json, carb_ratios_json, insulin_sensitivity_factor, correction_threshold, exercise_correction_factor, iob_calculation_method, insulin_profile_json, updated_at FROM user_settings"
                ).fetchall()]
        else:
            user_settings = []
    finally:
        conn.close()
    return walk_sources, walk_analytics, share_links, user_settings


def migrate_from_sqlite(source_path: Path) -> dict:
    if not source_path.exists():
        raise FileNotFoundError(f"Source sqlite DB not found: {source_path}")

    init_analytics_db()
    walk_sources, walk_analytics, share_links, user_settings = read_source_rows(source_path)

    sources_by_walk: dict[str, list[dict]] = {}
    for row in walk_sources:
        sources_by_walk.setdefault(row['walk_id'], []).append(row)

    for row in walk_analytics:
        walk_id = row['walk_id']
        source_rows = sources_by_walk.get(walk_id, [])
        if source_rows:
            upsert_walk_sources(walk_id, source_rows)

        metrics = row['metrics_json']
        summary = row['summary_json']
        if isinstance(metrics, str):
            metrics = json.loads(metrics)
        if isinstance(summary, str):
            summary = json.loads(summary)

        route_fingerprint = row.get('route_fingerprint_json')
        route_series = row.get('route_series_json')
        if isinstance(route_fingerprint, str) and route_fingerprint:
            route_fingerprint = json.loads(route_fingerprint)
        if isinstance(route_series, str) and route_series:
            route_series = json.loads(route_series)

        upsert_walk_analytics(
            {
                'id': walk_id,
                'date': row['walk_date'],
                'name': row['walk_name'],
                'start_time': row['start_time'],
            },
            row['source_hash'],
            row['computed_at'],
            metrics,
            summary,
            route_fingerprint=route_fingerprint,
            route_series=route_series,
        )

    for row in share_links:
        upsert_share_link(
            token=row['token'],
            owner_sub=row['owner_sub'],
            walk_id=row['walk_id'],
            created_at=row['created_at'],
            expires_at=row.get('expires_at'),
            revoked_at=row.get('revoked_at'),
        )

    for row in user_settings:
        basal_rates = json.loads(row['basal_rates_json']) if row.get('basal_rates_json') else None
        carb_ratios = json.loads(row['carb_ratios_json']) if row.get('carb_ratios_json') else None
        insulin_profile = json.loads(row['insulin_profile_json']) if row.get('insulin_profile_json') else None
        upsert_user_settings(
            user_sub=row['user_sub'],
            settings={
                'user_type': row.get('user_type'),
                'insulin_type': row['insulin_type'],
                'pump_brand_model': row.get('pump_brand_model'),
                'basal_rates': basal_rates,
                'carb_ratios': carb_ratios,
                'insulin_sensitivity_factor': row.get('insulin_sensitivity_factor'),
                'correction_threshold': row.get('correction_threshold'),
                'exercise_correction_factor': row.get('exercise_correction_factor'),
                'iob_calculation_method': row.get('iob_calculation_method'),
                'insulin_profile': insulin_profile,
            }
        )

    return {
        'target_backend': ANALYTICS_ADAPTER.kind,
        'source_path': str(source_path),
        'walk_source_rows': len(walk_sources),
        'walk_analytics_rows': len(walk_analytics),
        'share_link_rows': len(share_links),
        'user_settings_rows': len(user_settings),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='Analytics migration tool')
    parser.add_argument(
        '--source',
        default=str(ANALYTICS_DB_PATH),
        help='Path to the source sqlite file. Defaults to backend/analytics.sqlite3',
    )
    parser.add_argument(
        '--migrate-walk-uuids',
        action='store_true',
        help='Migrate all existing walk directories and database records to UUIDs',
    )
    parser.add_argument(
        '--data-dir',
        default=None,
        help='Path to the data directory to migrate walk UUIDs',
    )
    args = parser.parse_args()

    if args.migrate_walk_uuids:
        data_root = Path(args.data_dir).resolve() if args.data_dir else None
        res = migrate_walk_ids_to_uuids(data_root)
        print(f"Migrated {res['migrated_walks']} walk(s) to UUIDs.")
        for item in res['migrations']:
            print(f"  {item['old_id']} -> {item['new_id']}")
        return 0

    result = migrate_from_sqlite(Path(args.source).resolve())
    print(
        'Migrated analytics data:',
        f"backend={result['target_backend']}",
        f"walk_sources={result['walk_source_rows']}",
        f"walk_analytics={result['walk_analytics_rows']}",
        f"share_links={result['share_link_rows']}",
        f"user_settings={result['user_settings_rows']}",
        sep='\n  ',
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

