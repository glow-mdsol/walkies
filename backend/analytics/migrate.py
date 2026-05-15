import argparse
import sqlite3
from pathlib import Path

from .db import (
    ANALYTICS_ADAPTER,
    ANALYTICS_DB_PATH,
    init_analytics_db,
    upsert_share_link,
    upsert_walk_analytics,
    upsert_walk_sources,
)


def read_source_rows(source_path: Path) -> tuple[list[dict], list[dict], list[dict]]:
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
    finally:
        conn.close()
    return walk_sources, walk_analytics, share_links


def migrate_from_sqlite(source_path: Path) -> dict:
    if not source_path.exists():
        raise FileNotFoundError(f"Source sqlite DB not found: {source_path}")

    init_analytics_db()
    walk_sources, walk_analytics, share_links = read_source_rows(source_path)

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
            import json
            metrics = json.loads(metrics)
        if isinstance(summary, str):
            import json
            summary = json.loads(summary)

        route_fingerprint = row.get('route_fingerprint_json')
        route_series = row.get('route_series_json')
        if isinstance(route_fingerprint, str) and route_fingerprint:
            import json
            route_fingerprint = json.loads(route_fingerprint)
        if isinstance(route_series, str) and route_series:
            import json
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

    return {
        'target_backend': ANALYTICS_ADAPTER.kind,
        'source_path': str(source_path),
        'walk_source_rows': len(walk_sources),
        'walk_analytics_rows': len(walk_analytics),
        'share_link_rows': len(share_links),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description='Migrate analytics data from a legacy sqlite file into the configured analytics backend.')
    parser.add_argument(
        '--source',
        default=str(ANALYTICS_DB_PATH),
        help='Path to the source sqlite file. Defaults to backend/analytics.sqlite3',
    )
    args = parser.parse_args()

    result = migrate_from_sqlite(Path(args.source).resolve())
    print(
        'Migrated analytics data:',
        f"backend={result['target_backend']}",
        f"walk_sources={result['walk_source_rows']}",
        f"walk_analytics={result['walk_analytics_rows']}",
        f"share_links={result['share_link_rows']}",
        sep='\n  ',
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
