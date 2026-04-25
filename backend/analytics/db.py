import json
import sqlite3
from pathlib import Path


ANALYTICS_DB_PATH = Path(__file__).resolve().parent.parent / "analytics.sqlite3"
ANALYTICS_VERSION = "2026-04-25-v1"


def analytics_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(ANALYTICS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_analytics_db() -> None:
    with analytics_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS walk_sources (
                walk_id TEXT NOT NULL,
                filename TEXT NOT NULL,
                file_type TEXT,
                size_bytes INTEGER NOT NULL,
                modified_time TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                PRIMARY KEY (walk_id, filename)
            );

            CREATE TABLE IF NOT EXISTS walk_analytics (
                walk_id TEXT PRIMARY KEY,
                walk_date TEXT NOT NULL,
                walk_name TEXT,
                start_time TEXT,
                source_hash TEXT NOT NULL,
                analytics_version TEXT NOT NULL,
                computed_at TEXT NOT NULL,
                distance_km REAL,
                duration_h REAL,
                avg_hr REAL,
                bg_delta REAL,
                tir_pct REAL,
                bolus_units REAL,
                weather_stress_score REAL,
                hr_decoupling_score REAL,
                metrics_json TEXT NOT NULL,
                summary_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_walk_analytics_order
                ON walk_analytics(start_time DESC, walk_date DESC, walk_id DESC);
            """
        )


def upsert_walk_sources(walk_id: str, source_rows: list[dict]) -> None:
    with analytics_conn() as conn:
        conn.execute("DELETE FROM walk_sources WHERE walk_id = ?", (walk_id,))
        conn.executemany(
            """
            INSERT INTO walk_sources (walk_id, filename, file_type, size_bytes, modified_time, content_hash)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    walk_id,
                    row["filename"],
                    row["file_type"],
                    row["size_bytes"],
                    row["modified_time"],
                    row["content_hash"],
                )
                for row in source_rows
            ],
        )


def upsert_walk_analytics(
    walk_meta: dict,
    source_hash: str,
    computed_at: str,
    metrics: dict,
    summary: dict,
) -> None:
    with analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO walk_analytics (
                walk_id, walk_date, walk_name, start_time, source_hash, analytics_version, computed_at,
                distance_km, duration_h, avg_hr, bg_delta, tir_pct, bolus_units,
                weather_stress_score, hr_decoupling_score, metrics_json, summary_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(walk_id) DO UPDATE SET
                walk_date = excluded.walk_date,
                walk_name = excluded.walk_name,
                start_time = excluded.start_time,
                source_hash = excluded.source_hash,
                analytics_version = excluded.analytics_version,
                computed_at = excluded.computed_at,
                distance_km = excluded.distance_km,
                duration_h = excluded.duration_h,
                avg_hr = excluded.avg_hr,
                bg_delta = excluded.bg_delta,
                tir_pct = excluded.tir_pct,
                bolus_units = excluded.bolus_units,
                weather_stress_score = excluded.weather_stress_score,
                hr_decoupling_score = excluded.hr_decoupling_score,
                metrics_json = excluded.metrics_json,
                summary_json = excluded.summary_json
            """,
            (
                walk_meta["id"],
                walk_meta["date"],
                walk_meta.get("name") or None,
                walk_meta.get("start_time"),
                source_hash,
                ANALYTICS_VERSION,
                computed_at,
                metrics.get("distance_km"),
                metrics.get("duration_h"),
                metrics.get("avg_hr"),
                metrics.get("bg_delta"),
                metrics.get("tir_pct"),
                metrics.get("bolus_units"),
                metrics.get("weather_stress_score"),
                metrics.get("hr_decoupling_score"),
                json.dumps(metrics),
                json.dumps(summary),
            ),
        )


def delete_walk_analytics(walk_id: str) -> None:
    with analytics_conn() as conn:
        conn.execute("DELETE FROM walk_sources WHERE walk_id = ?", (walk_id,))
        conn.execute("DELETE FROM walk_analytics WHERE walk_id = ?", (walk_id,))


def analytics_row_to_dict(row: sqlite3.Row) -> dict:
    return {
        "walk_id": row["walk_id"],
        "date": row["walk_date"],
        "name": row["walk_name"],
        "start_time": row["start_time"],
        "source_hash": row["source_hash"],
        "analytics_version": row["analytics_version"],
        "computed_at": row["computed_at"],
        "distance_km": row["distance_km"],
        "duration_h": row["duration_h"],
        "avg_hr": row["avg_hr"],
        "bg_delta": row["bg_delta"],
        "tir_pct": row["tir_pct"],
        "bolus_units": row["bolus_units"],
        "weather_stress_score": row["weather_stress_score"],
        "hr_decoupling_score": row["hr_decoupling_score"],
        "metrics": json.loads(row["metrics_json"]),
        "summary": json.loads(row["summary_json"]),
    }


def get_cached_analytics_row(walk_id: str) -> dict | None:
    with analytics_conn() as conn:
        row = conn.execute("SELECT * FROM walk_analytics WHERE walk_id = ?", (walk_id,)).fetchone()
    return analytics_row_to_dict(row) if row else None