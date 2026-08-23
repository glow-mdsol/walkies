import json
from pathlib import Path

from .adapter import build_analytics_adapter

ANALYTICS_DB_PATH = Path(__file__).resolve().parent.parent / "analytics.sqlite3"
ANALYTICS_VERSION = "2026-05-05-v2"
ANALYTICS_ADAPTER = build_analytics_adapter(ANALYTICS_DB_PATH)


def analytics_conn():
    return ANALYTICS_ADAPTER.connect()


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
                summary_json TEXT NOT NULL,
                route_fingerprint_json TEXT,
                route_series_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_walk_analytics_order
                ON walk_analytics(start_time DESC, walk_date DESC, walk_id DESC);

            CREATE TABLE IF NOT EXISTS share_links (
                token TEXT PRIMARY KEY,
                owner_sub TEXT NOT NULL,
                walk_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT,
                revoked_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_share_links_owner_walk
                ON share_links(owner_sub, walk_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS user_settings (
                user_sub TEXT PRIMARY KEY,
                user_type TEXT,                      -- 'diabetic_pump', 'diabetic_mdi', 'non_diabetic_cgm', 'non_diabetic'
                insulin_type TEXT NOT NULL,          -- 'None', 'MDI', 'Pump'
                pump_brand_model TEXT,
                basal_rates_json TEXT,
                carb_ratios_json TEXT,
                insulin_sensitivity_factor REAL,
                correction_threshold REAL,
                exercise_correction_factor REAL,
                iob_calculation_method TEXT,
                insulin_profile_json TEXT,
                updated_at TEXT NOT NULL
            );
            """
        )
        # Idempotent migrations for older DB files that predate these columns.
        for col, typedef in [
            ("route_fingerprint_json", "TEXT"),
            ("route_series_json", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE walk_analytics ADD COLUMN {col} {typedef}")
            except Exception:
                pass  # column already exists
        try:
            conn.execute("ALTER TABLE user_settings ADD COLUMN user_type TEXT")
        except Exception:
            pass  # column already exists


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
    route_fingerprint: dict | None = None,
    route_series: dict | None = None,
) -> None:
    with analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO walk_analytics (
                walk_id, walk_date, walk_name, start_time, source_hash, analytics_version, computed_at,
                distance_km, duration_h, avg_hr, bg_delta, tir_pct, bolus_units,
                weather_stress_score, hr_decoupling_score, metrics_json, summary_json,
                route_fingerprint_json, route_series_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                summary_json = excluded.summary_json,
                route_fingerprint_json = excluded.route_fingerprint_json,
                route_series_json = excluded.route_series_json
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
                json.dumps(route_fingerprint) if route_fingerprint is not None else None,
                json.dumps(route_series) if route_series is not None else None,
            ),
        )


def delete_walk_analytics(walk_id: str) -> None:
    with analytics_conn() as conn:
        conn.execute("DELETE FROM walk_sources WHERE walk_id = ?", (walk_id,))
        conn.execute("DELETE FROM walk_analytics WHERE walk_id = ?", (walk_id,))


def analytics_row_to_dict(row) -> dict:
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
        "route_fingerprint": json.loads(row["route_fingerprint_json"]) if row["route_fingerprint_json"] else None,
        "route_series": json.loads(row["route_series_json"]) if row["route_series_json"] else None,
    }


def get_cached_analytics_row(walk_id: str) -> dict | None:
    with analytics_conn() as conn:
        row = conn.execute("SELECT * FROM walk_analytics WHERE walk_id = ?", (walk_id,)).fetchone()
    return analytics_row_to_dict(row) if row else None


def list_all_analytics_rows() -> list[dict]:
    with analytics_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM walk_analytics ORDER BY start_time DESC, walk_date DESC, walk_id DESC"
        ).fetchall()
    return [analytics_row_to_dict(row) for row in rows]


def create_share_link(token: str, owner_sub: str, walk_id: str, created_at: str, expires_at: str | None = None) -> None:
    with analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO share_links (token, owner_sub, walk_id, created_at, expires_at, revoked_at)
            VALUES (?, ?, ?, ?, ?, NULL)
            """,
            (token, owner_sub, walk_id, created_at, expires_at),
        )


def upsert_share_link(
    token: str,
    owner_sub: str,
    walk_id: str,
    created_at: str,
    expires_at: str | None = None,
    revoked_at: str | None = None,
) -> None:
    with analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO share_links (token, owner_sub, walk_id, created_at, expires_at, revoked_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(token) DO UPDATE SET
                owner_sub = excluded.owner_sub,
                walk_id = excluded.walk_id,
                created_at = excluded.created_at,
                expires_at = excluded.expires_at,
                revoked_at = excluded.revoked_at
            """,
            (token, owner_sub, walk_id, created_at, expires_at, revoked_at),
        )


def list_share_links_for_walk(owner_sub: str, walk_id: str) -> list[dict]:
    with analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT token, owner_sub, walk_id, created_at, expires_at, revoked_at
            FROM share_links
            WHERE owner_sub = ? AND walk_id = ?
            ORDER BY created_at DESC
            """,
            (owner_sub, walk_id),
        ).fetchall()
    return [dict(row) for row in rows]


def get_share_link(token: str) -> dict | None:
    with analytics_conn() as conn:
        row = conn.execute(
            """
            SELECT token, owner_sub, walk_id, created_at, expires_at, revoked_at
            FROM share_links
            WHERE token = ?
            """,
            (token,),
        ).fetchone()
    return dict(row) if row else None


def revoke_share_link(token: str, owner_sub: str, revoked_at: str) -> bool:
    with analytics_conn() as conn:
        cursor = conn.execute(
            """
            UPDATE share_links
            SET revoked_at = ?
            WHERE token = ? AND owner_sub = ? AND revoked_at IS NULL
            """,
            (revoked_at, token, owner_sub),
        )
        return cursor.rowcount > 0


def get_user_settings(user_sub: str) -> dict | None:
    with analytics_conn() as conn:
        row = conn.execute(
            """
            SELECT user_sub, user_type, insulin_type, pump_brand_model, basal_rates_json, carb_ratios_json,
                   insulin_sensitivity_factor, correction_threshold, exercise_correction_factor,
                   iob_calculation_method, insulin_profile_json, updated_at
            FROM user_settings
            WHERE user_sub = ?
            """,
            (user_sub,),
        ).fetchone()
    if not row:
        return None
    res = dict(row)
    res['basal_rates'] = json.loads(res['basal_rates_json']) if res.get('basal_rates_json') else None
    res['carb_ratios'] = json.loads(res['carb_ratios_json']) if res.get('carb_ratios_json') else None
    res['insulin_profile'] = json.loads(res['insulin_profile_json']) if res.get('insulin_profile_json') else None
    res.pop('basal_rates_json', None)
    res.pop('carb_ratios_json', None)
    res.pop('insulin_profile_json', None)
    return res


def upsert_user_settings(user_sub: str, settings: dict) -> None:
    from datetime import datetime, timezone
    
    user_type = settings.get("user_type")
    insulin_type = settings.get("insulin_type", "None")
    
    if not user_type:
        if insulin_type == "Pump":
            user_type = "diabetic_pump"
        elif insulin_type == "MDI":
            user_type = "diabetic_mdi"
        else:
            user_type = "non_diabetic"
            
    if user_type not in ("diabetic_pump", "diabetic_mdi", "non_diabetic_cgm", "non_diabetic"):
        user_type = "non_diabetic"

    # Enforce logical consistency for insulin_type based on user_type
    if user_type == "diabetic_pump":
        insulin_type = "Pump"
    elif user_type == "diabetic_mdi":
        insulin_type = "MDI"
    else:
        insulin_type = "None"
        
    if insulin_type == "None":
        pump_brand_model = None
        basal_rates_json = None
        carb_ratios_json = None
        insulin_sensitivity_factor = None
        correction_threshold = None
        exercise_correction_factor = None
        iob_calculation_method = None
        insulin_profile_json = None
    else:
        pump_brand_model = settings.get("pump_brand_model")
        if insulin_type == "MDI":
            basal_rates_json = None
        else:
            basal_rates_json = json.dumps(settings.get("basal_rates")) if settings.get("basal_rates") is not None else None
        carb_ratios_json = json.dumps(settings.get("carb_ratios")) if settings.get("carb_ratios") is not None else None
        insulin_sensitivity_factor = settings.get("insulin_sensitivity_factor")
        correction_threshold = settings.get("correction_threshold")
        exercise_correction_factor = settings.get("exercise_correction_factor")
        iob_calculation_method = settings.get("iob_calculation_method")
        insulin_profile_json = json.dumps(settings.get("insulin_profile")) if settings.get("insulin_profile") is not None else None

    updated_at = datetime.now(timezone.utc).isoformat()
    
    with analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO user_settings (
                user_sub, user_type, insulin_type, pump_brand_model, basal_rates_json, carb_ratios_json,
                insulin_sensitivity_factor, correction_threshold, exercise_correction_factor,
                iob_calculation_method, insulin_profile_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_sub) DO UPDATE SET
                user_type = excluded.user_type,
                insulin_type = excluded.insulin_type,
                pump_brand_model = excluded.pump_brand_model,
                basal_rates_json = excluded.basal_rates_json,
                carb_ratios_json = excluded.carb_ratios_json,
                insulin_sensitivity_factor = excluded.insulin_sensitivity_factor,
                correction_threshold = excluded.correction_threshold,
                exercise_correction_factor = excluded.exercise_correction_factor,
                iob_calculation_method = excluded.iob_calculation_method,
                insulin_profile_json = excluded.insulin_profile_json,
                updated_at = excluded.updated_at
            """,
            (
                user_sub,
                user_type,
                insulin_type,
                pump_brand_model,
                basal_rates_json,
                carb_ratios_json,
                insulin_sensitivity_factor,
                correction_threshold,
                exercise_correction_factor,
                iob_calculation_method,
                insulin_profile_json,
                updated_at,
            )
        )


def delete_user_data(user_sub: str, walk_ids: list[str]) -> None:
    with analytics_conn() as conn:
        conn.execute("DELETE FROM user_settings WHERE user_sub = ?", (user_sub,))
        conn.execute("DELETE FROM share_links WHERE owner_sub = ?", (user_sub,))
        for walk_id in walk_ids:
            conn.execute("DELETE FROM walk_sources WHERE walk_id = ?", (walk_id,))
            conn.execute("DELETE FROM walk_analytics WHERE walk_id = ?", (walk_id,))
