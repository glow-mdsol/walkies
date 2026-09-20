import json
from datetime import datetime, timezone
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
                route_series_json TEXT,
                rating INTEGER DEFAULT 3
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
                garmin_email TEXT,
                carelink_email TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                level TEXT NOT NULL,
                category TEXT NOT NULL,
                service TEXT,
                message TEXT NOT NULL,
                details TEXT,
                user_sub TEXT,
                is_auth_issue INTEGER DEFAULT 0,
                is_connection_issue INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_system_logs_ts
                ON system_logs(timestamp DESC);

            CREATE INDEX IF NOT EXISTS idx_system_logs_cat
                ON system_logs(category, timestamp DESC);

            CREATE INDEX IF NOT EXISTS idx_system_logs_auth
                ON system_logs(is_auth_issue, timestamp DESC);

            CREATE INDEX IF NOT EXISTS idx_system_logs_conn
                ON system_logs(is_connection_issue, timestamp DESC);

            CREATE INDEX IF NOT EXISTS idx_system_logs_user
                ON system_logs(user_sub, timestamp DESC);
            """
        )
        # Idempotent migrations for older DB files that predate these columns.
        for col, typedef in [
            ("route_fingerprint_json", "TEXT"),
            ("route_series_json", "TEXT"),
            ("rating", "INTEGER DEFAULT 3"),
        ]:
            try:
                conn.execute(f"ALTER TABLE walk_analytics ADD COLUMN {col} {typedef}")
            except Exception:
                pass  # column already exists
        try:
            conn.execute("ALTER TABLE user_settings ADD COLUMN user_type TEXT")
        except Exception:
            pass  # column already exists
        try:
            conn.execute("ALTER TABLE user_settings ADD COLUMN garmin_email TEXT")
        except Exception:
            pass  # column already exists
        try:
            conn.execute("ALTER TABLE user_settings ADD COLUMN carelink_email TEXT")
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
                walk_id, walk_date, walk_name, rating, start_time, source_hash, analytics_version, computed_at,
                distance_km, duration_h, avg_hr, bg_delta, tir_pct, bolus_units,
                weather_stress_score, hr_decoupling_score, metrics_json, summary_json,
                route_fingerprint_json, route_series_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(walk_id) DO UPDATE SET
                walk_date = excluded.walk_date,
                walk_name = excluded.walk_name,
                rating = excluded.rating,
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
                walk_meta.get("rating", 3),
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
    row_dict = dict(row)
    return {
        "walk_id": row["walk_id"],
        "date": row["walk_date"],
        "name": row["walk_name"],
        "rating": row_dict.get("rating", 3),
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
                   iob_calculation_method, insulin_profile_json, garmin_email, carelink_email, updated_at
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
    
    if "garmin_email" in settings:
        garmin_email = settings["garmin_email"]
    else:
        existing = get_user_settings(user_sub)
        garmin_email = existing.get("garmin_email") if existing else None

    if "carelink_email" in settings:
        carelink_email = settings["carelink_email"]
    else:
        existing = get_user_settings(user_sub)
        carelink_email = existing.get("carelink_email") if existing else None

    with analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO user_settings (
                user_sub, user_type, insulin_type, pump_brand_model, basal_rates_json, carb_ratios_json,
                insulin_sensitivity_factor, correction_threshold, exercise_correction_factor,
                iob_calculation_method, insulin_profile_json, garmin_email, carelink_email, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                garmin_email = excluded.garmin_email,
                carelink_email = excluded.carelink_email,
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
                garmin_email,
                carelink_email,
                updated_at,
            )
        )


def delete_user_data(user_sub: str, walk_ids: list[str]) -> None:
    with analytics_conn() as conn:
        conn.execute("DELETE FROM user_settings WHERE user_sub = ?", (user_sub,))
        conn.execute("DELETE FROM share_links WHERE owner_sub = ?", (user_sub,))
        conn.execute("DELETE FROM system_logs WHERE user_sub = ?", (user_sub,))
        for walk_id in walk_ids:
            conn.execute("DELETE FROM walk_sources WHERE walk_id = ?", (walk_id,))
            conn.execute("DELETE FROM walk_analytics WHERE walk_id = ?", (walk_id,))


def update_walk_id_references(old_walk_id: str, new_walk_id: str) -> None:
    """
    Atomically updates all database records referencing old_walk_id to use new_walk_id.
    """
    with analytics_conn() as conn:
        conn.execute("UPDATE walk_sources SET walk_id = ? WHERE walk_id = ?", (new_walk_id, old_walk_id))
        conn.execute("UPDATE walk_analytics SET walk_id = ? WHERE walk_id = ?", (new_walk_id, old_walk_id))
        conn.execute("UPDATE share_links SET walk_id = ? WHERE walk_id = ?", (new_walk_id, old_walk_id))



def insert_system_log(
    level: str,
    category: str,
    message: str,
    service: str | None = None,
    details: str | dict | None = None,
    user_sub: str | None = None,
    is_auth_issue: bool = False,
    is_connection_issue: bool = False,
    timestamp: str | None = None,
) -> int:
    if timestamp is None:
        timestamp = datetime.now(timezone.utc).isoformat()
    if isinstance(details, (dict, list)):
        details_str = json.dumps(details)
    else:
        details_str = str(details) if details is not None else None

    with analytics_conn() as conn:
        cursor = conn.execute(
            """
            INSERT INTO system_logs (
                timestamp, level, category, service, message, details,
                user_sub, is_auth_issue, is_connection_issue
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                level.upper(),
                category.lower(),
                service.lower() if service else None,
                message,
                details_str,
                user_sub,
                1 if is_auth_issue else 0,
                1 if is_connection_issue else 0,
            ),
        )
        return cursor.lastrowid


def query_system_logs(
    category: str | None = None,
    auth_only: bool = False,
    exclude_connection: bool = False,
    is_connection: bool | None = None,
    level: str | None = None,
    service: str | None = None,
    search: str | None = None,
    user_sub: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict], int]:
    clauses = []
    params = []

    if user_sub is not None:
        # Include logs for this user OR system-wide logs (where user_sub is NULL)
        clauses.append("(user_sub = ? OR user_sub IS NULL)")
        params.append(user_sub)

    if auth_only:
        clauses.append("(is_auth_issue = 1 OR category = 'auth')")
    elif category and category.lower() != "all":
        clauses.append("category = ?")
        params.append(category.lower())

    if exclude_connection:
        clauses.append("is_connection_issue = 0")
    elif is_connection is not None:
        clauses.append("is_connection_issue = ?")
        params.append(1 if is_connection else 0)

    if level and level.upper() != "ALL":
        clauses.append("level = ?")
        params.append(level.upper())

    if service and service.lower() != "all":
        clauses.append("service = ?")
        params.append(service.lower())

    if search:
        search_pattern = f"%{search.strip()}%"
        clauses.append("(message LIKE ? OR details LIKE ? OR service LIKE ?)")
        params.extend([search_pattern, search_pattern, search_pattern])

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with analytics_conn() as conn:
        count_query = f"SELECT COUNT(*) FROM system_logs {where_sql}"
        total_count = conn.execute(count_query, params).fetchone()[0]

        query = f"""
            SELECT id, timestamp, level, category, service, message, details,
                   user_sub, is_auth_issue, is_connection_issue
            FROM system_logs
            {where_sql}
            ORDER BY timestamp DESC, id DESC
            LIMIT ? OFFSET ?
        """
        rows = conn.execute(query, params + [limit, offset]).fetchall()

        logs = []
        for r in rows:
            parsed_details = None
            if r[6]:
                try:
                    parsed_details = json.loads(r[6])
                except Exception:
                    parsed_details = r[6]
            logs.append({
                "id": r[0],
                "timestamp": r[1],
                "level": r[2],
                "category": r[3],
                "service": r[4],
                "message": r[5],
                "details": parsed_details,
                "user_sub": r[7],
                "is_auth_issue": bool(r[8]),
                "is_connection_issue": bool(r[9]),
            })

        return logs, total_count


def get_logs_summary(user_sub: str | None = None) -> dict:
    clauses = []
    params = []
    if user_sub is not None:
        clauses.append("(user_sub = ? OR user_sub IS NULL)")
        params.append(user_sub)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with analytics_conn() as conn:
        total = conn.execute(f"SELECT COUNT(*) FROM system_logs {where_sql}", params).fetchone()[0]
        
        auth_where = f"WHERE (is_auth_issue = 1 OR category = 'auth') {'AND ' + ' AND '.join(clauses) if clauses else ''}"
        auth_count = conn.execute(f"SELECT COUNT(*) FROM system_logs {auth_where}", params).fetchone()[0]

        conn_where = f"WHERE is_connection_issue = 1 {'AND ' + ' AND '.join(clauses) if clauses else ''}"
        connection_count = conn.execute(f"SELECT COUNT(*) FROM system_logs {conn_where}", params).fetchone()[0]

        err_where = f"WHERE level = 'ERROR' {'AND ' + ' AND '.join(clauses) if clauses else ''}"
        error_count = conn.execute(f"SELECT COUNT(*) FROM system_logs {err_where}", params).fetchone()[0]

        warn_where = f"WHERE level = 'WARNING' {'AND ' + ' AND '.join(clauses) if clauses else ''}"
        warning_count = conn.execute(f"SELECT COUNT(*) FROM system_logs {warn_where}", params).fetchone()[0]

        return {
            "total_logs": total,
            "auth_issues_count": auth_count,
            "connection_issues_count": connection_count,
            "error_count": error_count,
            "warning_count": warning_count,
        }


def clear_system_logs(user_sub: str | None = None, before_timestamp: str | None = None) -> int:
    clauses = []
    params = []
    if user_sub is not None:
        clauses.append("user_sub = ?")
        params.append(user_sub)
    if before_timestamp is not None:
        clauses.append("timestamp < ?")
        params.append(before_timestamp)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with analytics_conn() as conn:
        cursor = conn.execute(f"DELETE FROM system_logs {where_sql}", params)
        return cursor.rowcount

