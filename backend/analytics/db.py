import json
from datetime import datetime, timezone
from pathlib import Path

from .adapter import build_analytics_adapter

ANALYTICS_DB_PATH = Path(__file__).resolve().parent.parent / "analytics.sqlite3"
ANALYTICS_VERSION = "2026-10-22-v3"
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
                share_type TEXT NOT NULL DEFAULT 'walk',
                target_start_date TEXT,
                target_end_date TEXT,
                created_at TEXT NOT NULL,
                expires_at TEXT,
                revoked_at TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_share_links_owner_walk
                ON share_links(owner_sub, walk_id, created_at DESC);

            CREATE TABLE IF NOT EXISTS user_settings (
                user_sub TEXT PRIMARY KEY,
                user_type TEXT,                      -- 'diabetic_closed_loop', 'diabetic_pump', 'diabetic_mdi', 'diabetic_lifestyle', 'non_diabetic_cgm', 'non_diabetic'
                insulin_type TEXT NOT NULL,          -- 'ClosedLoopPump', 'Pump', 'MDI', 'None'
                glucose_unit TEXT NOT NULL DEFAULT 'mmol/L',
                target_low_mmol REAL NOT NULL DEFAULT 4.0,
                target_high_mmol REAL NOT NULL DEFAULT 8.0,
                hypo_threshold_mmol REAL NOT NULL DEFAULT 3.9,
                hyper_threshold_mmol REAL NOT NULL DEFAULT 12.9,
                pump_brand_model TEXT,
                cgm_provider TEXT DEFAULT 'carelink',
                basal_rates_json TEXT,
                carb_ratios_json TEXT,
                insulin_sensitivity_factor REAL,
                correction_threshold REAL,
                exercise_correction_factor REAL,
                iob_calculation_method TEXT,
                insulin_profile_json TEXT,
                garmin_email TEXT,
                carelink_email TEXT,
                dexcom_username TEXT,
                dexcom_region TEXT DEFAULT 'ous',
                last_garmin_sync_at TEXT,
                last_cgm_sync_at TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS activity_epochs (
                user_sub TEXT NOT NULL,
                epoch_start_utc TEXT NOT NULL,
                epoch_end_utc TEXT NOT NULL,
                local_date TEXT NOT NULL,
                tz_offset_mins INTEGER NOT NULL DEFAULT 0,
                steps INTEGER NOT NULL DEFAULT 0,
                activity_category TEXT NOT NULL DEFAULT 'sedentary',
                intensity_level TEXT DEFAULT 'sedentary',
                distance_m REAL DEFAULT 0.0,
                active_kcal REAL DEFAULT 0.0,
                mean_hr REAL,
                max_hr REAL,
                linked_walk_id TEXT,
                source_provider TEXT NOT NULL DEFAULT 'garmin',
                ingested_at TEXT NOT NULL,
                PRIMARY KEY (user_sub, epoch_start_utc)
            );

            CREATE INDEX IF NOT EXISTS idx_activity_epochs_user_date
                ON activity_epochs(user_sub, local_date, epoch_start_utc);

            CREATE TABLE IF NOT EXISTS daily_wellness_summaries (
                user_sub TEXT NOT NULL,
                local_date TEXT NOT NULL,
                total_steps INTEGER NOT NULL DEFAULT 0,
                total_distance_m REAL DEFAULT 0.0,
                moderate_intensity_mins INTEGER DEFAULT 0,
                vigorous_intensity_mins INTEGER DEFAULT 0,
                resting_hr REAL,
                min_hr REAL,
                max_hr REAL,
                active_kcal REAL DEFAULT 0.0,
                raw_summary_json TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (user_sub, local_date)
            );

            CREATE TABLE IF NOT EXISTS glycemic_events (
                user_sub TEXT NOT NULL,
                timestamp_utc TEXT NOT NULL,
                event_type TEXT NOT NULL,
                local_date TEXT NOT NULL,
                tz_offset_mins INTEGER NOT NULL DEFAULT 0,
                bg_mmol REAL,
                trend_arrow TEXT,
                bolus_units REAL,
                basal_rate_u_hr REAL,
                carb_grams REAL,
                pump_state TEXT,
                source_provider TEXT NOT NULL,
                ingested_at TEXT NOT NULL,
                PRIMARY KEY (user_sub, timestamp_utc, event_type)
            );

            CREATE INDEX IF NOT EXISTS idx_glycemic_events_user_date
                ON glycemic_events(user_sub, local_date, timestamp_utc);

            CREATE TABLE IF NOT EXISTS daily_analytics (
                user_sub TEXT NOT NULL,
                local_date TEXT NOT NULL,
                source_hash TEXT NOT NULL,
                analytics_version TEXT NOT NULL,
                computed_at TEXT NOT NULL,
                total_steps INTEGER NOT NULL DEFAULT 0,
                active_minutes INTEGER NOT NULL DEFAULT 0,
                tir_pct REAL,
                tbr_pct REAL,
                tar_pct REAL,
                avg_bg_mmol REAL,
                bg_cv_pct REAL,
                hypos_count INTEGER NOT NULL DEFAULT 0,
                bolus_total_units REAL DEFAULT 0.0,
                carb_total_grams REAL DEFAULT 0.0,
                sensor_coverage_pct REAL DEFAULT 0.0,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (user_sub, local_date)
            );

            CREATE INDEX IF NOT EXISTS idx_daily_analytics_user_date
                ON daily_analytics(user_sub, local_date DESC);

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

        for col, typedef in [
            ("share_type", "TEXT NOT NULL DEFAULT 'walk'"),
            ("target_start_date", "TEXT"),
            ("target_end_date", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE share_links ADD COLUMN {col} {typedef}")
            except Exception:
                pass

        for col, typedef in [
            ("user_type", "TEXT"),
            ("glucose_unit", "TEXT NOT NULL DEFAULT 'mmol/L'"),
            ("target_low_mmol", "REAL NOT NULL DEFAULT 4.0"),
            ("target_high_mmol", "REAL NOT NULL DEFAULT 8.0"),
            ("hypo_threshold_mmol", "REAL NOT NULL DEFAULT 3.9"),
            ("hyper_threshold_mmol", "REAL NOT NULL DEFAULT 12.9"),
            ("cgm_provider", "TEXT DEFAULT 'carelink'"),
            ("garmin_email", "TEXT"),
            ("carelink_email", "TEXT"),
            ("dexcom_username", "TEXT"),
            ("dexcom_region", "TEXT DEFAULT 'ous'"),
            ("last_garmin_sync_at", "TEXT"),
            ("last_cgm_sync_at", "TEXT"),
        ]:
            try:
                conn.execute(f"ALTER TABLE user_settings ADD COLUMN {col} {typedef}")
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


def create_share_link(
    token: str,
    owner_sub: str,
    walk_id: str,
    created_at: str,
    expires_at: str | None = None,
    share_type: str = "walk",
    target_start_date: str | None = None,
    target_end_date: str | None = None,
) -> None:
    with analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO share_links (
                token, owner_sub, walk_id, share_type, target_start_date, target_end_date, created_at, expires_at, revoked_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)
            """,
            (token, owner_sub, walk_id, share_type, target_start_date, target_end_date, created_at, expires_at),
        )


def upsert_share_link(
    token: str,
    owner_sub: str,
    walk_id: str,
    created_at: str,
    expires_at: str | None = None,
    revoked_at: str | None = None,
    share_type: str = "walk",
    target_start_date: str | None = None,
    target_end_date: str | None = None,
) -> None:
    with analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO share_links (
                token, owner_sub, walk_id, share_type, target_start_date, target_end_date, created_at, expires_at, revoked_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(token) DO UPDATE SET
                owner_sub = excluded.owner_sub,
                walk_id = excluded.walk_id,
                share_type = excluded.share_type,
                target_start_date = excluded.target_start_date,
                target_end_date = excluded.target_end_date,
                created_at = excluded.created_at,
                expires_at = excluded.expires_at,
                revoked_at = excluded.revoked_at
            """,
            (token, owner_sub, walk_id, share_type, target_start_date, target_end_date, created_at, expires_at, revoked_at),
        )


def list_share_links_for_walk(owner_sub: str, walk_id: str) -> list[dict]:
    with analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT token, owner_sub, walk_id, share_type, target_start_date, target_end_date, created_at, expires_at, revoked_at
            FROM share_links
            WHERE owner_sub = ? AND walk_id = ?
            ORDER BY created_at DESC
            """,
            (owner_sub, walk_id),
        ).fetchall()
    return [dict(row) for row in rows]


def list_share_links_for_target(
    owner_sub: str,
    start_date: str | None = None,
    end_date: str | None = None,
    share_type: str | None = None,
) -> list[dict]:
    query = """
        SELECT token, owner_sub, walk_id, share_type, target_start_date, target_end_date, created_at, expires_at, revoked_at
        FROM share_links
        WHERE owner_sub = ? AND share_type IN ('daily', 'range') AND revoked_at IS NULL
    """
    params: list[Any] = [owner_sub]
    if share_type:
        query += " AND share_type = ?"
        params.append(share_type)
    if start_date:
        query += " AND target_start_date = ?"
        params.append(start_date)
    if end_date:
        query += " AND target_end_date = ?"
        params.append(end_date)
    query += " ORDER BY created_at DESC"
    with analytics_conn() as conn:
        rows = conn.execute(query, tuple(params)).fetchall()
    return [dict(row) for row in rows]


def get_share_link(token: str) -> dict | None:
    with analytics_conn() as conn:
        row = conn.execute(
            """
            SELECT token, owner_sub, walk_id, share_type, target_start_date, target_end_date, created_at, expires_at, revoked_at
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
            SELECT user_sub, user_type, insulin_type, glucose_unit, target_low_mmol, target_high_mmol,
                   hypo_threshold_mmol, hyper_threshold_mmol, pump_brand_model, cgm_provider,
                   basal_rates_json, carb_ratios_json, insulin_sensitivity_factor, correction_threshold,
                   exercise_correction_factor, iob_calculation_method, insulin_profile_json,
                   garmin_email, carelink_email, dexcom_username, dexcom_region,
                   last_garmin_sync_at, last_cgm_sync_at, updated_at
            FROM user_settings
            WHERE user_sub = ?
            """,
            (user_sub,),
        ).fetchone()
    if not row:
        return None
    res = dict(row)
    res["glucose_unit"] = res.get("glucose_unit") or "mmol/L"
    res["target_low_mmol"] = float(res["target_low_mmol"]) if res.get("target_low_mmol") is not None else 4.0
    res["target_high_mmol"] = float(res["target_high_mmol"]) if res.get("target_high_mmol") is not None else 8.0
    res["hypo_threshold_mmol"] = float(res["hypo_threshold_mmol"]) if res.get("hypo_threshold_mmol") is not None else 3.9
    res["hyper_threshold_mmol"] = float(res["hyper_threshold_mmol"]) if res.get("hyper_threshold_mmol") is not None else 12.9
    res["basal_rates"] = json.loads(res["basal_rates_json"]) if res.get("basal_rates_json") else None
    res["carb_ratios"] = json.loads(res["carb_ratios_json"]) if res.get("carb_ratios_json") else None
    res["insulin_profile"] = json.loads(res["insulin_profile_json"]) if res.get("insulin_profile_json") else None
    res.pop("basal_rates_json", None)
    res.pop("carb_ratios_json", None)
    res.pop("insulin_profile_json", None)
    return res


def _to_mmol_if_needed(val: float | int | None, input_unit: str) -> float | None:
    if val is None:
        return None
    try:
        fval = float(val)
    except (TypeError, ValueError):
        return None
    if input_unit == "mg/dL" and fval > 25.0:
        return round(fval / 18.0182, 2)
    return round(fval, 2)


def upsert_user_settings(user_sub: str, settings: dict) -> None:
    existing = get_user_settings(user_sub) or {}

    user_type = settings.get("user_type")
    insulin_type = settings.get("insulin_type", existing.get("insulin_type", "None"))

    valid_user_types = (
        "diabetic_closed_loop",
        "diabetic_pump",
        "diabetic_mdi",
        "diabetic_lifestyle",
        "non_diabetic_cgm",
        "non_diabetic",
    )

    if not user_type:
        if insulin_type == "ClosedLoopPump":
            user_type = "diabetic_closed_loop"
        elif insulin_type == "Pump":
            user_type = "diabetic_pump"
        elif insulin_type == "MDI":
            user_type = "diabetic_mdi"
        else:
            user_type = "non_diabetic"

    if user_type not in valid_user_types:
        user_type = "non_diabetic"

    # Enforce logical consistency for insulin_type based on user_type while preserving backward compatibility
    if user_type == "diabetic_closed_loop":
        insulin_type = "ClosedLoopPump"
    elif user_type == "diabetic_pump":
        insulin_type = "Pump"
    elif user_type == "diabetic_mdi":
        insulin_type = "MDI"
    else:
        insulin_type = "None"

    raw_unit = str(settings.get("glucose_unit") or existing.get("glucose_unit") or "mmol/L").strip()
    glucose_unit = "mg/dL" if raw_unit.lower() in ("mg/dl", "mgdl") else "mmol/L"
    input_unit = "mg/dL" if str(settings.get("input_unit") or "").strip().lower() in ("mg/dl", "mgdl") else "mmol/L"

    def _resolve_threshold(key: str, default_val: float) -> float:
        if key in settings and settings[key] is not None:
            converted = _to_mmol_if_needed(settings[key], input_unit)
            if converted is not None:
                return converted
        if existing.get(key) is not None:
            return float(existing[key])
        return default_val

    target_low_mmol = _resolve_threshold("target_low_mmol", 4.0)
    target_high_mmol = _resolve_threshold("target_high_mmol", 8.0)
    if target_low_mmol < 2.5:
        target_low_mmol = 2.5
    if target_high_mmol > 15.0:
        target_high_mmol = 15.0
    if target_low_mmol >= target_high_mmol:
        target_low_mmol, target_high_mmol = 4.0, 8.0

    hypo_threshold_mmol = _resolve_threshold("hypo_threshold_mmol", 3.9)
    if hypo_threshold_mmol > target_low_mmol:
        hypo_threshold_mmol = target_low_mmol

    hyper_threshold_mmol = _resolve_threshold("hyper_threshold_mmol", 12.9)
    if hyper_threshold_mmol < target_high_mmol:
        hyper_threshold_mmol = target_high_mmol

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
        pump_brand_model = settings.get("pump_brand_model") if insulin_type in ("Pump", "ClosedLoopPump") else None
        if insulin_type == "MDI":
            basal_rates_json = None
        else:
            basal_rates_json = json.dumps(settings.get("basal_rates")) if settings.get("basal_rates") is not None else None
        carb_ratios_json = json.dumps(settings.get("carb_ratios")) if settings.get("carb_ratios") is not None else None

        raw_isf = settings.get("insulin_sensitivity_factor")
        if raw_isf is not None:
            insulin_sensitivity_factor = _to_mmol_if_needed(raw_isf, input_unit)
        else:
            insulin_sensitivity_factor = None

        raw_corr = settings.get("correction_threshold")
        if raw_corr is not None:
            correction_threshold = _to_mmol_if_needed(raw_corr, input_unit)
        else:
            correction_threshold = None

        exercise_correction_factor = settings.get("exercise_correction_factor")
        iob_calculation_method = settings.get("iob_calculation_method")
        insulin_profile_json = json.dumps(settings.get("insulin_profile")) if settings.get("insulin_profile") is not None else None

    updated_at = datetime.now(timezone.utc).isoformat()

    garmin_email = settings["garmin_email"] if "garmin_email" in settings else existing.get("garmin_email")
    carelink_email = settings["carelink_email"] if "carelink_email" in settings else existing.get("carelink_email")
    dexcom_username = settings["dexcom_username"] if "dexcom_username" in settings else existing.get("dexcom_username")
    dexcom_region = settings.get("dexcom_region") or existing.get("dexcom_region") or "ous"
    if dexcom_region not in ("us", "ous"):
        dexcom_region = "ous"
    cgm_provider = settings.get("cgm_provider") or existing.get("cgm_provider") or "carelink"
    if cgm_provider not in ("carelink", "dexcom", "tandem", "none"):
        cgm_provider = "carelink"
    last_garmin_sync_at = settings["last_garmin_sync_at"] if "last_garmin_sync_at" in settings else existing.get("last_garmin_sync_at")
    last_cgm_sync_at = settings["last_cgm_sync_at"] if "last_cgm_sync_at" in settings else existing.get("last_cgm_sync_at")

    with analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO user_settings (
                user_sub, user_type, insulin_type, glucose_unit, target_low_mmol, target_high_mmol,
                hypo_threshold_mmol, hyper_threshold_mmol, pump_brand_model, cgm_provider,
                basal_rates_json, carb_ratios_json, insulin_sensitivity_factor, correction_threshold,
                exercise_correction_factor, iob_calculation_method, insulin_profile_json,
                garmin_email, carelink_email, dexcom_username, dexcom_region,
                last_garmin_sync_at, last_cgm_sync_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_sub) DO UPDATE SET
                user_type = excluded.user_type,
                insulin_type = excluded.insulin_type,
                glucose_unit = excluded.glucose_unit,
                target_low_mmol = excluded.target_low_mmol,
                target_high_mmol = excluded.target_high_mmol,
                hypo_threshold_mmol = excluded.hypo_threshold_mmol,
                hyper_threshold_mmol = excluded.hyper_threshold_mmol,
                pump_brand_model = excluded.pump_brand_model,
                cgm_provider = excluded.cgm_provider,
                basal_rates_json = excluded.basal_rates_json,
                carb_ratios_json = excluded.carb_ratios_json,
                insulin_sensitivity_factor = excluded.insulin_sensitivity_factor,
                correction_threshold = excluded.correction_threshold,
                exercise_correction_factor = excluded.exercise_correction_factor,
                iob_calculation_method = excluded.iob_calculation_method,
                insulin_profile_json = excluded.insulin_profile_json,
                garmin_email = excluded.garmin_email,
                carelink_email = excluded.carelink_email,
                dexcom_username = excluded.dexcom_username,
                dexcom_region = excluded.dexcom_region,
                last_garmin_sync_at = excluded.last_garmin_sync_at,
                last_cgm_sync_at = excluded.last_cgm_sync_at,
                updated_at = excluded.updated_at
            """,
            (
                user_sub,
                user_type,
                insulin_type,
                glucose_unit,
                target_low_mmol,
                target_high_mmol,
                hypo_threshold_mmol,
                hyper_threshold_mmol,
                pump_brand_model,
                cgm_provider,
                basal_rates_json,
                carb_ratios_json,
                insulin_sensitivity_factor,
                correction_threshold,
                exercise_correction_factor,
                iob_calculation_method,
                insulin_profile_json,
                garmin_email,
                carelink_email,
                dexcom_username,
                dexcom_region,
                last_garmin_sync_at,
                last_cgm_sync_at,
                updated_at,
            ),
        )


def upsert_activity_epochs(user_sub: str, epoch_rows: list[dict]) -> int:
    if not epoch_rows:
        return 0
    now_iso = datetime.now(timezone.utc).isoformat()
    params = []
    for row in epoch_rows:
        steps = max(0, int(row.get("steps") or 0))
        category = str(row.get("activity_category") or "sedentary").lower()
        if category not in ("sedentary", "walking", "running", "cycling", "highly_active", "sleeping", "other"):
            category = "other"
        intensity = str(row.get("intensity_level") or "sedentary").lower()
        if intensity not in ("sedentary", "light", "moderate", "vigorous"):
            intensity = "sedentary"
        params.append(
            (
                user_sub,
                row["epoch_start_utc"],
                row["epoch_end_utc"],
                row["local_date"],
                int(row.get("tz_offset_mins") or 0),
                steps,
                category,
                intensity,
                float(row.get("distance_m") or 0.0),
                float(row.get("active_kcal") or 0.0),
                float(row["mean_hr"]) if row.get("mean_hr") is not None else None,
                float(row["max_hr"]) if row.get("max_hr") is not None else None,
                row.get("linked_walk_id"),
                str(row.get("source_provider") or "garmin"),
                row.get("ingested_at") or now_iso,
            )
        )
    with analytics_conn() as conn:
        conn.executemany(
            """
            INSERT INTO activity_epochs (
                user_sub, epoch_start_utc, epoch_end_utc, local_date, tz_offset_mins,
                steps, activity_category, intensity_level, distance_m, active_kcal,
                mean_hr, max_hr, linked_walk_id, source_provider, ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_sub, epoch_start_utc) DO UPDATE SET
                epoch_end_utc = excluded.epoch_end_utc,
                local_date = excluded.local_date,
                tz_offset_mins = excluded.tz_offset_mins,
                steps = excluded.steps,
                activity_category = excluded.activity_category,
                intensity_level = excluded.intensity_level,
                distance_m = excluded.distance_m,
                active_kcal = excluded.active_kcal,
                mean_hr = COALESCE(excluded.mean_hr, activity_epochs.mean_hr),
                max_hr = COALESCE(excluded.max_hr, activity_epochs.max_hr),
                linked_walk_id = COALESCE(excluded.linked_walk_id, activity_epochs.linked_walk_id),
                source_provider = excluded.source_provider,
                ingested_at = excluded.ingested_at
            """,
            params,
        )
    return len(params)


def get_activity_epochs_for_date(user_sub: str, local_date: str) -> list[dict]:
    with analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT user_sub, epoch_start_utc, epoch_end_utc, local_date, tz_offset_mins,
                   steps, activity_category, intensity_level, distance_m, active_kcal,
                   mean_hr, max_hr, linked_walk_id, source_provider, ingested_at
            FROM activity_epochs
            WHERE user_sub = ? AND local_date = ?
            ORDER BY epoch_start_utc ASC
            """,
            (user_sub, local_date),
        ).fetchall()
    return [dict(r) for r in rows]


def upsert_daily_wellness_summary(user_sub: str, local_date: str, summary: dict) -> None:
    now_iso = datetime.now(timezone.utc).isoformat()
    raw_json = json.dumps(summary.get("raw_summary")) if summary.get("raw_summary") is not None else None
    with analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO daily_wellness_summaries (
                user_sub, local_date, total_steps, total_distance_m,
                moderate_intensity_mins, vigorous_intensity_mins,
                resting_hr, min_hr, max_hr, active_kcal, raw_summary_json, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_sub, local_date) DO UPDATE SET
                total_steps = excluded.total_steps,
                total_distance_m = excluded.total_distance_m,
                moderate_intensity_mins = excluded.moderate_intensity_mins,
                vigorous_intensity_mins = excluded.vigorous_intensity_mins,
                resting_hr = excluded.resting_hr,
                min_hr = excluded.min_hr,
                max_hr = excluded.max_hr,
                active_kcal = excluded.active_kcal,
                raw_summary_json = excluded.raw_summary_json,
                updated_at = excluded.updated_at
            """,
            (
                user_sub,
                local_date,
                max(0, int(summary.get("total_steps") or 0)),
                float(summary.get("total_distance_m") or 0.0),
                int(summary.get("moderate_intensity_mins") or 0),
                int(summary.get("vigorous_intensity_mins") or 0),
                float(summary["resting_hr"]) if summary.get("resting_hr") is not None else None,
                float(summary["min_hr"]) if summary.get("min_hr") is not None else None,
                float(summary["max_hr"]) if summary.get("max_hr") is not None else None,
                float(summary.get("active_kcal") or 0.0),
                raw_json,
                now_iso,
            ),
        )


def get_daily_wellness_summary(user_sub: str, local_date: str) -> dict | None:
    with analytics_conn() as conn:
        row = conn.execute(
            """
            SELECT user_sub, local_date, total_steps, total_distance_m,
                   moderate_intensity_mins, vigorous_intensity_mins,
                   resting_hr, min_hr, max_hr, active_kcal, raw_summary_json, updated_at
            FROM daily_wellness_summaries
            WHERE user_sub = ? AND local_date = ?
            """,
            (user_sub, local_date),
        ).fetchone()
    if not row:
        return None
    res = dict(row)
    res["raw_summary"] = json.loads(res["raw_summary_json"]) if res.get("raw_summary_json") else None
    res.pop("raw_summary_json", None)
    return res


def upsert_glycemic_events(user_sub: str, event_rows: list[dict]) -> int:
    if not event_rows:
        return 0
    now_iso = datetime.now(timezone.utc).isoformat()
    params = []
    for row in event_rows:
        etype = str(row.get("event_type") or "cgm").lower()
        if etype not in ("cgm", "bolus", "basal", "carb", "pump_state"):
            continue
        bg_mmol = row.get("bg_mmol")
        if etype == "cgm":
            if bg_mmol is None or not (1.0 <= float(bg_mmol) <= 35.0):
                continue
            bg_mmol = round(float(bg_mmol), 2)
        bolus_units = row.get("bolus_units")
        if etype == "bolus":
            if bolus_units is None or float(bolus_units) <= 0:
                continue
            bolus_units = round(float(bolus_units), 3)
        basal_rate = row.get("basal_rate_u_hr")
        if etype == "basal":
            if basal_rate is None or float(basal_rate) < 0:
                continue
            basal_rate = round(float(basal_rate), 3)
        carb_grams = row.get("carb_grams")
        if etype == "carb":
            if carb_grams is None or float(carb_grams) <= 0:
                continue
            carb_grams = round(float(carb_grams), 1)
        pump_state = row.get("pump_state")
        provider = str(row.get("source_provider") or "carelink").lower()
        if provider not in ("carelink", "dexcom", "tandem", "manual"):
            provider = "manual"
        params.append(
            (
                user_sub,
                row["timestamp_utc"],
                etype,
                row["local_date"],
                int(row.get("tz_offset_mins") or 0),
                bg_mmol,
                row.get("trend_arrow"),
                bolus_units,
                basal_rate,
                carb_grams,
                pump_state,
                provider,
                row.get("ingested_at") or now_iso,
            )
        )
    if not params:
        return 0
    with analytics_conn() as conn:
        conn.executemany(
            """
            INSERT INTO glycemic_events (
                user_sub, timestamp_utc, event_type, local_date, tz_offset_mins,
                bg_mmol, trend_arrow, bolus_units, basal_rate_u_hr, carb_grams,
                pump_state, source_provider, ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_sub, timestamp_utc, event_type) DO UPDATE SET
                local_date = excluded.local_date,
                tz_offset_mins = excluded.tz_offset_mins,
                bg_mmol = excluded.bg_mmol,
                trend_arrow = COALESCE(excluded.trend_arrow, glycemic_events.trend_arrow),
                bolus_units = excluded.bolus_units,
                basal_rate_u_hr = excluded.basal_rate_u_hr,
                carb_grams = excluded.carb_grams,
                pump_state = excluded.pump_state,
                source_provider = excluded.source_provider,
                ingested_at = excluded.ingested_at
            """,
            params,
        )
    return len(params)


def get_glycemic_events_for_date(user_sub: str, local_date: str) -> list[dict]:
    with analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT user_sub, timestamp_utc, event_type, local_date, tz_offset_mins,
                   bg_mmol, trend_arrow, bolus_units, basal_rate_u_hr, carb_grams,
                   pump_state, source_provider, ingested_at
            FROM glycemic_events
            WHERE user_sub = ? AND local_date = ?
            ORDER BY timestamp_utc ASC, event_type ASC
            """,
            (user_sub, local_date),
        ).fetchall()
    return [dict(r) for r in rows]


def get_glycemic_events_for_window(user_sub: str, start_utc: str, end_utc: str) -> list[dict]:
    if len(str(start_utc).strip()) == 10 and len(str(end_utc).strip()) == 10:
        with analytics_conn() as conn:
            rows = conn.execute(
                """
                SELECT user_sub, timestamp_utc, event_type, local_date, tz_offset_mins,
                       bg_mmol, trend_arrow, bolus_units, basal_rate_u_hr, carb_grams,
                       pump_state, source_provider, ingested_at
                FROM glycemic_events
                WHERE user_sub = ? AND local_date >= ? AND local_date <= ?
                ORDER BY timestamp_utc ASC, event_type ASC
                """,
                (user_sub, str(start_utc).strip(), str(end_utc).strip()),
            ).fetchall()
        return [dict(r) for r in rows]

    with analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT user_sub, timestamp_utc, event_type, local_date, tz_offset_mins,
                   bg_mmol, trend_arrow, bolus_units, basal_rate_u_hr, carb_grams,
                   pump_state, source_provider, ingested_at
            FROM glycemic_events
            WHERE user_sub = ? AND timestamp_utc >= ? AND timestamp_utc <= ?
            ORDER BY timestamp_utc ASC, event_type ASC
            """,
            (user_sub, start_utc, end_utc),
        ).fetchall()
    return [dict(r) for r in rows]


def upsert_daily_analytics(
    user_sub: str,
    local_date: str,
    source_hash: str,
    computed_at: str,
    summary: dict,
    payload: dict,
) -> None:
    with analytics_conn() as conn:
        conn.execute(
            """
            INSERT INTO daily_analytics (
                user_sub, local_date, source_hash, analytics_version, computed_at,
                total_steps, active_minutes, tir_pct, tbr_pct, tar_pct,
                avg_bg_mmol, bg_cv_pct, hypos_count, bolus_total_units,
                carb_total_grams, sensor_coverage_pct, payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_sub, local_date) DO UPDATE SET
                source_hash = excluded.source_hash,
                analytics_version = excluded.analytics_version,
                computed_at = excluded.computed_at,
                total_steps = excluded.total_steps,
                active_minutes = excluded.active_minutes,
                tir_pct = excluded.tir_pct,
                tbr_pct = excluded.tbr_pct,
                tar_pct = excluded.tar_pct,
                avg_bg_mmol = excluded.avg_bg_mmol,
                bg_cv_pct = excluded.bg_cv_pct,
                hypos_count = excluded.hypos_count,
                bolus_total_units = excluded.bolus_total_units,
                carb_total_grams = excluded.carb_total_grams,
                sensor_coverage_pct = excluded.sensor_coverage_pct,
                payload_json = excluded.payload_json
            """,
            (
                user_sub,
                local_date,
                source_hash,
                ANALYTICS_VERSION,
                computed_at,
                int(summary.get("total_steps") or 0),
                int(summary.get("active_minutes") or 0),
                summary.get("tir_pct"),
                summary.get("tbr_pct"),
                summary.get("tar_pct"),
                summary.get("avg_bg_mmol"),
                summary.get("bg_cv_pct"),
                int(summary.get("hypos_count") or 0),
                float(summary.get("bolus_total_units") or 0.0),
                float(summary.get("carb_total_grams") or 0.0),
                float(summary.get("sensor_coverage_pct") or 0.0),
                json.dumps(payload),
            ),
        )


def _daily_analytics_row_to_dict(row) -> dict:
    r = dict(row)
    r["payload"] = json.loads(r.pop("payload_json"))
    return r


def get_cached_daily_analytics(user_sub: str, local_date: str) -> dict | None:
    with analytics_conn() as conn:
        row = conn.execute(
            "SELECT * FROM daily_analytics WHERE user_sub = ? AND local_date = ?",
            (user_sub, local_date),
        ).fetchone()
    return _daily_analytics_row_to_dict(row) if row else None


def list_daily_analytics_range(user_sub: str, start_date: str, end_date: str) -> list[dict]:
    with analytics_conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM daily_analytics
            WHERE user_sub = ? AND local_date >= ? AND local_date <= ?
            ORDER BY local_date ASC
            """,
            (user_sub, start_date, end_date),
        ).fetchall()
    return [_daily_analytics_row_to_dict(r) for r in rows]


def delete_user_data(user_sub: str, walk_ids: list[str]) -> None:
    with analytics_conn() as conn:
        conn.execute("DELETE FROM user_settings WHERE user_sub = ?", (user_sub,))
        conn.execute("DELETE FROM activity_epochs WHERE user_sub = ?", (user_sub,))
        conn.execute("DELETE FROM daily_wellness_summaries WHERE user_sub = ?", (user_sub,))
        conn.execute("DELETE FROM glycemic_events WHERE user_sub = ?", (user_sub,))
        conn.execute("DELETE FROM daily_analytics WHERE user_sub = ?", (user_sub,))
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
