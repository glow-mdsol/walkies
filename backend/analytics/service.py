import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from fastapi import HTTPException

from .db import (
    ANALYTICS_VERSION,
    get_activity_epochs_for_date,
    get_cached_analytics_row,
    get_cached_daily_analytics,
    get_daily_wellness_summary,
    get_glycemic_events_for_window,
    get_user_settings,
    upsert_daily_analytics,
    upsert_walk_analytics,
    upsert_walk_sources,
)
from .models.daily_correlation import compute_daily_correlation, compute_daily_range_summary


FindWalkDir = Callable[[str], Path | None]
IterWalkDirs = Callable[[], list[Path]]
LoadWalkMeta = Callable[[Path], dict]
GetWalkAnalysisData = Callable[[str, bool], tuple[str, str | None, dict, dict]]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def walk_source_rows(walk_dir: Path) -> list[dict]:
    rows: list[dict] = []
    for file_path in sorted(child for child in walk_dir.iterdir() if child.is_file()):
        stat = file_path.stat()
        rows.append({
            "filename": file_path.name,
            "file_type": file_path.suffix.lower().lstrip("."),
            "size_bytes": int(stat.st_size),
            "modified_time": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            "content_hash": _sha256_file(file_path),
        })
    return rows


def walk_source_hash(source_rows: list[dict]) -> str:
    digest = hashlib.sha256()
    for row in source_rows:
        digest.update(
            "\0".join([
                row["filename"],
                row.get("file_type") or "",
                str(row["size_bytes"]),
                row["modified_time"],
                row["content_hash"],
            ]).encode("utf-8")
        )
        digest.update(b"\n")
    return digest.hexdigest()


def analytics_summary_payload(walk_meta: dict, payload: dict, metrics: dict) -> dict:
    phase_rows = payload.get("phaseAnalytics", {}).get("phases", []) or []
    intensity_rows = payload.get("intensityAnalytics", []) or []
    stress_summary = payload.get("stressAnalytics", {}).get("summary", {}) or {}
    insulin_stress_summary = payload.get("insulinStressEffect", {}).get("summary", {}) or {}
    smartguard_summary = payload.get("smartguard", {}).get("summary", {}) or {}
    glucose_values = [point.get("bg") for point in payload.get("bg", []) if point.get("bg") is not None]
    dominant_zone = None
    if intensity_rows:
        dominant = max(intensity_rows, key=lambda row: row.get("minutes") or 0)
        dominant_zone = dominant.get("zone")

    return {
        "walk": {
            "id": walk_meta.get("id"),
            "date": walk_meta.get("date"),
            "name": walk_meta.get("name"),
            "start_time": walk_meta.get("start_time"),
        },
        "glucose": {
            "samples": len(glucose_values),
            "start_bg": glucose_values[0] if glucose_values else None,
            "end_bg": glucose_values[-1] if glucose_values else None,
            "min_bg": min(glucose_values) if glucose_values else None,
            "max_bg": max(glucose_values) if glucose_values else None,
            "bg_slope_during_h": metrics.get("bg_slope_during_h"),
            "hypos": metrics.get("hypos"),
        },
        "insulin": {
            "bolus_units": metrics.get("bolus_units"),
            "bolus_event_count": len(payload.get("bolus", []) or []),
            "basal_sample_count": len(payload.get("basal", []) or []),
            "stress_multiplier": insulin_stress_summary.get("stress_multiplier"),
            "stress_band": insulin_stress_summary.get("band"),
            "iob_overlap_minutes": insulin_stress_summary.get("overlap_minutes"),
        },
        "smartguard": {
            "event_count": smartguard_summary.get("event_count"),
            "predicted_low_suspend_count": smartguard_summary.get("predicted_low_suspend_count"),
            "suspend_count": smartguard_summary.get("suspend_count"),
            "resume_count": smartguard_summary.get("resume_count"),
            "any_activity": smartguard_summary.get("any_activity"),
        },
        "weather": {
            "weather_stress_score": metrics.get("weather_stress_score"),
            "weather_stress_band": metrics.get("weather_stress_band"),
            "headwind_exposure_pct": metrics.get("headwind_exposure_pct"),
            "wind_avg_kph": metrics.get("wind_avg_kph"),
            "temp_avg_c": metrics.get("temp_avg_c"),
        },
        "stress": {
            "score": stress_summary.get("score"),
            "band": stress_summary.get("band"),
            "elevated_minutes": stress_summary.get("elevated_minutes"),
            "max_residual_bpm": stress_summary.get("max_residual_bpm"),
        },
        "training": {
            "phase_rows": phase_rows,
            "intensity_rows": intensity_rows,
            "dominant_hr_zone": dominant_zone,
        },
    }


def persist_walk_analytics(
    walk_dir: Path,
    walk_meta: dict,
    payload: dict,
    metrics: dict,
    route_fingerprint: dict | None = None,
    route_series: dict | None = None,
) -> dict:
    source_rows = walk_source_rows(walk_dir)
    source_hash = walk_source_hash(source_rows)
    summary = analytics_summary_payload(walk_meta, payload, metrics)
    computed_at = datetime.now(timezone.utc).isoformat()

    upsert_walk_sources(walk_meta["id"], source_rows)
    upsert_walk_analytics(
        walk_meta,
        source_hash,
        computed_at,
        metrics,
        summary,
        route_fingerprint=route_fingerprint,
        route_series=route_series,
    )

    return {
        "walk_id": walk_meta["id"],
        "source_hash": source_hash,
        "analytics_version": ANALYTICS_VERSION,
        "computed_at": computed_at,
    }


def refresh_walk_analytics_if_needed(
    walk_id: str,
    *,
    force: bool = False,
    find_walk_dir: FindWalkDir,
    get_walk_analysis_data: GetWalkAnalysisData,
) -> dict:
    walk_dir = find_walk_dir(walk_id)
    if walk_dir is None or not walk_dir.exists():
        raise HTTPException(status_code=404, detail="Walk not found")

    source_hash = walk_source_hash(walk_source_rows(walk_dir))
    cached = get_cached_analytics_row(walk_id)
    is_fresh = cached is not None and cached["source_hash"] == source_hash and cached["analytics_version"] == ANALYTICS_VERSION
    if not force and is_fresh:
        return {
            "walk_id": walk_id,
            "status": "fresh",
            "computed_at": cached["computed_at"],
            "analytics_version": cached["analytics_version"],
        }

    date, walk_name, _payload, metrics = get_walk_analysis_data(walk_id, True)
    cached = get_cached_analytics_row(walk_id)
    return {
        "walk_id": walk_id,
        "date": date,
        "name": walk_name,
        "status": "recomputed",
        "computed_at": cached["computed_at"] if cached else None,
        "analytics_version": ANALYTICS_VERSION,
        "distance_km": metrics.get("distance_km"),
    }


def list_walk_analytics(
    *,
    iter_walk_dirs: IterWalkDirs,
    load_walk_meta: LoadWalkMeta,
) -> list[dict]:
    items: list[dict] = []
    for walk_dir in iter_walk_dirs():
        walk_meta = load_walk_meta(walk_dir)
        source_hash = walk_source_hash(walk_source_rows(walk_dir))
        cached = get_cached_analytics_row(walk_meta["id"])
        cache_status = "missing"
        if cached is not None:
            cache_status = "fresh" if cached["source_hash"] == source_hash and cached["analytics_version"] == ANALYTICS_VERSION else "stale"

        items.append({
            "walk_id": walk_meta["id"],
            "date": walk_meta["date"],
            "name": walk_meta.get("name") or None,
            "start_time": walk_meta.get("start_time"),
            "cache_status": cache_status,
            "analytics_version": cached["analytics_version"] if cached else None,
            "computed_at": cached["computed_at"] if cached else None,
            "metrics": cached["metrics"] if cached else None,
            "summary": cached["summary"] if cached else None,
        })

    items.sort(key=lambda item: (item.get("start_time") or "", item["date"], item["walk_id"]), reverse=True)
    return items


def daily_source_hash(
    local_date: str,
    epochs: list[dict],
    glycemic_events: list[dict],
    wellness_summary: dict | None,
    user_settings: dict | None,
    linked_walks: list[dict] | None,
) -> str:
    """Compute a deterministic SHA-256 fingerprint for a day's telemetry inputs and clinical settings."""
    import json
    from datetime import date, timedelta

    settings = user_settings or {}
    fingerprint_obj = {
        "v": ANALYTICS_VERSION,
        "date": local_date,
        "regimen": {
            "user_type": settings.get("user_type"),
            "insulin_type": settings.get("insulin_type"),
            "glucose_unit": settings.get("glucose_unit"),
            "target_low_mmol": settings.get("target_low_mmol"),
            "target_high_mmol": settings.get("target_high_mmol"),
            "hypo_threshold_mmol": settings.get("hypo_threshold_mmol"),
            "hyper_threshold_mmol": settings.get("hyper_threshold_mmol"),
            "insulin_profile": settings.get("insulin_profile"),
        },
        "wellness": {
            "total_steps": (wellness_summary or {}).get("total_steps"),
            "moderate_intensity_mins": (wellness_summary or {}).get("moderate_intensity_mins"),
            "vigorous_intensity_mins": (wellness_summary or {}).get("vigorous_intensity_mins"),
            "resting_hr": (wellness_summary or {}).get("resting_hr"),
        },
        "epochs": [
            (
                e.get("epoch_start_utc"),
                e.get("steps"),
                e.get("activity_category"),
                e.get("intensity_level"),
                e.get("mean_hr"),
                e.get("linked_walk_id"),
            )
            for e in epochs
        ],
        "events": [
            (
                ev.get("timestamp_utc"),
                ev.get("event_type"),
                ev.get("bg_mmol"),
                ev.get("bolus_units"),
                ev.get("carb_grams"),
                ev.get("pump_state"),
            )
            for ev in glycemic_events
        ],
        "walks": [
            (w.get("id"), w.get("name"), w.get("start_time"), w.get("distance_km"), w.get("tir_pct"))
            for w in (linked_walks or [])
        ],
    }
    raw_bytes = json.dumps(fingerprint_obj, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw_bytes).hexdigest()


def get_or_compute_daily_analytics(
    user_sub: str,
    local_date: str,
    *,
    force: bool = False,
    linked_walks: list[dict] | None = None,
) -> dict:
    """Return cached daily correlation analytics if fresh, or recompute and persist."""
    from datetime import date as dt_date, timedelta

    try:
        d_obj = dt_date.fromisoformat(str(local_date).strip()[:10])
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid date format: {local_date}. Expected YYYY-MM-DD.")

    prev_date = (d_obj - timedelta(days=1)).isoformat()
    next_date = (d_obj + timedelta(days=1)).isoformat()

    epochs = get_activity_epochs_for_date(user_sub, local_date)
    wellness = get_daily_wellness_summary(user_sub, local_date)
    # Include prev_date and next_date events for 4h prior IOB and 1-2h post-activity lag windows
    events = get_glycemic_events_for_window(user_sub, prev_date, next_date)
    settings = get_user_settings(user_sub) or {}

    src_hash = daily_source_hash(local_date, epochs, events, wellness, settings, linked_walks)
    cached = get_cached_daily_analytics(user_sub, local_date)

    if (
        not force
        and cached is not None
        and cached.get("source_hash") == src_hash
        and cached.get("analytics_version") == ANALYTICS_VERSION
        and isinstance(cached.get("payload"), dict)
    ):
        payload = dict(cached["payload"])
        payload["analytics_version"] = cached["analytics_version"]
        payload["computed_at"] = cached["computed_at"]
        # Keep display unit in sync with latest user_settings
        if "regimen" in payload and settings.get("glucose_unit"):
            payload["regimen"]["glucose_unit"] = settings["glucose_unit"]
        return payload

    computed = compute_daily_correlation(
        local_date=local_date,
        epochs=epochs,
        glycemic_events=events,
        wellness_summary=wellness,
        user_settings=settings,
        linked_walks=linked_walks,
    )
    computed_at = datetime.now(timezone.utc).isoformat()
    computed["analytics_version"] = ANALYTICS_VERSION
    computed["computed_at"] = computed_at

    upsert_daily_analytics(
        user_sub=user_sub,
        local_date=local_date,
        source_hash=src_hash,
        computed_at=computed_at,
        summary=computed["summary"],
        payload=computed,
    )
    return computed


def get_or_compute_daily_range_analytics(
    user_sub: str,
    start_date: str,
    end_date: str,
    *,
    force: bool = False,
    linked_walks_by_date: dict[str, list[dict]] | None = None,
) -> dict:
    """Compute or retrieve multi-day aggregate correlation across [start_date, end_date]."""
    from datetime import date as dt_date, timedelta

    try:
        s_dt = dt_date.fromisoformat(str(start_date).strip()[:10])
        e_dt = dt_date.fromisoformat(str(end_date).strip()[:10])
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid start_date or end_date. Expected YYYY-MM-DD.")

    if e_dt < s_dt:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date.")

    span_days = (e_dt - s_dt).days + 1
    if span_days > 90:
        raise HTTPException(status_code=400, detail="Date range cannot exceed 90 days.")

    walks_map = linked_walks_by_date or {}
    day_payloads: list[dict] = []
    for offset in range(span_days):
        d_str = (s_dt + timedelta(days=offset)).isoformat()
        day_data = get_or_compute_daily_analytics(
            user_sub=user_sub,
            local_date=d_str,
            force=force,
            linked_walks=walks_map.get(d_str, []),
        )
        day_payloads.append(day_data)

    settings = get_user_settings(user_sub) or {}
    return compute_daily_range_summary(
        start_date=s_dt.isoformat(),
        end_date=e_dt.isoformat(),
        day_payloads=day_payloads,
        user_settings=settings,
    )

