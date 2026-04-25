import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from fastapi import HTTPException

from .db import (
    ANALYTICS_VERSION,
    get_cached_analytics_row,
    upsert_walk_analytics,
    upsert_walk_sources,
)


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


def persist_walk_analytics(walk_dir: Path, walk_meta: dict, payload: dict, metrics: dict) -> dict:
    source_rows = walk_source_rows(walk_dir)
    source_hash = walk_source_hash(source_rows)
    summary = analytics_summary_payload(walk_meta, payload, metrics)
    computed_at = datetime.now(timezone.utc).isoformat()

    upsert_walk_sources(walk_meta["id"], source_rows)
    upsert_walk_analytics(walk_meta, source_hash, computed_at, metrics, summary)

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