"""Garmin Connect 15-minute intraday epoch and daily wellness summary fetcher & normalizer."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from .common import normalize_timestamp_utc

MAX_SYNC_RANGE_DAYS = 30
DEFAULT_SYNC_LOOKBACK_DAYS = 7

_CATEGORY_MAP = {
    "sedentary": "sedentary",
    "walking": "walking",
    "running": "running",
    "cycling": "cycling",
    "highlyactive": "highly_active",
    "highly_active": "highly_active",
    "sleeping": "sleeping",
    "other": "other",
}


def resolve_sync_date_range(
    start_date: str | None = None,
    end_date: str | None = None,
    default_days: int = DEFAULT_SYNC_LOOKBACK_DAYS,
    max_days: int = MAX_SYNC_RANGE_DAYS,
    today: date | None = None,
) -> list[str]:
    """Resolve and validate a list of YYYY-MM-DD dates for Garmin/CGM sync."""
    ref_today = today or datetime.now(timezone.utc).date()

    if end_date:
        end_dt = date.fromisoformat(str(end_date).strip()[:10])
    else:
        end_dt = ref_today

    if start_date:
        start_dt = date.fromisoformat(str(start_date).strip()[:10])
    else:
        start_dt = end_dt - timedelta(days=max(1, default_days) - 1)

    if end_dt < start_dt:
        raise ValueError("end_date must be greater than or equal to start_date")

    span_days = (end_dt - start_dt).days + 1
    if span_days > max_days:
        raise ValueError(f"Date range cannot exceed {max_days} days (requested {span_days} days)")

    return [(start_dt + timedelta(days=offset)).isoformat() for offset in range(span_days)]


def _map_activity_category(raw_level: str | None, steps: int = 0) -> str:
    if not raw_level:
        return "walking" if steps >= 100 else "sedentary"
    key = str(raw_level).strip().lower().replace(" ", "_")
    if key in _CATEGORY_MAP:
        return _CATEGORY_MAP[key]
    if "walk" in key or "hike" in key:
        return "walking"
    if "run" in key:
        return "running"
    if "cycl" in key or "bike" in key:
        return "cycling"
    if "high" in key or "active" in key:
        return "highly_active"
    if "sleep" in key:
        return "sleeping"
    if "seden" in key or "rest" in key:
        return "sedentary"
    return "other"


def _infer_intensity_level(category: str, steps: int, raw_intensity: str | None = None) -> str:
    if raw_intensity:
        cleaned = str(raw_intensity).strip().lower()
        if cleaned in ("sedentary", "light", "moderate", "vigorous"):
            return cleaned
    if category in ("running", "highly_active") or steps >= 1200:
        return "vigorous"
    if category in ("walking", "cycling") or steps >= 250:
        return "moderate"
    if steps > 15 and category not in ("sedentary", "sleeping"):
        return "light"
    if steps >= 50:
        return "light"
    return "sedentary"


def _extract_hr_samples(hr_payload: Any) -> list[tuple[float, float]]:
    """Extract (unix_seconds, bpm) pairs from Garmin get_heart_rates payload."""
    samples: list[tuple[float, float]] = []
    if not hr_payload:
        return samples

    raw_values = []
    if isinstance(hr_payload, dict):
        raw_values = hr_payload.get("heartRateValues") or hr_payload.get("samples") or []
    elif isinstance(hr_payload, list):
        raw_values = hr_payload

    for item in raw_values:
        if isinstance(item, (list, tuple)) and len(item) >= 2:
            ts_raw, bpm_raw = item[0], item[1]
            if ts_raw is None or bpm_raw is None:
                continue
            try:
                ts_val = float(ts_raw)
                if ts_val > 1e11:
                    ts_val /= 1000.0
                bpm_val = float(bpm_raw)
                if bpm_val > 0:
                    samples.append((ts_val, bpm_val))
            except (TypeError, ValueError):
                continue
        elif isinstance(item, dict):
            ts_raw = item.get("timestamp") or item.get("startGMT")
            bpm_raw = item.get("hr") or item.get("heartRate") or item.get("bpm")
            if ts_raw is None or bpm_raw is None:
                continue
            try:
                ts_iso, _, _ = normalize_timestamp_utc(ts_raw)
                ts_val = datetime.fromisoformat(ts_iso).timestamp()
                bpm_val = float(bpm_raw)
                if bpm_val > 0:
                    samples.append((ts_val, bpm_val))
            except (TypeError, ValueError):
                continue

    samples.sort(key=lambda pair: pair[0])
    return samples


def normalize_garmin_day_epochs(
    local_date: str,
    raw_epochs: list[dict] | None,
    hr_payload: Any = None,
    walk_windows: list[dict] | None = None,
    default_tz_offset_mins: int = 0,
) -> list[dict]:
    """Normalize Garmin 15-minute step/activity epochs for a single local_date."""
    if not raw_epochs:
        return []

    hr_samples = _extract_hr_samples(hr_payload)
    windows = walk_windows or []
    normalized: list[dict] = []

    for raw in raw_epochs:
        if not isinstance(raw, dict):
            continue
        start_raw = (
            raw.get("startGMT")
            or raw.get("epoch_start_utc")
            or raw.get("startTimeGMT")
            or raw.get("startTimeLocal")
        )
        if not start_raw:
            continue

        try:
            start_iso, inferred_date, tz_offset = normalize_timestamp_utc(
                start_raw, default_tz_offset_mins=default_tz_offset_mins
            )
            start_dt = datetime.fromisoformat(start_iso)
        except ValueError:
            continue

        end_raw = raw.get("endGMT") or raw.get("epoch_end_utc") or raw.get("endTimeGMT")
        if end_raw:
            try:
                end_iso, _, _ = normalize_timestamp_utc(
                    end_raw, default_tz_offset_mins=default_tz_offset_mins
                )
                end_dt = datetime.fromisoformat(end_iso)
            except ValueError:
                end_dt = start_dt + timedelta(minutes=15)
                end_iso = end_dt.isoformat()
        else:
            end_dt = start_dt + timedelta(minutes=15)
            end_iso = end_dt.isoformat()

        steps = max(0, int(raw.get("steps") or 0))
        category = _map_activity_category(
            raw.get("primaryActivityLevel") or raw.get("activity_category"),
            steps=steps,
        )
        intensity = _infer_intensity_level(
            category,
            steps,
            raw_intensity=raw.get("intensity_level"),
        )

        # Match heart rate samples in [start_dt, end_dt)
        mean_hr = raw.get("mean_hr")
        max_hr = raw.get("max_hr")
        if (mean_hr is None or max_hr is None) and hr_samples:
            s_unix = start_dt.timestamp()
            e_unix = end_dt.timestamp()
            bucket_hrs = [bpm for ts, bpm in hr_samples if s_unix <= ts < e_unix]
            if bucket_hrs:
                if mean_hr is None:
                    mean_hr = round(sum(bucket_hrs) / len(bucket_hrs), 1)
                if max_hr is None:
                    max_hr = round(max(bucket_hrs), 1)

        # Tag linked_walk_id if overlapping with a recorded walk window
        linked_walk_id = raw.get("linked_walk_id")
        if not linked_walk_id and windows:
            for win in windows:
                w_start = win.get("start_utc")
                w_end = win.get("end_utc")
                if isinstance(w_start, str):
                    try:
                        w_start = datetime.fromisoformat(w_start.replace("Z", "+00:00"))
                    except ValueError:
                        w_start = None
                if isinstance(w_end, str):
                    try:
                        w_end = datetime.fromisoformat(w_end.replace("Z", "+00:00"))
                    except ValueError:
                        w_end = None
                if w_start and w_end:
                    if start_dt < w_end and end_dt > w_start:
                        linked_walk_id = win.get("walk_id")
                        break

        distance_m = raw.get("distance_m")
        if distance_m is None and raw.get("distance") is not None:
            distance_m = float(raw["distance"])
        elif distance_m is None and steps > 0:
            # Standard stride approximation (0.762 m/step) when distance is omitted in epoch API
            distance_m = round(steps * 0.762, 1)

        active_kcal = raw.get("active_kcal")
        if active_kcal is None and raw.get("activeKilocalories") is not None:
            active_kcal = float(raw["activeKilocalories"])

        normalized.append({
            "epoch_start_utc": start_iso,
            "epoch_end_utc": end_iso,
            "local_date": raw.get("local_date") or local_date or inferred_date,
            "tz_offset_mins": int(raw.get("tz_offset_mins", tz_offset)),
            "steps": steps,
            "activity_category": category,
            "intensity_level": intensity,
            "distance_m": round(float(distance_m), 1) if distance_m is not None else None,
            "active_kcal": round(float(active_kcal), 1) if active_kcal is not None else None,
            "mean_hr": round(float(mean_hr), 1) if mean_hr is not None else None,
            "max_hr": round(float(max_hr), 1) if max_hr is not None else None,
            "linked_walk_id": linked_walk_id,
            "source_provider": "garmin",
        })

    normalized.sort(key=lambda row: row["epoch_start_utc"])
    return normalized


def normalize_garmin_daily_summary(
    local_date: str,
    stats_payload: dict | None,
    hr_payload: dict | None = None,
    epochs: list[dict] | None = None,
) -> dict:
    """Normalize Garmin daily wellness summary from get_stats(date) + epoch fallback."""
    stats = stats_payload if isinstance(stats_payload, dict) else {}
    hr = hr_payload if isinstance(hr_payload, dict) else {}
    epoch_rows = epochs or []

    epoch_steps_sum = sum(int(e.get("steps") or 0) for e in epoch_rows)
    total_steps = stats.get("totalSteps")
    if total_steps is None:
        total_steps = stats.get("total_steps", epoch_steps_sum)

    total_distance_m = stats.get("totalDistanceMeters", stats.get("total_distance_m"))
    if total_distance_m is None and epoch_rows:
        dist_vals = [float(e["distance_m"]) for e in epoch_rows if e.get("distance_m") is not None]
        if dist_vals:
            total_distance_m = round(sum(dist_vals), 1)

    moderate_mins = stats.get("moderateIntensityMinutes", stats.get("moderate_intensity_mins"))
    if moderate_mins is None and epoch_rows:
        moderate_mins = sum(15 for e in epoch_rows if e.get("intensity_level") == "moderate")

    vigorous_mins = stats.get("vigorousIntensityMinutes", stats.get("vigorous_intensity_mins"))
    if vigorous_mins is None and epoch_rows:
        vigorous_mins = sum(15 for e in epoch_rows if e.get("intensity_level") == "vigorous")

    resting_hr = stats.get("restingHeartRate", stats.get("resting_hr", hr.get("restingHeartRate")))
    min_hr = stats.get("minHeartRate", stats.get("min_hr", hr.get("minHeartRate")))
    max_hr = stats.get("maxHeartRate", stats.get("max_hr", hr.get("maxHeartRate")))
    active_kcal = stats.get("activeKilocalories", stats.get("active_kcal"))

    return {
        "local_date": local_date,
        "total_steps": max(0, int(total_steps or 0)),
        "total_distance_m": float(total_distance_m) if total_distance_m is not None else None,
        "moderate_intensity_mins": int(moderate_mins) if moderate_mins is not None else None,
        "vigorous_intensity_mins": int(vigorous_mins) if vigorous_mins is not None else None,
        "resting_hr": float(resting_hr) if resting_hr is not None else None,
        "min_hr": float(min_hr) if min_hr is not None else None,
        "max_hr": float(max_hr) if max_hr is not None else None,
        "active_kcal": float(active_kcal) if active_kcal is not None else None,
        "raw_summary": stats if stats else None,
    }


def fetch_garmin_epochs_for_dates(
    client: Any,
    dates: list[str],
    walk_windows: list[dict] | None = None,
) -> tuple[list[dict], list[dict]]:
    """Fetch and normalize Garmin 15-min epochs and daily summaries across a list of dates."""
    all_epochs: list[dict] = []
    all_summaries: list[dict] = []

    has_steps = hasattr(client, "get_steps_data")
    has_stats = hasattr(client, "get_stats")
    has_hr = hasattr(client, "get_heart_rates")

    for d_str in dates:
        raw_steps = []
        raw_stats = {}
        raw_hr = {}

        if has_steps:
            try:
                res = client.get_steps_data(d_str)
                if isinstance(res, list):
                    raw_steps = res
            except Exception:
                raw_steps = []

        if has_stats:
            try:
                res = client.get_stats(d_str)
                if isinstance(res, dict):
                    raw_stats = res
            except Exception:
                raw_stats = {}

        if has_hr:
            try:
                res = client.get_heart_rates(d_str)
                if isinstance(res, (dict, list)):
                    raw_hr = res
            except Exception:
                raw_hr = {}

        day_epochs = normalize_garmin_day_epochs(
            local_date=d_str,
            raw_epochs=raw_steps,
            hr_payload=raw_hr,
            walk_windows=walk_windows,
        )
        all_epochs.extend(day_epochs)

        if raw_stats or day_epochs or raw_hr:
            summary = normalize_garmin_daily_summary(
                local_date=d_str,
                stats_payload=raw_stats,
                hr_payload=raw_hr if isinstance(raw_hr, dict) else None,
                epochs=day_epochs,
            )
            all_summaries.append(summary)

    return all_epochs, all_summaries
