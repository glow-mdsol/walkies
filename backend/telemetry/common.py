"""Common unit conversion and timestamp normalization helpers for multi-provider telemetry."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

MGDL_PER_MMOL: float = 18.0182


def mgdl_to_mmol(val_mgdl: float | int | None) -> float | None:
    """Convert a glucose reading in mg/dL to canonical mmol/L rounded to 2 decimal places."""
    if val_mgdl is None:
        return None
    try:
        fval = float(val_mgdl)
    except (TypeError, ValueError):
        return None
    if fval <= 0:
        return None
    return round(fval / MGDL_PER_MMOL, 2)


def mmol_to_mgdl(val_mmol: float | int | None) -> float | None:
    """Convert a canonical mmol/L reading to mg/dL rounded to the nearest integer (as float)."""
    if val_mmol is None:
        return None
    try:
        fval = float(val_mmol)
    except (TypeError, ValueError):
        return None
    if fval <= 0:
        return None
    return float(round(fval * MGDL_PER_MMOL))


def normalize_glucose_to_mmol(value: float | int | str | None, unit: str | None = None) -> float | None:
    """Normalize a glucose reading to canonical mmol/L.

    If `unit` is explicitly `'mg/dL'` (or `'mgdl'`), divides by 18.0182.
    If `unit` is `'mmol/L'` (or `'mmol'`), rounds to 2 decimals.
    If `unit` is `'auto'` or `None`, values > 35.0 are treated as `mg/dL` and converted.
    """
    if value is None or value == "":
        return None
    try:
        fval = float(value)
    except (TypeError, ValueError):
        return None
    if fval <= 0:
        return None

    norm_unit = (unit or "auto").strip().lower()
    if norm_unit in ("mg/dl", "mgdl", "mg"):
        mmol = round(fval / MGDL_PER_MMOL, 2)
    elif norm_unit in ("mmol/l", "mmol"):
        mmol = round(fval, 2)
    else:
        # Heuristic auto-detection: physiological CGM values > 35.0 are in mg/dL
        if fval > 35.0:
            mmol = round(fval / MGDL_PER_MMOL, 2)
        else:
            mmol = round(fval, 2)

    if mmol < 1.0 or mmol > 35.0:
        return None
    return mmol


def normalize_timestamp_utc(
    ts: str | datetime,
    tz_offset_mins: int = 0,
    default_tz_offset_mins: int | None = None,
) -> tuple[str, str, int]:
    """Normalize a timestamp string or datetime into `(timestamp_utc_iso, local_date_yyyy_mm_dd, tz_offset_mins)`.

    - If `ts` includes an explicit timezone offset, preserves that offset for `local_date` and `tz_offset_mins`
      and converts `timestamp_utc_iso` to UTC (`...Z`).
    - If `ts` is naive (no timezone), treats it as wall-clock local time with `tz_offset_mins` and shifts to UTC.
    """
    if default_tz_offset_mins is not None and tz_offset_mins == 0:
        tz_offset_mins = default_tz_offset_mins
    if isinstance(ts, str):
        raw = ts.strip()
        if raw.endswith("Z"):
            dt = datetime.fromisoformat(raw[:-1] + "+00:00")
        else:
            try:
                dt = datetime.fromisoformat(raw)
            except ValueError:
                # Fallback for common CSV formats: YYYY/MM/DD HH:MM:SS or YYYY-MM-DD HH:MM:SS
                cleaned = raw.replace("/", "-")
                dt = datetime.fromisoformat(cleaned)
    elif isinstance(ts, datetime):
        dt = ts
    else:
        raise ValueError(f"Unsupported timestamp value: {ts!r}")

    if dt.tzinfo is not None:
        utcoff = dt.utcoffset() or timedelta(0)
        detected_offset_mins = int(round(utcoff.total_seconds() / 60.0))
        if detected_offset_mins == 0 and tz_offset_mins != 0:
            # Timestamp was supplied in UTC, apply caller-supplied local offset for local_date
            local_dt = dt.astimezone(timezone.utc) + timedelta(minutes=tz_offset_mins)
            effective_offset = tz_offset_mins
        else:
            local_dt = dt
            effective_offset = detected_offset_mins
        utc_dt = dt.astimezone(timezone.utc)
    else:
        effective_offset = int(tz_offset_mins or 0)
        local_tz = timezone(timedelta(minutes=effective_offset))
        local_dt = dt.replace(tzinfo=local_tz)
        utc_dt = local_dt.astimezone(timezone.utc)

    utc_iso = utc_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    local_date = local_dt.strftime("%Y-%m-%d")
    return utc_iso, local_date, effective_offset
