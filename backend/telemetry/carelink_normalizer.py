"""Medtronic CareLink 24-hour API payload and CSV normalizer into canonical glycemic_events."""

from __future__ import annotations

import csv
import io
from typing import Any

from .common import normalize_glucose_to_mmol, normalize_timestamp_utc


def normalize_carelink_api_payload(
    recent_data: dict,
    tz_offset_mins: int = 0,
    allowed_dates: set[str] | None = None,
) -> list[dict]:
    """Normalize CareLink API patientData (`sgs` and `markers`) into canonical `glycemic_events`."""
    if not isinstance(recent_data, dict):
        return []

    data = recent_data.get("patientData", recent_data)
    if not isinstance(data, dict):
        return []

    sgs = data.get("sgs") or []
    markers = data.get("markers") or []
    events: list[dict] = []

    for sg in sgs:
        if not isinstance(sg, dict):
            continue
        ts_raw = sg.get("timestamp") or sg.get("datetime")
        val_raw = sg.get("sg")
        if not ts_raw or val_raw is None:
            continue
        try:
            ts_utc, local_date, eff_tz = normalize_timestamp_utc(ts_raw, tz_offset_mins=tz_offset_mins)
        except ValueError:
            continue
        if allowed_dates and local_date not in allowed_dates:
            continue

        bg_mmol = normalize_glucose_to_mmol(val_raw, unit="mg/dL" if float(val_raw or 0) > 35.0 else "auto")
        if bg_mmol is None:
            continue

        events.append({
            "timestamp_utc": ts_utc,
            "event_type": "cgm",
            "local_date": local_date,
            "tz_offset_mins": eff_tz,
            "bg_mmol": bg_mmol,
            "trend_arrow": sg.get("trend") or sg.get("trendArrow"),
            "source_provider": "carelink",
        })

    for marker in markers:
        if not isinstance(marker, dict):
            continue
        ts_raw = marker.get("timestamp") or marker.get("dateTime")
        if not ts_raw:
            continue
        try:
            ts_utc, local_date, eff_tz = normalize_timestamp_utc(ts_raw, tz_offset_mins=tz_offset_mins)
        except ValueError:
            continue
        if allowed_dates and local_date not in allowed_dates:
            continue

        m_type = str(marker.get("type") or "").upper()
        if m_type in ("INSULIN", "BOLUS"):
            amt = marker.get("amount") or marker.get("programmedExtendedAmount") or marker.get("deliveredExtendedAmount")
            try:
                units = float(amt)
            except (TypeError, ValueError):
                units = 0.0
            if units > 0:
                events.append({
                    "timestamp_utc": ts_utc,
                    "event_type": "bolus",
                    "local_date": local_date,
                    "tz_offset_mins": eff_tz,
                    "bolus_units": round(units, 2),
                    "source_provider": "carelink",
                })
        elif m_type in ("MEAL", "CARB", "CARBS"):
            carbs = marker.get("amount") or marker.get("carbAmount")
            try:
                grams = float(carbs)
            except (TypeError, ValueError):
                grams = 0.0
            if grams > 0:
                events.append({
                    "timestamp_utc": ts_utc,
                    "event_type": "carb",
                    "local_date": local_date,
                    "tz_offset_mins": eff_tz,
                    "carb_grams": round(grams, 1),
                    "source_provider": "carelink",
                })
        elif m_type in ("AUTO_BASAL_DELIVERY", "BASAL"):
            rate_raw = marker.get("bolusAmount") or marker.get("rate") or marker.get("amount")
            try:
                rate = float(rate_raw)
            except (TypeError, ValueError):
                rate = -1.0
            if rate >= 0:
                events.append({
                    "timestamp_utc": ts_utc,
                    "event_type": "basal",
                    "local_date": local_date,
                    "tz_offset_mins": eff_tz,
                    "basal_rate_u_hr": round(rate, 3),
                    "source_provider": "carelink",
                })
        elif "SUSPEND" in m_type or "PLGM" in m_type or "PUMP" in m_type:
            state = "PLGM_PREDICTED_LOW_SG" if "PLGM" in m_type or "LOW" in m_type else "AUTO_SUSPEND"
            events.append({
                "timestamp_utc": ts_utc,
                "event_type": "pump_state",
                "local_date": local_date,
                "tz_offset_mins": eff_tz,
                "pump_state": state,
                "source_provider": "carelink",
            })

    events.sort(key=lambda e: (e["timestamp_utc"], e["event_type"]))
    return events


def parse_carelink_csv(
    csv_text: str,
    tz_offset_mins: int = 0,
) -> tuple[list[dict], str]:
    """Parse a Medtronic CareLink CSV file into canonical `glycemic_events` and return `(events, detected_unit)`."""
    reader = csv.reader(io.StringIO(csv_text))
    headers: list[str] = []
    header_map: dict[str, int] = {}
    detected_unit = "mmol/L"
    events: list[dict] = []

    for row in reader:
        if not row or not any(cell.strip() for cell in row):
            continue
        joined_lower = " ".join(c.strip().lower() for c in row)
        if "date" in joined_lower and "time" in joined_lower and (
            "sensor glucose" in joined_lower
            or "bg reading" in joined_lower
            or "bolus volume" in joined_lower
            or "basal rate" in joined_lower
        ):
            headers = [c.strip() for c in row]
            header_map = {h.lower(): idx for idx, h in enumerate(headers)}
            if "mg/dl" in joined_lower and "mmol/l" not in joined_lower:
                detected_unit = "mg/dL"
            continue

        if not header_map:
            continue

        def _col(*names: str) -> str:
            for n in names:
                idx = header_map.get(n.lower())
                if idx is not None and idx < len(row):
                    val = row[idx].strip()
                    if val != "":
                        return val
            return ""

        date_str = _col("Date")
        time_str = _col("Time")
        if not date_str or not time_str:
            continue

        try:
            ts_utc, local_date, eff_tz = normalize_timestamp_utc(
                f"{date_str} {time_str}", tz_offset_mins=tz_offset_mins
            )
        except ValueError:
            continue

        sg_mmol_str = _col("Sensor Glucose (mmol/L)", "BG Reading (mmol/L)")
        sg_mgdl_str = _col("Sensor Glucose (mg/dL)", "BG Reading (mg/dL)")
        if sg_mmol_str:
            bg_mmol = normalize_glucose_to_mmol(sg_mmol_str, unit="mmol/L")
        elif sg_mgdl_str:
            detected_unit = "mg/dL"
            bg_mmol = normalize_glucose_to_mmol(sg_mgdl_str, unit="mg/dL")
        else:
            bg_mmol = None

        if bg_mmol is not None:
            events.append({
                "timestamp_utc": ts_utc,
                "event_type": "cgm",
                "local_date": local_date,
                "tz_offset_mins": eff_tz,
                "bg_mmol": bg_mmol,
                "source_provider": "carelink",
            })

        bolus_str = _col("Bolus Volume Delivered (U)", "Bolus Volume Selected (U)")
        if bolus_str:
            try:
                b_val = float(bolus_str)
                if b_val > 0:
                    events.append({
                        "timestamp_utc": ts_utc,
                        "event_type": "bolus",
                        "local_date": local_date,
                        "tz_offset_mins": eff_tz,
                        "bolus_units": round(b_val, 2),
                        "source_provider": "carelink",
                    })
            except ValueError:
                pass

        basal_str = _col("Basal Rate (U/h)")
        if basal_str:
            try:
                bas_val = float(basal_str)
                if bas_val >= 0:
                    events.append({
                        "timestamp_utc": ts_utc,
                        "event_type": "basal",
                        "local_date": local_date,
                        "tz_offset_mins": eff_tz,
                        "basal_rate_u_hr": round(bas_val, 3),
                        "source_provider": "carelink",
                    })
            except ValueError:
                pass

        carb_str = _col("BWZ Carb Input (grams)", "BWZ Carb Input (g)", "Meal (grams)")
        if carb_str:
            try:
                c_val = float(carb_str)
                if c_val > 0:
                    events.append({
                        "timestamp_utc": ts_utc,
                        "event_type": "carb",
                        "local_date": local_date,
                        "tz_offset_mins": eff_tz,
                        "carb_grams": round(c_val, 1),
                        "source_provider": "carelink",
                    })
            except ValueError:
                pass

        suspend_str = _col("Suspend").upper()
        if suspend_str:
            if "PLGM" in suspend_str or "PREDICTED_LOW" in suspend_str:
                p_state = "PLGM_PREDICTED_LOW_SG"
            elif "NORMAL_PUMPING" in suspend_str or "RESUME" in suspend_str:
                p_state = "NORMAL_PUMPING"
            elif "USER_SUSPEND" in suspend_str or "MANUAL" in suspend_str:
                p_state = "MANUAL_SUSPEND"
            else:
                p_state = "AUTO_SUSPEND"
            events.append({
                "timestamp_utc": ts_utc,
                "event_type": "pump_state",
                "local_date": local_date,
                "tz_offset_mins": eff_tz,
                "pump_state": p_state,
                "source_provider": "carelink",
            })

    events.sort(key=lambda e: (e["timestamp_utc"], e["event_type"]))
    return events, detected_unit
