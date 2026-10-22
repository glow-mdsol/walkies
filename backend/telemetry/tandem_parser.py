"""Tandem t:connect CSV parser and unified multi-provider CSV auto-detector."""

from __future__ import annotations

import csv
import io

from .carelink_normalizer import parse_carelink_csv
from .common import normalize_glucose_to_mmol, normalize_timestamp_utc
from .dexcom_client import parse_dexcom_clarity_csv


def parse_tandem_csv(
    csv_text: str,
    tz_offset_mins: int = 0,
) -> tuple[list[dict], str]:
    """Parse a Tandem t:connect CSV export into canonical `glycemic_events` and return `(events, detected_unit)`."""
    reader = csv.reader(io.StringIO(csv_text))
    header_map: dict[str, int] = {}
    detected_unit = "mg/dL"
    events: list[dict] = []

    for row in reader:
        if not row or not any(c.strip() for c in row):
            continue
        joined_lower = " ".join(c.strip().lower() for c in row)
        if (
            ("eventdatetime" in joined_lower or "timestamp" in joined_lower or ("date" in joined_lower and "time" in joined_lower))
            and (
                "readings (mg/dl)" in joined_lower
                or "readings (mmol/l)" in joined_lower
                or "cgm_glucose" in joined_lower
                or "egv" in joined_lower
                or "bolusvolume" in joined_lower
                or "insulin_delivered" in joined_lower
            )
        ):
            header_map = {c.strip().lower(): i for i, c in enumerate(row)}
            if "mmol/l" in joined_lower and "mg/dl" not in joined_lower:
                detected_unit = "mmol/L"
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

        ts_raw = _col("EventDateTime", "Timestamp", "CompletionDateTime")
        if not ts_raw:
            d_part = _col("Date")
            t_part = _col("Time")
            if d_part and t_part:
                ts_raw = f"{d_part} {t_part}"
        if not ts_raw:
            continue

        try:
            ts_utc, local_date, eff_tz = normalize_timestamp_utc(ts_raw, tz_offset_mins=tz_offset_mins)
        except ValueError:
            continue

        bg_str = _col(
            "Readings (mmol/L)",
            "Readings (mg/dL)",
            "CGM_Glucose (mmol/L)",
            "CGM_Glucose (mg/dL)",
            "EGV",
            "BG",
        )
        if bg_str:
            bg_mmol = normalize_glucose_to_mmol(bg_str, unit=detected_unit)
            if bg_mmol is not None:
                # Detect unit if auto-heuristic converted a >35 mg/dL reading
                try:
                    if float(bg_str) > 35.0:
                        detected_unit = "mg/dL"
                except ValueError:
                    pass
                events.append({
                    "timestamp_utc": ts_utc,
                    "event_type": "cgm",
                    "local_date": local_date,
                    "tz_offset_mins": eff_tz,
                    "bg_mmol": bg_mmol,
                    "source_provider": "tandem",
                })

        bolus_str = _col("BolusVolumeDelivered", "Insulin_Delivered", "StandardBolus", "BolusAmount")
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
                        "source_provider": "tandem",
                    })
            except ValueError:
                pass

        basal_str = _col("BasalRate", "ActualBasalRate", "CommandedBasalRate")
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
                        "source_provider": "tandem",
                    })
            except ValueError:
                pass

        carb_str = _col("CarbSize", "Carbs", "CarbAmount")
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
                        "source_provider": "tandem",
                    })
            except ValueError:
                pass

        ciq_str = _col("ControlIQMode", "PumpActivityMode", "EventType", "Description").upper()
        if "EXERCISE" in ciq_str:
            events.append({
                "timestamp_utc": ts_utc,
                "event_type": "pump_state",
                "local_date": local_date,
                "tz_offset_mins": eff_tz,
                "pump_state": "CONTROL_IQ_EXERCISE",
                "source_provider": "tandem",
            })
        elif "SUSPEND" in ciq_str:
            events.append({
                "timestamp_utc": ts_utc,
                "event_type": "pump_state",
                "local_date": local_date,
                "tz_offset_mins": eff_tz,
                "pump_state": "AUTO_SUSPEND",
                "source_provider": "tandem",
            })

    events.sort(key=lambda e: (e["timestamp_utc"], e["event_type"]))
    return events, detected_unit


def detect_and_parse_telemetry_csv(
    csv_text: str,
    provider: str = "auto",
    tz_offset_mins: int = 0,
) -> tuple[str, str, list[dict]]:
    """Auto-detect or parse a CareLink, Dexcom Clarity, or Tandem t:connect CSV extract.

    Returns `(detected_provider, source_unit, events)`.
    """
    clean_text = csv_text.lstrip("\ufeff")
    prov = (provider or "auto").strip().lower()

    if prov == "auto":
        lower_sample = clean_text[:8192].lower()
        if "timestamp (yyyy-mm-ddthh:mm:ss)" in lower_sample or (
            "event type" in lower_sample and "glucose value" in lower_sample
        ):
            prov = "dexcom"
        elif (
            "eventdatetime" in lower_sample
            or "bolusvolumedelivered" in lower_sample
            or "controliq" in lower_sample
            or "readings (mg/dl)" in lower_sample
        ):
            prov = "tandem"
        elif (
            "sensor glucose" in lower_sample
            or "bolus volume delivered (u)" in lower_sample
            or "bwz carb input" in lower_sample
        ):
            prov = "carelink"
        else:
            raise ValueError(
                "Unrecognized CSV telemetry format. Supported formats: Medtronic CareLink, Dexcom Clarity, and Tandem t:connect."
            )

    if prov == "carelink":
        events, unit = parse_carelink_csv(clean_text, tz_offset_mins=tz_offset_mins)
        return "carelink", unit, events
    if prov == "dexcom":
        events, unit = parse_dexcom_clarity_csv(clean_text, tz_offset_mins=tz_offset_mins)
        return "dexcom", unit, events
    if prov == "tandem":
        events, unit = parse_tandem_csv(clean_text, tz_offset_mins=tz_offset_mins)
        return "tandem", unit, events

    raise ValueError(f"Unsupported provider '{provider}'. Expected 'auto', 'carelink', 'dexcom', or 'tandem'.")
