"""Dexcom Share direct cloud client ('us' and 'ous' regions) and Dexcom Clarity CSV parser."""

from __future__ import annotations

import csv
import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from .common import normalize_glucose_to_mmol, normalize_timestamp_utc

DEXCOM_APP_ID = "d89443d2-327c-4a6f-89e5-496bbb0317db"
DEXCOM_BASE_URLS = {
    "us": "https://share2.dexcom.com/ShareWebServices/Services",
    "ous": "https://shareous1.dexcom.com/ShareWebServices/Services",
}

_DEXCOM_TREND_MAP = {
    0: "NONE",
    1: "DOUBLE_UP",
    2: "SINGLE_UP",
    3: "FORTY_FIVE_UP",
    4: "FLAT",
    5: "FORTY_FIVE_DOWN",
    6: "SINGLE_DOWN",
    7: "DOUBLE_DOWN",
    8: "NOT_COMPUTABLE",
    9: "RATE_OUT_OF_RANGE",
    "DoubleUp": "DOUBLE_UP",
    "SingleUp": "SINGLE_UP",
    "FortyFiveUp": "FORTY_FIVE_UP",
    "Flat": "FLAT",
    "FortyFiveDown": "FORTY_FIVE_DOWN",
    "SingleDown": "SINGLE_DOWN",
    "DoubleDown": "DOUBLE_DOWN",
}


class DexcomShareClient:
    """Direct cloud client for Dexcom Share supporting 'us' and 'ous' regional servers."""

    def __init__(
        self,
        username: str,
        password: str | None = None,
        region: str = "ous",
        token_path: Path | None = None,
    ) -> None:
        self.username = username.strip()
        self.password = password or ""
        self.region = "us" if str(region).strip().lower() == "us" else "ous"
        self.base_url = DEXCOM_BASE_URLS[self.region]
        self.token_path = token_path
        self.session_id: str | None = None
        self.account_id: str | None = None

        if self.token_path and self.token_path.exists():
            try:
                saved = json.loads(self.token_path.read_text(encoding="utf-8"))
                self.session_id = saved.get("session_id")
                self.account_id = saved.get("account_id")
                if not self.password and saved.get("password"):
                    self.password = saved["password"]
                if saved.get("region") in ("us", "ous"):
                    self.region = saved["region"]
                    self.base_url = DEXCOM_BASE_URLS[self.region]
            except Exception:
                pass

    def _save_token(self) -> None:
        if not self.token_path:
            return
        self.token_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "username": self.username,
            "password": self.password,
            "region": self.region,
            "account_id": self.account_id,
            "session_id": self.session_id,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.token_path.write_text(json.dumps(payload), encoding="utf-8")

    def authenticate(self) -> str:
        """Authenticate against Dexcom Share and persist session_id."""
        with httpx.Client(timeout=15.0) as client:
            auth_res = client.post(
                f"{self.base_url}/General/AuthenticatePublisherAccount",
                json={
                    "accountName": self.username,
                    "password": self.password,
                    "applicationId": DEXCOM_APP_ID,
                },
            )
            if auth_res.status_code >= 400:
                raise RuntimeError(f"Dexcom authentication failed ({auth_res.status_code})")
            account_id = str(auth_res.json()).strip().strip('"')
            if not account_id or account_id == "00000000-0000-0000-0000-000000000000":
                raise RuntimeError("Invalid Dexcom username or password.")
            self.account_id = account_id

            login_res = client.post(
                f"{self.base_url}/General/LoginPublisherAccountById",
                json={
                    "accountId": self.account_id,
                    "password": self.password,
                    "applicationId": DEXCOM_APP_ID,
                },
            )
            if login_res.status_code >= 400:
                raise RuntimeError(f"Dexcom session login failed ({login_res.status_code})")
            session_id = str(login_res.json()).strip().strip('"')
            if not session_id or session_id == "00000000-0000-0000-0000-000000000000":
                raise RuntimeError("Failed to establish Dexcom Share session.")
            self.session_id = session_id
            self._save_token()
            return session_id

    def fetch_glucose_readings(
        self,
        minutes: int = 10080,
        max_count: int = 2016,
        allowed_dates: set[str] | None = None,
    ) -> list[dict]:
        """Fetch latest CGM readings from Dexcom Share and normalize to canonical mmol/L."""
        if not self.session_id:
            self.authenticate()

        def _do_request(sess_id: str) -> httpx.Response:
            with httpx.Client(timeout=20.0) as client:
                return client.post(
                    f"{self.base_url}/Publisher/ReadPublisherLatestGlucoseValues",
                    params={
                        "sessionId": sess_id,
                        "minutes": min(max(15, int(minutes)), 43200),
                        "maxCount": min(max(1, int(max_count)), 8640),
                    },
                )

        res = _do_request(self.session_id)
        if res.status_code in (401, 500) and self.password:
            self.authenticate()
            res = _do_request(self.session_id)
        if res.status_code >= 400:
            raise RuntimeError(f"Dexcom Share read failed ({res.status_code})")

        raw_items = res.json()
        if not isinstance(raw_items, list):
            return []

        return normalize_dexcom_share_items(raw_items, allowed_dates=allowed_dates)


def _parse_dexcom_wt_timestamp(wt_str: str) -> tuple[str, str, int]:
    """Parse Dexcom Share Date(1729500000000+0100) or ISO timestamp into (utc_iso, local_date, tz_offset_mins)."""
    match = re.search(r"Date\((\d+)([+-]\d{4})?\)", str(wt_str))
    if match:
        ms = int(match.group(1))
        tz_part = match.group(2)
        tz_offset_mins = 0
        if tz_part:
            sign = 1 if tz_part[0] == "+" else -1
            tz_offset_mins = sign * (int(tz_part[1:3]) * 60 + int(tz_part[3:5]))
        dt_utc = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return normalize_timestamp_utc(dt_utc, tz_offset_mins=tz_offset_mins)
    return normalize_timestamp_utc(str(wt_str))


def normalize_dexcom_share_items(
    raw_items: list[dict],
    allowed_dates: set[str] | None = None,
) -> list[dict]:
    """Convert raw Dexcom Share glucose objects into canonical `glycemic_events`."""
    events: list[dict] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        wt = item.get("WT") or item.get("ST") or item.get("DT") or item.get("timestamp")
        val = item.get("Value") if item.get("Value") is not None else item.get("value")
        if not wt or val is None:
            continue
        try:
            ts_utc, local_date, eff_tz = _parse_dexcom_wt_timestamp(str(wt))
        except ValueError:
            continue
        if allowed_dates and local_date not in allowed_dates:
            continue

        bg_mmol = normalize_glucose_to_mmol(val, unit="mg/dL")
        if bg_mmol is None:
            continue

        raw_trend = item.get("Trend", item.get("trend"))
        trend_arrow = _DEXCOM_TREND_MAP.get(raw_trend, str(raw_trend) if raw_trend is not None else None)

        events.append({
            "timestamp_utc": ts_utc,
            "event_type": "cgm",
            "local_date": local_date,
            "tz_offset_mins": eff_tz,
            "bg_mmol": bg_mmol,
            "trend_arrow": trend_arrow,
            "source_provider": "dexcom",
        })

    events.sort(key=lambda e: e["timestamp_utc"])
    return events


def parse_dexcom_clarity_csv(
    csv_text: str,
    tz_offset_mins: int = 0,
) -> tuple[list[dict], str]:
    """Parse a Dexcom Clarity CSV export into canonical `glycemic_events` and return `(events, detected_unit)`."""
    reader = csv.reader(io.StringIO(csv_text))
    header_map: dict[str, int] = {}
    detected_unit = "mg/dL"
    events: list[dict] = []

    for row in reader:
        if not row or not any(c.strip() for c in row):
            continue
        joined_lower = " ".join(c.strip().lower() for c in row)
        if "timestamp" in joined_lower and "event type" in joined_lower and "glucose value" in joined_lower:
            header_map = {c.strip().lower(): i for i, c in enumerate(row)}
            if "glucose value (mmol/l)" in header_map:
                detected_unit = "mmol/L"
            elif "glucose value (mg/dl)" in header_map:
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

        ts_raw = _col(
            "Timestamp (YYYY-MM-DDThh:mm:ss)",
            "Timestamp",
            "Time",
        )
        ev_type_raw = _col("Event Type").lower()
        if not ts_raw or not ev_type_raw:
            continue

        try:
            ts_utc, local_date, eff_tz = normalize_timestamp_utc(ts_raw, tz_offset_mins=tz_offset_mins)
        except ValueError:
            continue

        if ev_type_raw in ("egv", "cgm", "calibration"):
            g_str = _col("Glucose Value (mmol/L)", "Glucose Value (mg/dL)", "Glucose Value")
            if g_str.lower() == "low":
                g_str = "39" if detected_unit == "mg/dL" else "2.2"
            elif g_str.lower() == "high":
                g_str = "401" if detected_unit == "mg/dL" else "22.2"
            bg_mmol = normalize_glucose_to_mmol(g_str, unit=detected_unit)
            if bg_mmol is not None:
                events.append({
                    "timestamp_utc": ts_utc,
                    "event_type": "cgm",
                    "local_date": local_date,
                    "tz_offset_mins": eff_tz,
                    "bg_mmol": bg_mmol,
                    "source_provider": "dexcom",
                })
        elif "insulin" in ev_type_raw or "bolus" in ev_type_raw:
            ins_str = _col("Insulin Value (u)", "Insulin Value", "Fast-Acting Insulin Value (u)")
            try:
                units = float(ins_str)
                if units > 0:
                    events.append({
                        "timestamp_utc": ts_utc,
                        "event_type": "bolus",
                        "local_date": local_date,
                        "tz_offset_mins": eff_tz,
                        "bolus_units": round(units, 2),
                        "source_provider": "dexcom",
                    })
            except ValueError:
                pass
        elif "carb" in ev_type_raw:
            carb_str = _col("Carb Value (grams)", "Carb Value (g)", "Carb Value")
            try:
                grams = float(carb_str)
                if grams > 0:
                    events.append({
                        "timestamp_utc": ts_utc,
                        "event_type": "carb",
                        "local_date": local_date,
                        "tz_offset_mins": eff_tz,
                        "carb_grams": round(grams, 1),
                        "source_provider": "dexcom",
                    })
            except ValueError:
                pass

    events.sort(key=lambda e: (e["timestamp_utc"], e["event_type"]))
    return events, detected_unit
