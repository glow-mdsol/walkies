import json
import csv
import hashlib
import math
import os
import re
import secrets
import shutil
import struct
import time
import urllib.parse
import urllib.request
import uuid
from datetime import date as date_type, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional
import xml.etree.ElementTree as ET

from fastapi import BackgroundTasks, Body, Depends, FastAPI, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.responses import JSONResponse
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token

from analytics import (
    ANALYTICS_VERSION,
    create_share_link,
    delete_walk_analytics,
    get_cached_analytics_row,
    get_share_link,
    init_analytics_db,
    list_all_analytics_rows,
    list_walk_analytics as list_cached_walk_analytics,
    list_share_links_for_walk,
    persist_walk_analytics,
    refresh_walk_analytics_if_needed,
    revoke_share_link,
    upsert_share_link,
    get_user_settings,
    upsert_user_settings,
    delete_user_data,
    insert_system_log,
    query_system_logs,
    get_logs_summary,
    clear_system_logs,
    update_walk_id_references,
    migrate_walk_ids_to_uuids,
    WalkiesDBLogHandler,
    log_system_event,
    list_share_links_for_target,
    get_or_compute_daily_analytics,
    get_or_compute_daily_range_analytics,
    upsert_activity_epochs,
    get_activity_epochs_for_date,
    upsert_daily_wellness_summary,
    get_daily_wellness_summary,
    upsert_glycemic_events,
    get_glycemic_events_for_date,
    get_glycemic_events_for_window,
)
from telemetry.garmin_epochs import (
    fetch_garmin_epochs_for_dates,
    resolve_sync_date_range,
)
from telemetry.carelink_normalizer import (
    normalize_carelink_api_payload,
    parse_carelink_csv,
)
from telemetry.dexcom_client import (
    DexcomShareClient,
    parse_dexcom_clarity_csv,
)
from telemetry.tandem_parser import (
    detect_and_parse_telemetry_csv,
    parse_tandem_csv,
)

import logging
import sys

# Configure standard application logger
LOG_LEVEL_NAME = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_LEVEL = getattr(logging, LOG_LEVEL_NAME, logging.INFO)

logging.basicConfig(
    level=LOG_LEVEL,
    format="[%(asctime)s] [%(levelname)s] [%(name)s] [%(filename)s:%(lineno)d] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("walkies")
db_log_handler = WalkiesDBLogHandler()
db_log_handler.setLevel(LOG_LEVEL)
logger.addHandler(db_log_handler)


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "").strip()
DEV_BYPASS_AUTH = os.getenv("WALKIES_DEV_BYPASS_AUTH", "").strip() == "1"
AUTH_SCHEME = HTTPBearer(auto_error=False)

FIT_EPOCH = datetime(1989, 12, 31, tzinfo=timezone.utc)
BG_CONTEXT_WINDOW = timedelta(minutes=30)
init_analytics_db()
try:
    migrate_walk_ids_to_uuids(DATA_DIR)
except Exception as _e:
    logger.warning(f"Initial walk UUID migration encountered error: {_e}")



def _safe_user_token(user_sub: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_-]+', '-', user_sub).strip('-') or 'user'


def _user_data_dir(user_sub: str) -> Path:
    user_dir = DATA_DIR / _safe_user_token(user_sub)
    user_dir.mkdir(exist_ok=True)
    return user_dir


def _auth_error(detail: str = "Authentication required") -> HTTPException:
    return HTTPException(status_code=401, detail=detail)


def _require_user(credentials: HTTPAuthorizationCredentials = Depends(AUTH_SCHEME)) -> dict:
    if DEV_BYPASS_AUTH:
        return {
            "sub": "dev-user",
            "email": "dev-user@local",
            "name": "Dev User",
        }

    if credentials is None or credentials.scheme.lower() != "bearer" or not credentials.credentials:
        raise _auth_error()

    if not GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=500, detail="GOOGLE_CLIENT_ID is not configured")

    try:
        info = id_token.verify_oauth2_token(
            credentials.credentials,
            google_requests.Request(),
            GOOGLE_CLIENT_ID,
        )
    except Exception:
        raise _auth_error("Invalid Google token")

    user_sub = str(info.get("sub") or "").strip()
    if not user_sub:
        raise _auth_error("Token missing subject")

    return {
        "sub": user_sub,
        "email": info.get("email"),
        "name": info.get("name"),
    }


def _parse_date(date_str: str) -> str:
    try:
        date_type.fromisoformat(date_str)
        return date_str
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format, expected YYYY-MM-DD")


def _slugify(text: str) -> str:
    slug = re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')
    return slug or 'walk'


def _walk_date_for_dir(walk_dir: Path, data_root: Path = DATA_DIR) -> str:
    return walk_dir.parent.name if walk_dir.parent != data_root else walk_dir.name


def _iter_walk_dirs(data_root: Path = DATA_DIR) -> list[Path]:
    walk_dirs: list[Path] = []
    if not data_root.exists():
        return walk_dirs

    for date_dir in sorted(data_root.iterdir(), reverse=True):
        if not date_dir.is_dir():
            continue
        child_dirs = sorted((child for child in date_dir.iterdir() if child.is_dir()), reverse=True)
        has_files = any(child.is_file() for child in date_dir.iterdir())
        if has_files:
            walk_dirs.append(date_dir)
        walk_dirs.extend(child_dirs)
    return walk_dirs


def _find_walk_dir(walk_id: str, data_root: Path = DATA_DIR) -> Path | None:
    for walk_dir in _iter_walk_dirs(data_root):
        if walk_dir.name == walk_id:
            return walk_dir
        meta_file = walk_dir / "meta.json"
        if meta_file.exists():
            try:
                meta = json.loads(meta_file.read_text(encoding="utf-8"))
                if meta.get("id") == walk_id or meta.get("legacy_id") == walk_id:
                    return walk_dir
            except Exception:
                pass
    return None


def _backfill_walk_meta(walk_dir: Path, meta: dict) -> dict:
    meta_file = walk_dir / "meta.json"
    updated_meta = dict(meta)
    changed = False

    if not updated_meta.get("fit_identity"):
        fit_files = sorted(walk_dir.glob('*.fit'))
        if fit_files:
            fit_identity = _fit_file_identity(fit_files[0].read_bytes())
            if fit_identity is not None:
                updated_meta["fit_identity"] = fit_identity
                changed = True

    if not updated_meta.get("carelink_start_time") or not updated_meta.get("carelink_end_time"):
        csv_files = sorted(walk_dir.glob('*.csv'))
        if csv_files:
            carelink_start_dt, carelink_end_dt = _carelink_time_bounds(csv_files[0])
            if carelink_start_dt is not None and carelink_end_dt is not None:
                updated_meta["carelink_start_time"] = carelink_start_dt.isoformat()
                updated_meta["carelink_end_time"] = carelink_end_dt.isoformat()
                changed = True

    if changed:
        meta_file.write_text(json.dumps(updated_meta), encoding="utf-8")
    return updated_meta


def _load_walk_meta(walk_dir: Path) -> dict:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", walk_dir.parent.name or ""):
        data_root = walk_dir.parent.parent
    else:
        data_root = walk_dir.parent
    date = _walk_date_for_dir(walk_dir, data_root)
    meta_file = walk_dir / "meta.json"
    meta: dict = {}
    if meta_file.exists():
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    meta = _backfill_walk_meta(walk_dir, meta)
    files = sorted(f.name for f in walk_dir.iterdir() if f.is_file() and f.name != "meta.json")
    walk_id = meta.get("id") or walk_dir.name
    return {
        "id": walk_id,
        "legacy_id": meta.get("legacy_id"),
        "date": meta.get("date", date),
        "name": meta.get("name", ""),
        "rating": meta.get("rating", 3),
        "start_time": meta.get("start_time"),
        "fit_identity": meta.get("fit_identity"),
        "carelink_start_time": meta.get("carelink_start_time"),
        "carelink_end_time": meta.get("carelink_end_time"),
        "garmin_activity_id": meta.get("garmin_activity_id"),
        "files": files,
    }


def _make_walk_id(date: str | None = None, fit_start_dt: datetime | None = None, name: str | None = None, date_dir: Path | None = None, user_sub: str | None = None) -> str:
    return str(uuid.uuid4())



def _fit_start_datetime(data: bytes) -> datetime | None:
    """Return the first timestamp from a FIT file, or None."""
    try:
        if len(data) < 12 or data[8:12] != b'.FIT':
            return None

        header_size = data[0]
        data_size = struct.unpack_from('<I', data, 4)[0]
        pos = header_size
        end = min(header_size + data_size, len(data))

        definitions: dict = {}

        while pos < end:
            header_byte = data[pos]
            pos += 1

            if header_byte & 0x80:
                local_num = (header_byte >> 5) & 0x03
                if local_num not in definitions:
                    break
                pos += sum(fs for _, fs, _ in definitions[local_num])
            else:
                local_num = header_byte & 0x0F
                if header_byte & 0x40:
                    pos += 1
                    little_endian = data[pos] == 0
                    pos += 3
                    num_fields = data[pos]
                    pos += 1
                    fields = []
                    for _ in range(num_fields):
                        fields.append((data[pos], data[pos + 1], little_endian))
                        pos += 3
                    definitions[local_num] = fields
                else:
                    if local_num not in definitions:
                        break
                    ts_value = None
                    for fnum, fsize, little_endian in definitions[local_num]:
                        if fnum == 253 and fsize == 4:
                            fmt = '<I' if little_endian else '>I'
                            ts_value = struct.unpack_from(fmt, data, pos)[0]
                        pos += fsize
                    if ts_value is not None:
                        return FIT_EPOCH + timedelta(seconds=ts_value)
    except Exception:
        pass
    return None


def _fit_time_bounds(data: bytes) -> tuple[datetime | None, datetime | None]:
    parsed = _parse_fit_records(data)
    points = parsed.get('points', [])
    if points:
        start_dt = datetime.fromisoformat(points[0]['timestamp_iso'])
        end_dt = datetime.fromisoformat(points[-1]['timestamp_iso'])
        return start_dt, end_dt

    start_dt = _fit_start_datetime(data)
    return start_dt, start_dt


def _fit_file_identity(data: bytes) -> str | None:
    """Return a stable identity from the FIT file_id message, or None."""
    try:
        if len(data) < 12 or data[8:12] != b'.FIT':
            return None

        header_size = data[0]
        data_size = struct.unpack_from('<I', data, 4)[0]
        pos = header_size
        end = min(header_size + data_size, len(data))

        definitions: dict[int, dict] = {}

        while pos < end:
            header_byte = data[pos]
            pos += 1

            if header_byte & 0x80:
                local_num = (header_byte >> 5) & 0x03
                definition = definitions.get(local_num)
                if definition is None:
                    break
                pos += sum(field_size for _, field_size, _ in definition['fields'])
                continue

            local_num = header_byte & 0x0F
            if header_byte & 0x40:
                if pos + 5 > end:
                    break
                pos += 1
                little_endian = data[pos] == 0
                pos += 1
                global_msg_num = _read_u16(data, pos, little_endian)
                pos += 2
                num_fields = data[pos]
                pos += 1

                fields = []
                for _ in range(num_fields):
                    if pos + 3 > end:
                        pos = end
                        break
                    fields.append((data[pos], data[pos + 1], little_endian))
                    pos += 3

                if header_byte & 0x20:
                    if pos >= end:
                        break
                    num_dev_fields = data[pos]
                    pos += 1
                    pos += 3 * num_dev_fields

                definitions[local_num] = {"global_msg_num": global_msg_num, "fields": fields}
                continue

            definition = definitions.get(local_num)
            if definition is None:
                break

            fields = definition['fields']
            if definition['global_msg_num'] != 0:
                pos += sum(field_size for _, field_size, _ in fields)
                continue

            file_type = None
            manufacturer = None
            product = None
            serial_number = None
            time_created = None

            for field_num, field_size, little_endian in fields:
                if pos + field_size > end:
                    pos = end
                    break
                if field_num == 0 and field_size == 1:
                    file_type = data[pos]
                elif field_num == 1 and field_size == 2:
                    manufacturer = _read_u16(data, pos, little_endian)
                elif field_num == 2 and field_size == 2:
                    product = _read_u16(data, pos, little_endian)
                elif field_num == 2 and field_size == 4:
                    product = _read_u32(data, pos, little_endian)
                elif field_num == 3 and field_size == 4:
                    serial_number = _read_u32(data, pos, little_endian)
                elif field_num == 4 and field_size == 4:
                    time_created = _read_u32(data, pos, little_endian)
                pos += field_size

            if serial_number is None or time_created is None:
                return None

            parts = [
                str(file_type if file_type is not None else ""),
                str(manufacturer if manufacturer is not None else ""),
                str(product if product is not None else ""),
                str(serial_number),
                str(time_created),
            ]
            return ":".join(parts)
    except Exception:
        pass
    return None


def _fit_start_date(data: bytes) -> str | None:
    """Return the start date (YYYY-MM-DD) from a FIT file, or None."""
    dt = _fit_start_datetime(data)
    return dt.strftime('%Y-%m-%d') if dt is not None else None


def _read_u16(data: bytes, pos: int, little_endian: bool) -> int:
    fmt = '<H' if little_endian else '>H'
    return struct.unpack_from(fmt, data, pos)[0]


def _read_u32(data: bytes, pos: int, little_endian: bool) -> int:
    fmt = '<I' if little_endian else '>I'
    return struct.unpack_from(fmt, data, pos)[0]


def _read_s32(data: bytes, pos: int, little_endian: bool) -> int:
    fmt = '<i' if little_endian else '>i'
    return struct.unpack_from(fmt, data, pos)[0]


def _safe_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_carelink_datetime(text: str | None) -> datetime | None:
    if not text:
        return None
    cleaned = text.strip()
    if not cleaned:
        return None

    for fmt in ('%Y/%m/%d %H:%M:%S', '%d/%m/%Y %H:%M:%S'):
        try:
            return datetime.strptime(cleaned, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue

    try:
        parsed = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _parse_fit_records(data: bytes) -> dict:
    if len(data) < 12 or data[8:12] != b'.FIT':
        return {'points': [], 'track': []}

    header_size = data[0]
    data_size = struct.unpack_from('<I', data, 4)[0]
    pos = header_size
    end = min(header_size + data_size, len(data))

    definitions: dict[int, dict] = {}
    last_timestamp: int | None = None
    points: list[dict] = []

    while pos < end:
        header_byte = data[pos]
        pos += 1

        if header_byte & 0x80:
            local_num = (header_byte >> 5) & 0x03
            if local_num not in definitions:
                break
            definition = definitions[local_num]
            fields = definition['fields']
            is_record = definition['global_msg_num'] == 20
            compressed_offset = header_byte & 0x1F
            timestamp = None
            if last_timestamp is not None:
                candidate = (last_timestamp & ~0x1F) | compressed_offset
                if candidate < last_timestamp:
                    candidate += 0x20
                timestamp = candidate
                last_timestamp = candidate

            entry = {'timestamp': timestamp, 'lat': None, 'lon': None, 'hr': None, 'distance_m': None, 'altitude_m': None}
            for field_num, field_size, little_endian in fields:
                if pos + field_size > end:
                    pos = end
                    break
                if is_record:
                    if field_num == 0 and field_size == 4:
                        raw = _read_s32(data, pos, little_endian)
                        if raw != 0x7FFFFFFF:
                            entry['lat'] = raw * 180.0 / (2 ** 31)
                    elif field_num == 1 and field_size == 4:
                        raw = _read_s32(data, pos, little_endian)
                        if raw != 0x7FFFFFFF:
                            entry['lon'] = raw * 180.0 / (2 ** 31)
                    elif field_num == 2 and field_size == 2:
                        raw = _read_u16(data, pos, little_endian)
                        if raw != 0xFFFF:
                            entry['altitude_m'] = (raw / 5.0) - 500.0
                    elif field_num == 78 and field_size == 4:
                        raw = _read_u32(data, pos, little_endian)
                        if raw != 0xFFFFFFFF:
                            entry['altitude_m'] = (raw / 5.0) - 500.0
                    elif field_num == 3 and field_size == 1:
                        raw = data[pos]
                        if raw != 0xFF:
                            entry['hr'] = raw
                    elif field_num == 5 and field_size == 4:
                        raw = _read_u32(data, pos, little_endian)
                        if raw != 0xFFFFFFFF:
                            entry['distance_m'] = raw / 100.0
                pos += field_size

            if is_record and entry['timestamp'] is not None:
                dt = FIT_EPOCH + timedelta(seconds=entry['timestamp'])
                entry['timestamp_iso'] = dt.isoformat()
                points.append(entry)
            continue

        local_num = header_byte & 0x0F
        if header_byte & 0x40:
            if pos + 5 > end:
                break
            pos += 1
            little_endian = data[pos] == 0
            pos += 1
            global_msg_num = _read_u16(data, pos, little_endian)
            pos += 2
            num_fields = data[pos]
            pos += 1

            fields = []
            for _ in range(num_fields):
                if pos + 3 > end:
                    pos = end
                    break
                field_num = data[pos]
                field_size = data[pos + 1]
                fields.append((field_num, field_size, little_endian))
                pos += 3

            if header_byte & 0x20:
                if pos >= end:
                    break
                num_dev_fields = data[pos]
                pos += 1
                pos += 3 * num_dev_fields

            definitions[local_num] = {'global_msg_num': global_msg_num, 'fields': fields}
            continue

        if local_num not in definitions:
            break

        definition = definitions[local_num]
        fields = definition['fields']
        is_record = definition['global_msg_num'] == 20
        timestamp = None
        entry = {'timestamp': None, 'lat': None, 'lon': None, 'hr': None, 'distance_m': None, 'altitude_m': None}

        for field_num, field_size, little_endian in fields:
            if pos + field_size > end:
                pos = end
                break
            if field_num == 253 and field_size == 4:
                raw = _read_u32(data, pos, little_endian)
                if raw != 0xFFFFFFFF:
                    timestamp = raw
                    last_timestamp = raw
            elif is_record:
                if field_num == 0 and field_size == 4:
                    raw = _read_s32(data, pos, little_endian)
                    if raw != 0x7FFFFFFF:
                        entry['lat'] = raw * 180.0 / (2 ** 31)
                elif field_num == 1 and field_size == 4:
                    raw = _read_s32(data, pos, little_endian)
                    if raw != 0x7FFFFFFF:
                        entry['lon'] = raw * 180.0 / (2 ** 31)
                elif field_num == 2 and field_size == 2:
                    raw = _read_u16(data, pos, little_endian)
                    if raw != 0xFFFF:
                        entry['altitude_m'] = (raw / 5.0) - 500.0
                elif field_num == 78 and field_size == 4:
                    raw = _read_u32(data, pos, little_endian)
                    if raw != 0xFFFFFFFF:
                        entry['altitude_m'] = (raw / 5.0) - 500.0
                elif field_num == 3 and field_size == 1:
                    raw = data[pos]
                    if raw != 0xFF:
                        entry['hr'] = raw
                elif field_num == 5 and field_size == 4:
                    raw = _read_u32(data, pos, little_endian)
                    if raw != 0xFFFFFFFF:
                        entry['distance_m'] = raw / 100.0
            pos += field_size

        if is_record and timestamp is not None:
            entry['timestamp'] = timestamp
            dt = FIT_EPOCH + timedelta(seconds=timestamp)
            entry['timestamp_iso'] = dt.isoformat()
            points.append(entry)

    points.sort(key=lambda p: p['timestamp'])
    track = [[p['lat'], p['lon'], p['hr']] for p in points if p['lat'] is not None and p['lon'] is not None and not (abs(p['lat']) < 1.0 and abs(p['lon']) < 1.0)]
    return {'points': points, 'track': track}


def _parse_gpx_track(gpx_path: Path) -> list[list[float]]:
    try:
        tree = ET.parse(gpx_path)
        root = tree.getroot()
    except Exception:
        return []

    ns = {'gpx': 'http://www.topografix.com/GPX/1/1'}
    points: list[list[float]] = []
    for node in root.findall('.//gpx:trkpt', ns):
        lat = _safe_float(node.attrib.get('lat'))
        lon = _safe_float(node.attrib.get('lon'))
        if lat is None or lon is None:
            continue
        points.append([lat, lon])
    return points


def _parse_carelink_csv(csv_path: Path) -> dict:
    rows = []
    with csv_path.open('r', encoding='utf-8-sig', errors='ignore', newline='') as f:
        reader = csv.reader(f)
        header = None
        for row in reader:
            if row and row[0] == 'Index' and 'Date' in row and 'Time' in row:
                header = row
                break
        if header is None:
            return {'bg': [], 'basal': [], 'bolus': []}

        indexes = {name: idx for idx, name in enumerate(header)}
        for row in reader:
            if len(row) < 3:
                continue
            date_text = row[indexes.get('Date', 1)] if indexes.get('Date', 1) < len(row) else ''
            time_text = row[indexes.get('Time', 2)] if indexes.get('Time', 2) < len(row) else ''
            if not date_text or not time_text:
                continue
            try:
                ts = datetime.strptime(f'{date_text} {time_text}', '%Y/%m/%d %H:%M:%S').replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            rows.append((ts, row, indexes))

    bg_points = []
    basal_points = []
    bolus_events = []
    smartguard_events = []

    for ts, row, indexes in rows:
        iso = ts.isoformat()

        sensor_col = indexes.get('Sensor Glucose (mmol/L)')
        meter_col = indexes.get('BG Reading (mmol/L)')
        sensor_bg = _safe_float(row[sensor_col]) if sensor_col is not None and sensor_col < len(row) else None
        meter_bg = _safe_float(row[meter_col]) if meter_col is not None and meter_col < len(row) else None
        if sensor_bg is not None or meter_bg is not None:
            bg_value = sensor_bg if sensor_bg is not None else meter_bg
            bg_points.append({'timestamp': iso, 'bg': bg_value})

        basal_col = indexes.get('Basal Rate (U/h)')
        basal_rate = _safe_float(row[basal_col]) if basal_col is not None and basal_col < len(row) else None
        if basal_rate is not None:
            basal_points.append({'timestamp': iso, 'rate': basal_rate})

        bolus_col = indexes.get('Bolus Volume Delivered (U)')
        bolus_units = _safe_float(row[bolus_col]) if bolus_col is not None and bolus_col < len(row) else None
        if bolus_units is not None and bolus_units > 0:
            bolus_events.append({'timestamp': iso, 'units': bolus_units})

        suspend_col = indexes.get('Suspend')
        suspend_value = row[suspend_col].strip() if suspend_col is not None and suspend_col < len(row) and row[suspend_col] else ''
        if suspend_value:
            upper = suspend_value.upper()
            if upper == 'NORMAL_PUMPING':
                state = 'resume'
            elif 'PLGM_PREDICTED_LOW_SG' in upper or 'PREDICTED_LOW' in upper:
                state = 'predicted_low_suspend'
            elif 'SUSPEND' in upper:
                state = 'suspend'
            else:
                state = 'other'
            smartguard_events.append({'timestamp': iso, 'raw_state': suspend_value, 'state': state})

    bg_points.sort(key=lambda x: x['timestamp'])
    basal_points.sort(key=lambda x: x['timestamp'])
    bolus_events.sort(key=lambda x: x['timestamp'])
    smartguard_events.sort(key=lambda x: x['timestamp'])

    return {'bg': bg_points, 'basal': basal_points, 'bolus': bolus_events, 'smartguard': smartguard_events}


def _smartguard_summary(activity_points: list[dict], smartguard_events: list[dict]) -> dict:
    if not activity_points:
        return {
            'event_count': 0,
            'predicted_low_suspend_count': 0,
            'suspend_count': 0,
            'resume_count': 0,
            'any_activity': False,
        }

    walk_start = datetime.fromisoformat(activity_points[0]['timestamp_iso'])
    walk_end = datetime.fromisoformat(activity_points[-1]['timestamp_iso'])

    event_count = 0
    predicted_low_suspend_count = 0
    suspend_count = 0
    resume_count = 0

    for event in smartguard_events:
        try:
            ts = datetime.fromisoformat(event['timestamp'])
        except (KeyError, ValueError):
            continue
        if ts < walk_start or ts > walk_end:
            continue

        event_count += 1
        state = event.get('state')
        if state == 'predicted_low_suspend':
            predicted_low_suspend_count += 1
        elif state == 'suspend':
            suspend_count += 1
        elif state == 'resume':
            resume_count += 1

    return {
        'event_count': event_count,
        'predicted_low_suspend_count': predicted_low_suspend_count,
        'suspend_count': suspend_count,
        'resume_count': resume_count,
        'any_activity': event_count > 0,
    }


def _carelink_time_bounds(csv_path: Path) -> tuple[datetime | None, datetime | None]:
    parsed = _parse_carelink_csv(csv_path)
    timestamps: list[datetime] = []
    for series_name in ('bg', 'basal', 'bolus'):
        for point in parsed[series_name]:
            point_dt = _parse_carelink_datetime(point.get('timestamp'))
            if point_dt is not None:
                timestamps.append(point_dt)

    if timestamps:
        return min(timestamps), max(timestamps)

    try:
        with csv_path.open('r', encoding='utf-8-sig', errors='ignore', newline='') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            values = next(reader, None)
        if not header or not values:
            return None, None

        indexes = {name: idx for idx, name in enumerate(header)}
        start_text = values[indexes['Start Date']] if 'Start Date' in indexes and indexes['Start Date'] < len(values) else None
        end_text = values[indexes['End Date']] if 'End Date' in indexes and indexes['End Date'] < len(values) else None
        return _parse_carelink_datetime(start_text), _parse_carelink_datetime(end_text)
    except Exception:
        return None, None


def _find_reusable_carelink_csv(
    activity_start_dt: datetime | None,
    activity_end_dt: datetime | None,
    data_root: Path = DATA_DIR,
) -> tuple[Path, dict] | None:
    if activity_start_dt is None or activity_end_dt is None:
        return None

    for walk_dir in _iter_walk_dirs(data_root):
        meta = _load_walk_meta(walk_dir)
        coverage_start = _parse_carelink_datetime(meta.get('carelink_start_time'))
        coverage_end = _parse_carelink_datetime(meta.get('carelink_end_time'))
        if coverage_start is None or coverage_end is None:
            continue
        if coverage_start <= activity_start_dt and activity_end_dt <= coverage_end:
            csv_files = sorted(walk_dir.glob('*.csv'))
            if csv_files:
                return csv_files[0], meta

    return None


def _ts_to_dist_km(ts_secs: float, dist_pairs: list[tuple[float, float]]) -> float | None:
    """Linearly interpolate distance_km for a POSIX timestamp from sorted (ts, dist_km) pairs."""
    if not dist_pairs:
        return None
    if ts_secs <= dist_pairs[0][0]:
        return dist_pairs[0][1]
    if ts_secs >= dist_pairs[-1][0]:
        return dist_pairs[-1][1]
    lo, hi = 0, len(dist_pairs) - 1
    while lo < hi - 1:
        mid = (lo + hi) // 2
        if dist_pairs[mid][0] <= ts_secs:
            lo = mid
        else:
            hi = mid
    t0, d0 = dist_pairs[lo]
    t1, d1 = dist_pairs[hi]
    frac = (ts_secs - t0) / (t1 - t0) if t1 != t0 else 0.0
    return d0 + frac * (d1 - d0)


def _window_filter(points: list[dict], start_dt: datetime | None, end_dt: datetime | None, key: str) -> list[dict]:
    if start_dt is None or end_dt is None:
        return points
    filtered = []
    for point in points:
        try:
            ts = datetime.fromisoformat(point[key])
        except (KeyError, ValueError):
            continue
        if start_dt <= ts <= end_dt:
            filtered.append(point)
    return filtered


def _summary_metrics(activity_points: list[dict], bg_points: list[dict], bolus_events: list[dict]) -> dict:
    if not activity_points:
        return {
            'distance_km': None,
            'duration_h': None,
            'avg_hr': None,
            'bg_delta': None,
            'tir_pct': None,
            'hypos': 0,
            'bolus_units': sum(item['units'] for item in bolus_events),
        }

    distances = [p['distance_m'] for p in activity_points if p.get('distance_m') is not None]
    distance_km = None
    if distances:
        distance_km = (max(distances) - min(distances)) / 1000.0

    start_dt = datetime.fromisoformat(activity_points[0]['timestamp_iso'])
    end_dt = datetime.fromisoformat(activity_points[-1]['timestamp_iso'])
    duration_h = max((end_dt - start_dt).total_seconds(), 0) / 3600.0

    hr_values = [p['hr'] for p in activity_points if p.get('hr') is not None]
    avg_hr = sum(hr_values) / len(hr_values) if hr_values else None

    window_bg = []
    for point in bg_points:
        ts = datetime.fromisoformat(point['timestamp'])
        if start_dt <= ts <= end_dt:
            window_bg.append(point['bg'])

    bg_delta = None
    if len(window_bg) >= 2:
        bg_delta = window_bg[-1] - window_bg[0]

    in_range = [v for v in window_bg if 4.0 <= v <= 8.0]
    tir_pct = (len(in_range) / len(window_bg) * 100.0) if window_bg else None
    hypos = sum(1 for v in window_bg if v <= 3.9)

    bolus_total = 0.0
    for bolus in bolus_events:
        ts = datetime.fromisoformat(bolus['timestamp'])
        if start_dt <= ts <= end_dt:
            bolus_total += bolus['units']

    return {
        'distance_km': distance_km,
        'duration_h': duration_h,
        'avg_hr': avg_hr,
        'bg_delta': bg_delta,
        'tir_pct': tir_pct,
        'hypos': hypos,
        'bolus_units': bolus_total,
    }


def _bg_slope_per_hour(bg_values: list[tuple[datetime, float]]) -> float | None:
    if len(bg_values) < 2:
        return None
    start_dt, start_bg = bg_values[0]
    end_dt, end_bg = bg_values[-1]
    duration_h = (end_dt - start_dt).total_seconds() / 3600.0
    if duration_h <= 0:
        return None
    return (end_bg - start_bg) / duration_h


def _phase_glucose_analytics(activity_points: list[dict], bg_points: list[dict]) -> dict:
    if not activity_points:
        return {'phases': [], 'during_slope_per_hour': None}

    walk_start = datetime.fromisoformat(activity_points[0]['timestamp_iso'])
    walk_end = datetime.fromisoformat(activity_points[-1]['timestamp_iso'])
    phase_defs = [
        ('pre', 'Pre (60m)', walk_start - timedelta(minutes=60), walk_start),
        ('during', 'During Walk', walk_start, walk_end),
        ('post', 'Post (120m)', walk_end, walk_end + timedelta(minutes=120)),
    ]

    all_bg: list[tuple[datetime, float]] = []
    for point in bg_points:
        try:
            point_dt = datetime.fromisoformat(point['timestamp'])
            point_bg = float(point['bg'])
        except (KeyError, ValueError, TypeError):
            continue
        all_bg.append((point_dt, point_bg))

    phases = []
    for key, label, start_dt, end_dt in phase_defs:
        phase_bg = [(ts, bg) for ts, bg in all_bg if start_dt <= ts <= end_dt]
        slope = _bg_slope_per_hour(phase_bg)
        delta = (phase_bg[-1][1] - phase_bg[0][1]) if len(phase_bg) >= 2 else None
        phases.append({
            'key': key,
            'label': label,
            'count': len(phase_bg),
            'delta': delta,
            'slope_per_hour': slope,
        })

    during = next((p for p in phases if p['key'] == 'during'), None)
    return {
        'phases': phases,
        'during_slope_per_hour': during['slope_per_hour'] if during else None,
    }


def _interp_bg(ts_unix: float, bg_pairs: list[tuple[float, float]]) -> float | None:
    if not bg_pairs:
        return None
    if ts_unix <= bg_pairs[0][0]:
        return bg_pairs[0][1]
    if ts_unix >= bg_pairs[-1][0]:
        return bg_pairs[-1][1]

    lo, hi = 0, len(bg_pairs) - 1
    while lo < hi - 1:
        mid = (lo + hi) // 2
        if bg_pairs[mid][0] <= ts_unix:
            lo = mid
        else:
            hi = mid
    t0, b0 = bg_pairs[lo]
    t1, b1 = bg_pairs[hi]
    if t1 == t0:
        return b0
    frac = (ts_unix - t0) / (t1 - t0)
    return b0 + frac * (b1 - b0)


def _hr_zone_label(hr: float) -> str:
    if hr < 110:
        return '<110'
    if hr < 130:
        return '110-129'
    if hr < 150:
        return '130-149'
    return '150+'


def _intensity_glucose_analytics(activity_points: list[dict], bg_points: list[dict]) -> list[dict]:
    zone_order = ['<110', '110-129', '130-149', '150+']
    zone_stats = {
        zone: {'minutes': 0.0, 'bg_values': [], 'points': []}
        for zone in zone_order
    }

    bg_pairs: list[tuple[float, float]] = []
    for point in bg_points:
        try:
            ts = datetime.fromisoformat(point['timestamp']).timestamp()
            bg = float(point['bg'])
        except (KeyError, ValueError, TypeError):
            continue
        bg_pairs.append((ts, bg))
    bg_pairs.sort(key=lambda x: x[0])

    walk_points: list[tuple[float, float]] = []
    for point in activity_points:
        if point.get('hr') is None:
            continue
        try:
            ts = datetime.fromisoformat(point['timestamp_iso']).timestamp()
            hr = float(point['hr'])
        except (KeyError, ValueError, TypeError):
            continue
        walk_points.append((ts, hr))
    walk_points.sort(key=lambda x: x[0])

    if not walk_points or not bg_pairs:
        return []

    for idx, (ts, hr) in enumerate(walk_points):
        zone = _hr_zone_label(hr)
        bg_val = _interp_bg(ts, bg_pairs)
        if bg_val is not None:
            zone_stats[zone]['bg_values'].append(bg_val)
            zone_stats[zone]['points'].append((ts, bg_val))

        if idx < len(walk_points) - 1:
            dt_mins = max((walk_points[idx + 1][0] - ts) / 60.0, 0.0)
            zone_stats[zone]['minutes'] += dt_mins

    results = []
    for zone in zone_order:
        stat = zone_stats[zone]
        vals = stat['bg_values']
        bg_std = None
        if len(vals) >= 2:
            mean = sum(vals) / len(vals)
            bg_std = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals))

        zone_slope = _bg_slope_per_hour([
            (datetime.fromtimestamp(ts, tz=timezone.utc), bg)
            for ts, bg in stat['points']
        ])

        results.append({
            'zone': zone,
            'minutes': stat['minutes'],
            'samples': len(vals),
            'bg_std': bg_std,
            'bg_slope_per_hour': zone_slope,
        })
    return results


def _fetch_open_meteo_weather(
    latitude: float,
    longitude: float,
    start_dt: datetime,
    end_dt: datetime,
) -> list[dict]:
    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=timezone.utc)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=timezone.utc)

    params = {
        'latitude': f'{latitude:.6f}',
        'longitude': f'{longitude:.6f}',
        'start_date': start_dt.date().isoformat(),
        'end_date': end_dt.date().isoformat(),
        'hourly': 'temperature_2m,apparent_temperature,wind_speed_10m,wind_direction_10m',
        'timezone': 'UTC',
    }
    url = f"https://archive-api.open-meteo.com/v1/archive?{urllib.parse.urlencode(params)}"

    try:
        with urllib.request.urlopen(url, timeout=12) as response:
            payload = json.loads(response.read().decode('utf-8'))
    except Exception:
        return []

    hourly = payload.get('hourly') or {}
    times = hourly.get('time') or []
    temps = hourly.get('temperature_2m') or []
    apparent = hourly.get('apparent_temperature') or []
    wind = hourly.get('wind_speed_10m') or []
    wind_dir = hourly.get('wind_direction_10m') or []

    rows: list[dict] = []
    for i, time_text in enumerate(times):
        try:
            ts = datetime.fromisoformat(time_text)
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        if ts < start_dt or ts > end_dt:
            continue

        rows.append({
            'timestamp': ts.isoformat(),
            'temp_c': temps[i] if i < len(temps) else None,
            'apparent_c': apparent[i] if i < len(apparent) else None,
            'wind_kph': wind[i] if i < len(wind) else None,
            'wind_dir_deg': wind_dir[i] if i < len(wind_dir) else None,
        })
    return rows


def _weather_metrics(weather_points: list[dict]) -> dict:
    if not weather_points:
        return {'temp_avg_c': None, 'temp_min_c': None, 'temp_max_c': None, 'wind_avg_kph': None}

    temps = [float(p['temp_c']) for p in weather_points if p.get('temp_c') is not None]
    winds = [float(p['wind_kph']) for p in weather_points if p.get('wind_kph') is not None]
    return {
        'temp_avg_c': (sum(temps) / len(temps)) if temps else None,
        'temp_min_c': min(temps) if temps else None,
        'temp_max_c': max(temps) if temps else None,
        'wind_avg_kph': (sum(winds) / len(winds)) if winds else None,
    }


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2.0) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2.0) ** 2
    return 2.0 * r * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def _bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    lam1 = math.radians(lon1)
    lam2 = math.radians(lon2)
    y = math.sin(lam2 - lam1) * math.cos(phi2)
    x = math.cos(phi1) * math.sin(phi2) - math.sin(phi1) * math.cos(phi2) * math.cos(lam2 - lam1)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def _ang_diff_deg(a: float, b: float) -> float:
    return ((a - b + 180.0) % 360.0) - 180.0


def _compute_route_fingerprint(map_track: list[list[float]], distance_km: float | None) -> dict | None:
    if not map_track or len(map_track) < 2 or not distance_km:
        return None
    lats = [p[0] for p in map_track]
    lons = [p[1] for p in map_track]
    step = max(1, len(map_track) // 40)
    simplified = [[round(p[0], 6), round(p[1], 6)] for p in map_track[::step]]
    last = [round(map_track[-1][0], 6), round(map_track[-1][1], 6)]
    if simplified[-1] != last:
        simplified.append(last)
    return {
        "start_lat": round(map_track[0][0], 6),
        "start_lon": round(map_track[0][1], 6),
        "end_lat": round(map_track[-1][0], 6),
        "end_lon": round(map_track[-1][1], 6),
        "distance_km": round(distance_km, 2),
        "bbox": [round(min(lats), 5), round(min(lons), 5), round(max(lats), 5), round(max(lons), 5)],
        "simplified_track": simplified,
    }


def _longest_shared_segment_km(
    track_a: list[list[float]],
    dist_a: float,
    track_b: list[list[float]],
    threshold_km: float = 0.25,
) -> float:
    """Length of the longest contiguous run of track_a points each within threshold_km of some point on track_b."""
    if not track_a or not track_b or not dist_a or len(track_a) < 2:
        return 0.0
    km_per_pt = dist_a / (len(track_a) - 1)
    max_run = 0
    cur = 0
    for pa in track_a:
        if any(_haversine_km(pa[0], pa[1], pb[0], pb[1]) <= threshold_km for pb in track_b):
            cur += 1
            max_run = max(max_run, cur)
        else:
            cur = 0
    return max_run * km_per_pt


def _routes_similar(a: dict, b: dict) -> bool:
    # Start points within 500 m — allows for different car parks or trail heads in the same area.
    if _haversine_km(a["start_lat"], a["start_lon"], b["start_lat"], b["start_lon"]) > 0.5:
        return False
    track_a = a.get("simplified_track") or []
    track_b = b.get("simplified_track") or []
    if track_a and track_b:
        # Match if the longest shared contiguous segment is >= 3 km.
        # This handles sub-routes and diverging walks: two walks sharing 7 km out of Ambleside
        # before heading different ways will still be grouped together.
        dist_a = a.get("distance_km") or 0.0
        dist_b = b.get("distance_km") or 0.0
        seg_ab = _longest_shared_segment_km(track_a, dist_a, track_b)
        seg_ba = _longest_shared_segment_km(track_b, dist_b, track_a)
        return max(seg_ab, seg_ba) >= 3.0
    # Fallback when no track data: require matching end point + similar distance.
    if not a.get("distance_km") or not b.get("distance_km"):
        return False
    if _haversine_km(a["end_lat"], a["end_lon"], b["end_lat"], b["end_lon"]) > 0.3:
        return False
    avg = (a["distance_km"] + b["distance_km"]) / 2
    return abs(a["distance_km"] - b["distance_km"]) / avg <= 0.20


def _build_route_groups(rows: list[dict]) -> list[list[dict]]:
    """Union-find clustering of walks by route overlap. Returns groups of ≥2 walks, oldest-first."""
    n = len(rows)
    parent = list(range(n))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int) -> None:
        parent[find(i)] = find(j)

    for i in range(n):
        for j in range(i + 1, n):
            if _routes_similar(rows[i]['route_fingerprint'], rows[j]['route_fingerprint']):
                union(i, j)

    buckets: dict[int, list[dict]] = {}
    for i, row in enumerate(rows):
        buckets.setdefault(find(i), []).append(row)

    result = []
    for group_rows in buckets.values():
        if len(group_rows) >= 2:
            result.append(sorted(group_rows, key=lambda r: (r.get('start_time') or '', r.get('date') or '', r.get('walk_id') or '')))
    return result


def _sample_at_distances(pairs: list[tuple[float, float]], sample_dists: list[float]) -> list[float | None]:
    if not pairs:
        return [None] * len(sample_dists)
    pairs = sorted(pairs, key=lambda p: p[0])
    xs = [p[0] for p in pairs]
    ys = [p[1] for p in pairs]
    result: list[float | None] = []
    for d in sample_dists:
        if d <= xs[0]:
            result.append(round(ys[0], 2))
        elif d >= xs[-1]:
            result.append(round(ys[-1], 2))
        else:
            lo, hi = 0, len(xs) - 1
            while lo < hi - 1:
                mid = (lo + hi) // 2
                if xs[mid] <= d:
                    lo = mid
                else:
                    hi = mid
            t = (d - xs[lo]) / (xs[hi] - xs[lo]) if xs[hi] != xs[lo] else 0.0
            result.append(round(ys[lo] + t * (ys[hi] - ys[lo]), 2))
    return result


def _compute_route_series(
    activity_points: list[dict],
    bg_points: list[dict],
    weather_points: list[dict],
    distance_km: float,
) -> dict:
    n = 50
    dists = [round(i * distance_km / (n - 1), 3) for i in range(n)]
    hr_pairs = [
        (p["distance_m"] / 1000.0, p["hr"])
        for p in activity_points if p.get("distance_m") is not None and p.get("hr") is not None
    ]
    alt_pairs = [
        (p["distance_m"] / 1000.0, p["altitude_m"])
        for p in activity_points if p.get("distance_m") is not None and p.get("altitude_m") is not None
    ]
    bg_pairs = [
        (p["distance_km"], p["bg"])
        for p in bg_points if p.get("distance_km") is not None and p.get("bg") is not None and p["distance_km"] >= 0
    ]
    temp_pairs = [(p["distance_km"], p["temp_c"]) for p in weather_points if p.get("temp_c") is not None]
    wind_pairs = [(p["distance_km"], p["wind_kph"]) for p in weather_points if p.get("wind_kph") is not None]
    return {
        "distances": dists,
        "hr": _sample_at_distances(hr_pairs, dists),
        "altitude_m": _sample_at_distances(alt_pairs, dists),
        "bg": _sample_at_distances(bg_pairs, dists),
        "temp_c": _sample_at_distances(temp_pairs, dists),
        "wind_kph": _sample_at_distances(wind_pairs, dists),
    }


def _weather_interp(ts: float, weather_pairs: list[tuple[float, dict]]) -> dict | None:
    if not weather_pairs:
        return None
    if ts <= weather_pairs[0][0]:
        return weather_pairs[0][1]
    if ts >= weather_pairs[-1][0]:
        return weather_pairs[-1][1]

    lo, hi = 0, len(weather_pairs) - 1
    while lo < hi - 1:
        mid = (lo + hi) // 2
        if weather_pairs[mid][0] <= ts:
            lo = mid
        else:
            hi = mid

    t0, w0 = weather_pairs[lo]
    t1, w1 = weather_pairs[hi]
    if t1 == t0:
        return w0

    frac = (ts - t0) / (t1 - t0)
    out: dict = {}
    for key in ('temp_c', 'apparent_c', 'wind_kph', 'wind_dir_deg'):
        v0 = w0.get(key)
        v1 = w1.get(key)
        if v0 is None and v1 is None:
            out[key] = None
        elif v0 is None:
            out[key] = v1
        elif v1 is None:
            out[key] = v0
        elif key == 'wind_dir_deg':
            delta = _ang_diff_deg(float(v1), float(v0))
            out[key] = (float(v0) + frac * delta) % 360.0
        else:
            out[key] = float(v0) + frac * (float(v1) - float(v0))
    return out


def _weather_effort_analytics(activity_points: list[dict], weather_points: list[dict], duration_h: float | None) -> dict:
    if len(activity_points) < 2 or not weather_points:
        return {
            'headwind_exposure_pct': None,
            'headwind_avg_kph': None,
            'tailwind_avg_kph': None,
            'weather_stress_score': None,
            'weather_stress_band': None,
            'wind_rose': [],
            'wind_profile': [],
        }

    weather_pairs: list[tuple[float, dict]] = []
    for row in weather_points:
        try:
            ts = datetime.fromisoformat(row['timestamp']).timestamp()
        except (KeyError, ValueError):
            continue
        weather_pairs.append((ts, row))
    weather_pairs.sort(key=lambda x: x[0])
    if not weather_pairs:
        return {
            'headwind_exposure_pct': None,
            'headwind_avg_kph': None,
            'tailwind_avg_kph': None,
            'weather_stress_score': None,
            'weather_stress_band': None,
            'wind_rose': [],
            'wind_profile': [],
        }

    rose_bins = [0.0] * 8
    total_km = 0.0
    headwind_km = 0.0
    headwind_sum = 0.0
    tailwind_sum = 0.0
    wind_profile: list[dict] = []

    for i in range(1, len(activity_points)):
        a = activity_points[i - 1]
        b = activity_points[i]
        if a.get('lat') is None or a.get('lon') is None or b.get('lat') is None or b.get('lon') is None:
            continue
        if a.get('timestamp') is None or b.get('timestamp') is None:
            continue

        seg_km = None
        if a.get('distance_m') is not None and b.get('distance_m') is not None:
            d = float(b['distance_m']) - float(a['distance_m'])
            if d > 0:
                seg_km = d / 1000.0
        if seg_km is None:
            seg_km = _haversine_km(float(a['lat']), float(a['lon']), float(b['lat']), float(b['lon']))
        if seg_km <= 0:
            continue

        mid_ts = (float(a['timestamp']) + float(b['timestamp'])) / 2.0
        weather = _weather_interp(mid_ts, weather_pairs)
        if not weather:
            continue

        wind_kph = weather.get('wind_kph')
        wind_dir = weather.get('wind_dir_deg')
        if wind_kph is None or wind_dir is None:
            continue

        heading = _bearing_deg(float(a['lat']), float(a['lon']), float(b['lat']), float(b['lon']))
        alignment = math.cos(math.radians(_ang_diff_deg(heading, float(wind_dir))))
        signed_component = float(wind_kph) * alignment
        head_component = max(signed_component, 0.0)
        tail_component = max(-signed_component, 0.0)

        total_km += seg_km
        headwind_sum += head_component * seg_km
        tailwind_sum += tail_component * seg_km
        if head_component >= 2.0:
            headwind_km += seg_km

        bin_index = int(((float(wind_dir) % 360.0) + 22.5) // 45.0) % 8
        rose_bins[bin_index] += seg_km

        wind_profile.append({
            'distance_km': (float(b['distance_m']) / 1000.0) if b.get('distance_m') is not None else None,
            'headwind_kph': head_component,
            'tailwind_kph': tail_component,
        })

    if total_km <= 0:
        return {
            'headwind_exposure_pct': None,
            'headwind_avg_kph': None,
            'tailwind_avg_kph': None,
            'weather_stress_score': None,
            'weather_stress_band': None,
            'wind_rose': [],
            'wind_profile': wind_profile,
        }

    headwind_avg = headwind_sum / total_km
    tailwind_avg = tailwind_sum / total_km
    headwind_exposure_pct = (headwind_km / total_km) * 100.0

    temps_apparent = [float(p['apparent_c']) for p in weather_points if p.get('apparent_c') is not None]
    apparent_avg = (sum(temps_apparent) / len(temps_apparent)) if temps_apparent else None
    apparent_max = max(temps_apparent) if temps_apparent else None
    duration_term = min(duration_h or 0.0, 8.0) * 2.0
    heat_term = (max((apparent_avg or 0.0) - 15.0, 0.0) * 2.0) + (max((apparent_max or 0.0) - 22.0, 0.0) * 1.5)
    wind_term = headwind_avg * 1.2
    stress_score = max(0.0, min(100.0, heat_term + wind_term + duration_term))
    if stress_score < 25.0:
        stress_band = 'Low'
    elif stress_score < 50.0:
        stress_band = 'Moderate'
    elif stress_score < 75.0:
        stress_band = 'High'
    else:
        stress_band = 'Very High'

    compass = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
    wind_rose = [
        {'dir': compass[i], 'distance_km': rose_bins[i], 'pct': (rose_bins[i] / total_km) * 100.0}
        for i in range(8)
    ]

    return {
        'headwind_exposure_pct': headwind_exposure_pct,
        'headwind_avg_kph': headwind_avg,
        'tailwind_avg_kph': tailwind_avg,
        'weather_stress_score': stress_score,
        'weather_stress_band': stress_band,
        'wind_rose': wind_rose,
        'wind_profile': wind_profile,
    }


def _stress_decoupling_analytics(activity_points: list[dict], weather_points: list[dict]) -> dict:
    if len(activity_points) < 3:
        return {
            'series': [],
            'episodes': [],
            'summary': {
                'score': None,
                'band': None,
                'elevated_minutes': None,
                'elevated_pct': None,
                'max_residual_bpm': None,
            },
            'defaults': {'smoothing_points': 5, 'threshold_bpm': 8.0},
        }

    weather_pairs: list[tuple[float, dict]] = []
    for row in weather_points:
        try:
            ts = datetime.fromisoformat(row['timestamp']).timestamp()
        except (KeyError, ValueError):
            continue
        weather_pairs.append((ts, row))
    weather_pairs.sort(key=lambda x: x[0])

    samples: list[dict] = []
    for i in range(1, len(activity_points)):
        prev = activity_points[i - 1]
        curr = activity_points[i]

        if prev.get('timestamp') is None or curr.get('timestamp') is None:
            continue
        if prev.get('hr') is None or curr.get('hr') is None:
            continue

        dt_s = float(curr['timestamp']) - float(prev['timestamp'])
        if dt_s <= 0 or dt_s > 120:
            continue

        dist_m = None
        if prev.get('distance_m') is not None and curr.get('distance_m') is not None:
            delta_m = float(curr['distance_m']) - float(prev['distance_m'])
            if delta_m > 0:
                dist_m = delta_m
        if dist_m is None and prev.get('lat') is not None and prev.get('lon') is not None and curr.get('lat') is not None and curr.get('lon') is not None:
            dist_m = _haversine_km(float(prev['lat']), float(prev['lon']), float(curr['lat']), float(curr['lon'])) * 1000.0
        if dist_m is None or dist_m <= 0:
            continue

        speed_kph = (dist_m / dt_s) * 3.6
        if speed_kph <= 0:
            continue

        grade_pct = 0.0
        if prev.get('altitude_m') is not None and curr.get('altitude_m') is not None and dist_m >= 20.0:
            grade_pct = ((float(curr['altitude_m']) - float(prev['altitude_m'])) / dist_m) * 100.0
            grade_pct = max(-12.0, min(12.0, grade_pct))

        mid_ts = (float(prev['timestamp']) + float(curr['timestamp'])) / 2.0
        wx = _weather_interp(mid_ts, weather_pairs) if weather_pairs else None
        apparent_c = None
        if wx is not None and wx.get('apparent_c') is not None:
            apparent_c = float(wx['apparent_c'])

        heat_load = max((apparent_c or 18.0) - 18.0, 0.0)
        uphill_load = max(grade_pct, 0.0)
        effort_index = speed_kph + (0.7 * uphill_load) + (0.25 * heat_load)

        hr = float(curr['hr'])
        distance_km = (float(curr['distance_m']) / 1000.0) if curr.get('distance_m') is not None else None
        samples.append({
            'timestamp': curr['timestamp_iso'],
            'distance_km': distance_km,
            'hr': hr,
            'effort_index': effort_index,
            'dt_s': dt_s,
        })

    if len(samples) < 8:
        return {
            'series': [],
            'episodes': [],
            'summary': {
                'score': None,
                'band': None,
                'elevated_minutes': None,
                'elevated_pct': None,
                'max_residual_bpm': None,
            },
            'defaults': {'smoothing_points': 5, 'threshold_bpm': 8.0},
        }

    mean_effort = sum(s['effort_index'] for s in samples) / len(samples)
    mean_hr = sum(s['hr'] for s in samples) / len(samples)
    var_effort = sum((s['effort_index'] - mean_effort) ** 2 for s in samples)
    cov = sum((s['effort_index'] - mean_effort) * (s['hr'] - mean_hr) for s in samples)

    slope = (cov / var_effort) if var_effort > 1e-6 else 0.0
    intercept = mean_hr - slope * mean_effort

    for idx, sample in enumerate(samples):
        expected_hr = intercept + slope * sample['effort_index']
        residual = sample['hr'] - expected_hr
        start = max(0, idx - 2)
        end = min(len(samples), idx + 3)
        smooth = sum(samples[j].get('residual_raw', residual) for j in range(start, end)) / (end - start)
        sample['expected_hr'] = expected_hr
        sample['residual_raw'] = residual
        sample['residual_bpm'] = smooth

    elevated_secs = 0.0
    max_residual = max(sample['residual_bpm'] for sample in samples)
    episode_start = None
    episodes: list[dict] = []

    for sample in samples:
        elevated = sample['residual_bpm'] >= 8.0
        sample['flag_elevated'] = elevated
        if elevated:
            elevated_secs += sample['dt_s']
            if episode_start is None:
                episode_start = sample
        elif episode_start is not None:
            episodes.append({'start': episode_start['timestamp'], 'end': sample['timestamp']})
            episode_start = None

    if episode_start is not None:
        episodes.append({'start': episode_start['timestamp'], 'end': samples[-1]['timestamp']})

    total_secs = sum(s['dt_s'] for s in samples)
    elevated_pct = (elevated_secs / total_secs) * 100.0 if total_secs > 0 else 0.0
    elevated_minutes = elevated_secs / 60.0

    score = min(100.0, max(0.0, elevated_pct * 1.2 + max(max_residual - 6.0, 0.0) * 4.0))
    if score < 20.0:
        band = 'Low'
    elif score < 45.0:
        band = 'Moderate'
    elif score < 70.0:
        band = 'High'
    else:
        band = 'Very High'

    return {
        'series': [
            {
                'timestamp': s['timestamp'],
                'distance_km': s['distance_km'],
                'hr': s['hr'],
                'expected_hr': s['expected_hr'],
                'residual_raw': s['residual_raw'],
                'residual_bpm': s['residual_bpm'],
                'flag_elevated': s['flag_elevated'],
            }
            for s in samples if s['distance_km'] is not None
        ],
        'episodes': episodes,
        'summary': {
            'score': score,
            'band': band,
            'elevated_minutes': elevated_minutes,
            'elevated_pct': elevated_pct,
            'max_residual_bpm': max_residual,
        },
        'defaults': {'smoothing_points': 5, 'threshold_bpm': 8.0},
    }


def _insulin_stress_effect_analytics(
    activity_points: list[dict],
    bolus_events_all: list[dict],
    weather_points: list[dict],
) -> dict:
    """
    Heuristic proxy for exercise-related insulin potentiation during active bolus windows.
    Non-diagnostic and intended for trend tracking only.
    """
    if len(activity_points) < 3:
        return {
            'series': [],
            'summary': {
                'stress_multiplier': None,
                'band': None,
                'overlap_minutes': None,
                'weighted_overlap_minutes': None,
                'peak_iob_units_proxy': None,
                'peak_stress_index': None,
            },
            'defaults': {
                'iob_window_minutes': 240,
                'max_hr_reference': 175,
            },
        }

    weather_pairs: list[tuple[float, dict]] = []
    for row in weather_points:
        try:
            ts = datetime.fromisoformat(row['timestamp']).timestamp()
        except (KeyError, ValueError):
            continue
        weather_pairs.append((ts, row))
    weather_pairs.sort(key=lambda x: x[0])

    bolus_rows: list[tuple[float, float]] = []
    for row in bolus_events_all:
        try:
            ts = datetime.fromisoformat(row['timestamp']).timestamp()
            units = float(row['units'])
        except (KeyError, TypeError, ValueError):
            continue
        if units > 0:
            bolus_rows.append((ts, units))
    bolus_rows.sort(key=lambda x: x[0])

    if not bolus_rows:
        return {
            'series': [],
            'summary': {
                'stress_multiplier': None,
                'band': None,
                'overlap_minutes': 0.0,
                'weighted_overlap_minutes': 0.0,
                'peak_iob_units_proxy': 0.0,
                'peak_stress_index': None,
            },
            'defaults': {
                'iob_window_minutes': 240,
                'max_hr_reference': 175,
            },
        }

    iob_window_s = 4.0 * 3600.0
    max_hr_reference = 175.0
    series: list[dict] = []
    weighted_overlap_minutes = 0.0
    overlap_minutes = 0.0

    peak_iob_units_proxy = 0.0
    peak_stress_index = 0.0
    weighted_stress_sum = 0.0
    weighted_base_sum = 0.0

    for i in range(1, len(activity_points)):
        prev = activity_points[i - 1]
        curr = activity_points[i]

        if prev.get('timestamp') is None or curr.get('timestamp') is None:
            continue
        if curr.get('hr') is None:
            continue

        dt_s = float(curr['timestamp']) - float(prev['timestamp'])
        if dt_s <= 0 or dt_s > 180:
            continue
        dt_min = dt_s / 60.0

        dist_m = None
        if prev.get('distance_m') is not None and curr.get('distance_m') is not None:
            delta_m = float(curr['distance_m']) - float(prev['distance_m'])
            if delta_m > 0:
                dist_m = delta_m
        if dist_m is None and prev.get('lat') is not None and prev.get('lon') is not None and curr.get('lat') is not None and curr.get('lon') is not None:
            dist_m = _haversine_km(float(prev['lat']), float(prev['lon']), float(curr['lat']), float(curr['lon'])) * 1000.0
        if dist_m is None or dist_m <= 0:
            continue

        grade_pct = 0.0
        if prev.get('altitude_m') is not None and curr.get('altitude_m') is not None and dist_m >= 20.0:
            grade_pct = ((float(curr['altitude_m']) - float(prev['altitude_m'])) / dist_m) * 100.0

        mid_ts = (float(prev['timestamp']) + float(curr['timestamp'])) / 2.0

        iob_units_proxy = 0.0
        for bolus_ts, bolus_units in bolus_rows:
            elapsed = mid_ts - bolus_ts
            if elapsed < 0 or elapsed > iob_window_s:
                continue
            remaining = 1.0 - (elapsed / iob_window_s)
            iob_units_proxy += bolus_units * max(remaining, 0.0)

        if iob_units_proxy < 0.05:
            continue

        hr = float(curr['hr'])
        hr_load = max(min((hr - 95.0) / (max_hr_reference - 95.0), 1.4), 0.0)
        uphill_load = max(min(grade_pct / 8.0, 1.0), 0.0)

        wx = _weather_interp(mid_ts, weather_pairs) if weather_pairs else None
        apparent_c = float(wx['apparent_c']) if wx and wx.get('apparent_c') is not None else None
        heat_load = max(min(((apparent_c or 18.0) - 18.0) / 10.0, 1.0), 0.0)

        stress_index = 1.0 + (0.45 * hr_load) + (0.20 * uphill_load) + (0.10 * heat_load)
        stress_index = max(0.8, min(1.9, stress_index))

        # Saturate at 3 U to prevent a single large bolus dominating the score.
        iob_weight = min(iob_units_proxy / 3.0, 1.0)
        overlap_minutes += dt_min
        weighted_overlap_minutes += dt_min * iob_weight
        weighted_stress_sum += dt_min * iob_weight * stress_index
        weighted_base_sum += dt_min * iob_weight

        peak_iob_units_proxy = max(peak_iob_units_proxy, iob_units_proxy)
        peak_stress_index = max(peak_stress_index, stress_index)

        series.append({
            'timestamp': curr['timestamp_iso'],
            'distance_km': (float(curr['distance_m']) / 1000.0) if curr.get('distance_m') is not None else None,
            'iob_units_proxy': iob_units_proxy,
            'stress_index': stress_index,
            'insulin_effect_index': stress_index * iob_weight,
        })

    if weighted_base_sum <= 0:
        return {
            'series': [row for row in series if row.get('distance_km') is not None],
            'summary': {
                'stress_multiplier': None,
                'band': None,
                'overlap_minutes': overlap_minutes,
                'weighted_overlap_minutes': weighted_overlap_minutes,
                'peak_iob_units_proxy': peak_iob_units_proxy,
                'peak_stress_index': peak_stress_index if peak_stress_index > 0 else None,
            },
            'defaults': {
                'iob_window_minutes': 240,
                'max_hr_reference': 175,
            },
        }

    multiplier = weighted_stress_sum / weighted_base_sum
    if multiplier < 1.10:
        band = 'Low'
    elif multiplier < 1.25:
        band = 'Moderate'
    elif multiplier < 1.40:
        band = 'High'
    else:
        band = 'Very High'

    return {
        'series': [row for row in series if row.get('distance_km') is not None],
        'summary': {
            'stress_multiplier': multiplier,
            'band': band,
            'overlap_minutes': overlap_minutes,
            'weighted_overlap_minutes': weighted_overlap_minutes,
            'peak_iob_units_proxy': peak_iob_units_proxy,
            'peak_stress_index': peak_stress_index,
        },
        'defaults': {
            'iob_window_minutes': 240,
            'max_hr_reference': 175,
        },
    }


def _sensor_gap_detection(activity_points: list[dict], bg_points_all: list[dict]) -> list[dict]:
    """Detect CGM sensor gaps > 15 min during the walk window."""
    if not activity_points or len(bg_points_all) < 2:
        return []

    walk_start = datetime.fromisoformat(activity_points[0]['timestamp_iso'])
    walk_end = datetime.fromisoformat(activity_points[-1]['timestamp_iso'])

    walk_bg_times: list[datetime] = []
    for p in bg_points_all:
        try:
            ts = datetime.fromisoformat(p['timestamp'])
        except (KeyError, ValueError):
            continue
        if walk_start <= ts <= walk_end:
            walk_bg_times.append(ts)
    walk_bg_times.sort()

    gap_threshold = timedelta(minutes=15)
    gaps: list[dict] = []
    for i in range(1, len(walk_bg_times)):
        gap = walk_bg_times[i] - walk_bg_times[i - 1]
        if gap > gap_threshold:
            gaps.append({
                'start': walk_bg_times[i - 1].isoformat(),
                'end': walk_bg_times[i].isoformat(),
                'duration_min': round(gap.total_seconds() / 60.0, 1),
            })
    return gaps


def _extended_recovery_phases(activity_points: list[dict], bg_points_all: list[dict]) -> list[dict]:
    """BG in 0–2 h, 2–6 h, 6–12 h post-walk for delayed-hypo detection.

    Nocturnal hypo risk can persist 6–12 h after prolonged aerobic exercise.
    The 8.0 mmol/L threshold in the early window is the carbohydrate-intake
    trigger from Scott et al. 2021 (Lancet D&E 9:304-17).
    """
    if not activity_points:
        return []

    walk_end = datetime.fromisoformat(activity_points[-1]['timestamp_iso'])
    phase_defs = [
        ('post_0_2h',  '0–2 h post',  walk_end,                         walk_end + timedelta(hours=2)),
        ('post_2_6h',  '2–6 h post',  walk_end + timedelta(hours=2),    walk_end + timedelta(hours=6)),
        ('post_6_12h', '6–12 h post', walk_end + timedelta(hours=6),    walk_end + timedelta(hours=12)),
    ]

    all_bg: list[tuple[datetime, float]] = []
    for p in bg_points_all:
        try:
            all_bg.append((datetime.fromisoformat(p['timestamp']), float(p['bg'])))
        except (KeyError, ValueError, TypeError):
            continue

    phases: list[dict] = []
    for key, label, start_dt, end_dt in phase_defs:
        phase_bg = [(ts, bg) for ts, bg in all_bg if start_dt <= ts < end_dt]
        if not phase_bg:
            phases.append({
                'key': key, 'label': label, 'count': 0,
                'min_bg': None, 'mean_bg': None, 'hypos': 0, 'slope_per_hour': None,
            })
            continue
        values = [bg for _, bg in phase_bg]
        phases.append({
            'key': key,
            'label': label,
            'count': len(phase_bg),
            'min_bg': round(min(values), 2),
            'mean_bg': round(sum(values) / len(values), 2),
            'hypos': sum(1 for v in values if v <= 3.9),
            'slope_per_hour': _bg_slope_per_hour(phase_bg),
        })
    return phases


def _detect_ihe_bursts(activity_points: list[dict], max_hr_reference: float = 175.0) -> int:
    """Count intermittent high-intensity HR excursions (IHE) during the walk.

    IHE paper 2024 (doi:10.1016/j.diabres.2024.111631): short bursts at ≥85% HRmax
    interspersed in zone-2/3 aerobic work attenuate glucose drop by ~1.47 mmol/L
    vs continuous moderate exercise (CME).

    A burst is a contiguous run of activity_points where HR ≥ 85% HRmax lasting ≥ 10 s.
    """
    threshold = 0.85 * max_hr_reference
    pts = sorted(activity_points, key=lambda p: p['timestamp'])
    burst_count = 0
    in_burst = False
    burst_start_ts: float | None = None
    for p in pts:
        hr = p.get('hr')
        if hr is None:
            if in_burst:
                in_burst = False
            continue
        ts = float(p['timestamp'])
        if hr >= threshold:
            if not in_burst:
                in_burst = True
                burst_start_ts = ts
        else:
            if in_burst and burst_start_ts is not None:
                if ts - burst_start_ts >= 10.0:
                    burst_count += 1
            in_burst = False
            burst_start_ts = None
    if in_burst and burst_start_ts is not None:
        last_ts = float(pts[-1]['timestamp'])
        if last_ts - burst_start_ts >= 10.0:
            burst_count += 1
    return burst_count


def _exercise_risk_profile(
    activity_points: list[dict],
    bg_points_all: list[dict],
    metrics: dict,
    recovery_phases: list[dict],
    bolus_events_all: list[dict] | None = None,
) -> dict:
    """Evidence-based risk assessment and next-walk preparation guide.

    Clinical basis:
    - Riddell et al. 2017, PMID 28126459: intensity-stratified insulin/carb adjustments
    - Scott et al. 2021, Lancet D&E 9:304-17: post-exercise recovery protocol
    - Zaharieva et al. 2020, PMID 33011134: closed-loop exercise strategies
    - Boom et al. 2021, doi:10.1111/jdi.13356: IOB at walk-start risk
    - Parent et al. 2024, doi:10.1016/j.diabres.2024.111631: IHE burst protection
    """
    if not activity_points:
        return {}

    walk_start = datetime.fromisoformat(activity_points[0]['timestamp_iso'])
    walk_end   = datetime.fromisoformat(activity_points[-1]['timestamp_iso'])

    all_bg: list[tuple[datetime, float]] = []
    for p in bg_points_all:
        try:
            all_bg.append((datetime.fromisoformat(p['timestamp']), float(p['bg'])))
        except (KeyError, ValueError, TypeError):
            continue
    all_bg.sort(key=lambda x: x[0])

    def _nearest_bg(target: datetime, window_min: float = 20.0) -> float | None:
        w = timedelta(minutes=window_min)
        candidates = [(ts, bg) for ts, bg in all_bg if abs(ts - target) <= w]
        return min(candidates, key=lambda x: abs(x[0] - target))[1] if candidates else None

    bg_at_start = _nearest_bg(walk_start)
    bg_at_end   = _nearest_bg(walk_end)

    # N-02: First-30-min drop rate (Riddell 2017 / IHE 2024 — early descent is
    # the primary predictor of whether an episode reaches hypo).
    bg_at_30min = _nearest_bg(walk_start + timedelta(minutes=30), window_min=10.0)
    first_30_drop_rate: float | None = None
    rest_drop_rate: float | None = None
    first_30_flag: str | None = None
    if bg_at_start is not None and bg_at_30min is not None:
        first_30_drop_rate = round((bg_at_30min - bg_at_start) / 0.5, 2)  # mmol/L/h
        first_30_flag = 'rapid' if first_30_drop_rate < -2.0 else ('elevated' if first_30_drop_rate < -0.5 else 'normal')
    # Rest-of-walk rate: bg_at_30min → bg_at_end
    walk_duration_h = (walk_end - walk_start).total_seconds() / 3600.0
    if bg_at_30min is not None and bg_at_end is not None and walk_duration_h > 0.5:
        rest_h = walk_duration_h - 0.5
        if rest_h > 0.1:
            rest_drop_rate = round((bg_at_end - bg_at_30min) / rest_h, 2)

    # N-03: IHE burst detection (Parent et al. 2024).
    ihe_burst_count = _detect_ihe_bursts(activity_points)

    # N-04: Post-exercise hyperglycaemia (Scott 2021) — BG 8–12 mmol/L at walk end.
    post_exercise_hyper = (
        bg_at_end is not None and 8.0 < bg_at_end <= 12.0
    )

    # N-06: Nocturnal risk window end time (Scott 2021 — 6–12 h; use 12 h ceiling).
    risk_window_end_iso = (walk_end + timedelta(hours=12)).isoformat()

    # IOB at walk-start: linear decay model over 4 h (same as insulin_stress_effect).
    # Boom 2021: finishing a half-marathon safely required accounting for IOB before
    # the bolus preceding activity. Flag if > 1 U remains at walk start.
    iob_window_s = 4.0 * 3600.0
    iob_at_start: float | None = None
    iob_flag: str | None = None
    if bolus_events_all:
        ws_ts = walk_start.timestamp()
        iob_sum = 0.0
        for row in bolus_events_all:
            try:
                bolus_ts = datetime.fromisoformat(row['timestamp']).timestamp()
                units = float(row['units'])
            except (KeyError, ValueError, TypeError):
                continue
            elapsed = ws_ts - bolus_ts
            if elapsed < 0 or elapsed > iob_window_s:
                continue
            iob_sum += units * max(1.0 - (elapsed / iob_window_s), 0.0)
        iob_at_start = round(iob_sum, 2)
        if iob_at_start >= 2.0:
            iob_flag = 'high'
        elif iob_at_start >= 1.0:
            iob_flag = 'moderate'
        else:
            iob_flag = 'low'

    # Intensity by avg HR: Riddell 2017 Table 3 stratification
    avg_hr = metrics.get('avg_hr')
    if avg_hr is not None and avg_hr >= 130:
        intensity           = 'vigorous'
        bolus_reduction_pct = 75
        basal_reduction_pct = 80
        carbs_per_hour_g    = 30
    elif avg_hr is not None and avg_hr >= 110:
        intensity           = 'moderate'
        bolus_reduction_pct = 50
        basal_reduction_pct = 65
        carbs_per_hour_g    = 25
    elif avg_hr is not None:
        intensity           = 'light'
        bolus_reduction_pct = 25
        basal_reduction_pct = 50
        carbs_per_hour_g    = 15
    else:
        intensity           = 'unknown'
        bolus_reduction_pct = 50
        basal_reduction_pct = 65
        carbs_per_hour_g    = 20

    # Pre-walk BG: Riddell 2017 target 7–10 mmol/L for moderate aerobic.
    # E-01: Zaharieva 2020 Table 3 — closed-loop contraindication at > 15.0 mmol/L.
    prewalk_target_min, prewalk_target_max = 7.0, 10.0
    if bg_at_start is None:
        prewalk_assessment = None
    elif bg_at_start > 15.0:
        prewalk_assessment = 'contraindicated_cl'
    elif bg_at_start < prewalk_target_min:
        prewalk_assessment = 'low'
    elif bg_at_start <= prewalk_target_max:
        prewalk_assessment = 'optimal'
    else:
        prewalk_assessment = 'high'

    # Drop rate and forward hypo projection from walk-end BG at current rate
    slope = metrics.get('bg_slope_during_h')
    drop_flag = None
    projected_hypo_min = None
    if slope is not None:
        if slope < -2.0:
            drop_flag = 'rapid'
        elif slope < -0.5:
            drop_flag = 'elevated'
        else:
            drop_flag = 'normal'
        if slope < 0 and bg_at_end is not None and bg_at_end > 3.9:
            projected_hypo_min = round(((bg_at_end - 3.9) / abs(slope)) * 60.0)

    # Overnight risk from 2–12 h recovery data
    overnight_risk = 'low'
    for phase in recovery_phases:
        if phase['key'] in ('post_2_6h', 'post_6_12h'):
            if phase.get('hypos', 0) > 0:
                overnight_risk = 'confirmed'
                break
            s, m = phase.get('slope_per_hour'), phase.get('min_bg')
            if (s is not None and s < -1.5) or (m is not None and m < 4.5):
                overnight_risk = 'elevated'

    # Scott 2021 early-recovery carb trigger: BG < 8.0 mmol/L in 0–2 h post
    early = next((p for p in recovery_phases if p['key'] == 'post_0_2h'), {})
    early_carb_trigger = (
        early.get('min_bg') is not None
        and early['min_bg'] < 8.0
        and early.get('count', 0) > 0
    )

    duration_h    = metrics.get('duration_h') or 0.0
    total_carbs_g = round(duration_h * carbs_per_hour_g)

    return {
        'prewalk': {
            'bg_at_start':    bg_at_start,
            'target_range':   [prewalk_target_min, prewalk_target_max],
            'assessment':     prewalk_assessment,
            'iob_at_start':   iob_at_start,
            'iob_flag':       iob_flag,
        },
        'during': {
            'intensity':              intensity,
            'bg_at_end':              bg_at_end,
            'drop_rate_mmol_h':       slope,
            'drop_flag':              drop_flag,
            'projected_hypo_min':     projected_hypo_min,
            'first_30_drop_rate':     first_30_drop_rate,
            'first_30_flag':          first_30_flag,
            'rest_drop_rate':         rest_drop_rate,
            'ihe_burst_count':        ihe_burst_count,
            'post_exercise_hyper':    post_exercise_hyper,
        },
        'recovery': {
            'early_carb_trigger_met': early_carb_trigger,
            'overnight_risk':         overnight_risk,
            'risk_window_end_iso':    risk_window_end_iso,
        },
        'nextWalk': {
            'target_bg_range':            [prewalk_target_min, prewalk_target_max],
            'intensity':                  intensity,
            'basal_reduction_pct':        basal_reduction_pct,
            'basal_timing_min':           90,    # Riddell: start 90 min before
            'bolus_reduction_pct':        bolus_reduction_pct,
            'carbs_per_hour_g':           carbs_per_hour_g,
            'total_carbs_g':              total_carbs_g,
            'recovery_bolus_reduction_pct': 35,  # Scott 2021: 20–50%
            'recovery_basal_reduction_h': 9,     # Scott 2021: 6–12 h
            # 780G SmartGuard: raise CL temp target to 8.3 mmol/L (150 mg/dL),
            # start 90 min before activity, cancel when BG returns to normal range.
            # Zaharieva 2020 (doi:10.1016/j.jcjd.2020.06.003), Boom 2021 (doi:10.1111/jdi.13356)
            'cl_temp_target_mmol':        8.3,
            'cl_temp_target_mgdl':        150,
            'cl_temp_start_min_before':   90,
        },
    }


def _stress_summary_for_walk_dir(walk_dir: Path) -> dict | None:
    fit_files = sorted(walk_dir.glob('*.fit'))
    if not fit_files:
        return None

    activity_points: list[dict] = []
    for fit_path in fit_files:
        parsed = _parse_fit_records(fit_path.read_bytes())
        activity_points.extend(parsed['points'])
    activity_points.sort(key=lambda p: p['timestamp'])
    if not activity_points:
        return None

    stress = _stress_decoupling_analytics(activity_points, [])
    summary = stress.get('summary') or {}
    if summary.get('score') is None:
        return None

    meta = _load_walk_meta(walk_dir)
    metrics = _summary_metrics(activity_points, [], [])
    return {
        'walk_id': meta.get('id'),
        'date': meta.get('date'),
        'name': meta.get('name') or meta.get('date') or meta.get('id'),
        'start_time': meta.get('start_time') or activity_points[0].get('timestamp_iso'),
        'score': summary.get('score'),
        'band': summary.get('band'),
        'elevated_minutes': summary.get('elevated_minutes'),
        'distance_km': metrics.get('distance_km'),
    }


def _fmt(value: float | None, digits: int = 2, suffix: str = '') -> str:
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return 'n/a'
    return f'{value:.{digits}f}{suffix}'


def _etag_for_payload(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _cache_headers(*, public: bool) -> dict[str, str]:
    if public:
        return {
            "Cache-Control": "public, max-age=300, s-maxage=3600, stale-while-revalidate=86400",
        }
    return {
        "Cache-Control": "private, max-age=0, must-revalidate",
    }


def _json_with_cache(payload: dict, request: Request, *, public: bool) -> Response:
    etag = _etag_for_payload(payload)
    request_etag = request.headers.get("if-none-match", "").strip().strip('"')
    headers = {
        **_cache_headers(public=public),
        "ETag": f'"{etag}"',
    }
    if request_etag and request_etag == etag:
        return Response(status_code=304, headers=headers)
    return JSONResponse(content=payload, headers=headers)


@app.post("/api/walks/parse-fit-date")
async def parse_fit_date(file: UploadFile = File(...), user: dict = Depends(_require_user)):
    _ = user
    if not file.filename:
        raise HTTPException(status_code=400, detail="Filename missing")
    
    # Sanitize filename and validate .fit extension
    safe_filename = Path(file.filename.replace('\\', '/')).name
    if not safe_filename or safe_filename in ('.', '..'):
        raise HTTPException(status_code=400, detail="Invalid filename")
    if not safe_filename.lower().endswith('.fit'):
        raise HTTPException(status_code=400, detail="Only .fit files are supported")
        
    data = await file.read()
    
    # Enforce 10MB size limit
    if len(data) > 10 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="File exceeds maximum allowed size of 10MB")
        
    date_str = _fit_start_date(data)
    if not date_str:
        raise HTTPException(status_code=422, detail="Could not extract date from FIT file")
    return {"date": date_str}


def _prefetch_analytics_background(walk_ids: list[str], data_root: Path) -> None:
    for walk_id in walk_ids:
        try:
            refresh_walk_analytics_if_needed(
                walk_id,
                find_walk_dir=lambda wid: _find_walk_dir(wid, data_root),
                get_walk_analysis_data=lambda wid, persist_analytics=False: _get_walk_analysis_data(
                    wid,
                    persist_analytics=persist_analytics,
                    data_root=data_root,
                ),
            )
        except Exception:
            pass


@app.get("/api/walks")
def list_walks(background_tasks: BackgroundTasks, user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    walks = []
    uncached_ids: list[str] = []
    for walk_dir in _iter_walk_dirs(data_root):
        meta = _load_walk_meta(walk_dir)
        cached = get_cached_analytics_row(meta["id"])
        needs_prefetch = (
            cached is None or cached["analytics_version"] != ANALYTICS_VERSION
        ) and any(walk_dir.glob("*.fit"))
        if needs_prefetch:
            uncached_ids.append(meta["id"])
        if cached and cached.get("analytics_version") == ANALYTICS_VERSION:
            meta["metrics"] = {
                "distance_km": cached.get("distance_km"),
                "duration_h": cached.get("duration_h"),
                "avg_hr": cached.get("avg_hr"),
                "bg_delta": cached.get("bg_delta"),
                "tir_pct": cached.get("tir_pct"),
            }
        else:
            meta["metrics"] = None
        walks.append(meta)
    if uncached_ids:
            background_tasks.add_task(_prefetch_analytics_background, uncached_ids, data_root)
    walks.sort(key=lambda walk: (walk.get("start_time") or "", walk["date"], walk["id"]), reverse=True)
    return walks


@app.post("/api/walks/upload")
async def upload_files(
    date: str = Form(...),
    name: Optional[str] = Form(""),
    files: List[UploadFile] = File(...),
    user: dict = Depends(_require_user),
):
    date = _parse_date(date)
    data_root = _user_data_dir(user['sub'])
    date_dir = data_root / date
    date_dir.mkdir(exist_ok=True)

    prepared_files: list[tuple[str, bytes]] = []
    fit_start_dt: datetime | None = None
    fit_end_dt: datetime | None = None
    fit_identity: str | None = None
    has_uploaded_carelink_csv = False
    
    MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
    
    for file in files:
        if not file.filename:
            continue
            
        # 1. Sanitize filename to prevent directory traversal
        safe_filename = Path(file.filename.replace('\\', '/')).name
        if not safe_filename or safe_filename in ('.', '..'):
            raise HTTPException(status_code=400, detail="Invalid filename")
            
        # 2. Validate file extension allow-list
        ext = Path(safe_filename).suffix.lower()
        if ext not in ('.fit', '.csv', '.gpx'):
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file type '{ext}' for file '{safe_filename}'. Only .fit, .csv, and .gpx files are allowed."
            )
            
        data = await file.read()
        
        # 3. Enforce maximum file size limit
        if len(data) > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"File '{safe_filename}' exceeds the maximum allowed size of 10MB"
            )
            
        prepared_files.append((safe_filename, data))
        
        if fit_start_dt is None and safe_filename.lower().endswith('.fit'):
            fit_start_dt, fit_end_dt = _fit_time_bounds(data)
        if fit_identity is None and safe_filename.lower().endswith('.fit'):
            fit_identity = _fit_file_identity(data)
        if safe_filename.lower().endswith('.csv'):
            has_uploaded_carelink_csv = True

    if fit_identity is not None:
        for existing_walk_dir in _iter_walk_dirs(data_root):
            existing_meta = _load_walk_meta(existing_walk_dir)
            existing_fit_identity = existing_meta.get("fit_identity")
            if existing_fit_identity is None:
                existing_fit_files = sorted(existing_walk_dir.glob('*.fit'))
                if existing_fit_files:
                    existing_fit_identity = _fit_file_identity(existing_fit_files[0].read_bytes())
            if existing_fit_identity == fit_identity:
                existing_label = existing_meta.get("name") or existing_meta["date"]
                raise HTTPException(
                    status_code=409,
                    detail=f"That FIT activity is already loaded as {existing_label} ({existing_meta['id']})",
                )

    reused_carelink: tuple[Path, dict] | None = None
    if not has_uploaded_carelink_csv:
        reused_carelink = _find_reusable_carelink_csv(fit_start_dt, fit_end_dt, data_root=data_root)
        if reused_carelink is None:
            raise HTTPException(
                status_code=400,
                detail="No Carelink CSV uploaded, and no existing loaded CSV covers this walk's activity window",
            )

    walk_name = (name or "").strip()
    walk_id = _make_walk_id(date, fit_start_dt, walk_name, date_dir, user_sub=user['sub'])
    walk_dir = date_dir / walk_id
    walk_dir.mkdir(exist_ok=False)

    meta = {
        "id": walk_id,
        "date": date,
        "name": walk_name,
        "start_time": fit_start_dt.isoformat() if fit_start_dt is not None else None,
        "fit_identity": fit_identity,
    }
    if reused_carelink is not None:
        reused_csv_path, reused_meta = reused_carelink
        meta["carelink_reused_from"] = reused_meta["id"]
        meta["carelink_start_time"] = reused_meta.get("carelink_start_time")
        meta["carelink_end_time"] = reused_meta.get("carelink_end_time")
    (walk_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

    saved = []
    reused = []
    for filename, data in prepared_files:
        dest = walk_dir / filename
        dest.write_bytes(data)
        saved.append(filename)

    if reused_carelink is not None:
        reused_csv_path, _ = reused_carelink
        dest = walk_dir / reused_csv_path.name
        dest.write_bytes(reused_csv_path.read_bytes())
        reused.append(reused_csv_path.name)

    return {"id": walk_id, "date": date, "uploaded": saved, "reused": reused}


@app.delete("/api/walks/{walk_id}")
def delete_walk(walk_id: str, user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    walk_dir = _find_walk_dir(walk_id, data_root)
    if walk_dir is None or not walk_dir.exists():
        raise HTTPException(status_code=404, detail="Walk not found")
    shutil.rmtree(walk_dir)
    delete_walk_analytics(walk_id)
    parent_dir = walk_dir.parent
    if parent_dir != DATA_DIR and parent_dir.exists() and not any(parent_dir.iterdir()):
        parent_dir.rmdir()
    return {"deleted": walk_id}


def _get_walk_analysis_data(
    walk_id: str,
    persist_analytics: bool = False,
    data_root: Path = DATA_DIR,
) -> tuple[str, str | None, dict, dict]:
    walk_dir = _find_walk_dir(walk_id, data_root)
    if walk_dir is None or not walk_dir.exists() or not walk_dir.is_dir():
        raise HTTPException(status_code=404, detail='Walk not found')

    walk_meta = _load_walk_meta(walk_dir)
    date = walk_meta['date']
    walk_name = walk_meta['name']

    fit_files = sorted(walk_dir.glob('*.fit'))
    csv_files = sorted(walk_dir.glob('*.csv'))
    gpx_files = sorted(walk_dir.glob('*.gpx'))

    activity_points: list[dict] = []
    fit_track: list[list[float]] = []
    for fit_path in fit_files:
        parsed = _parse_fit_records(fit_path.read_bytes())
        activity_points.extend(parsed['points'])
        fit_track.extend(parsed['track'])

    activity_points.sort(key=lambda p: p['timestamp'])
    activity_series = []
    for p in activity_points:
        activity_series.append({
            'timestamp_iso': p['timestamp_iso'],
            'distance_km': p['distance_m'] / 1000.0 if p.get('distance_m') is not None else None,
            'hr': p.get('hr'),
            'altitude_m': p.get('altitude_m'),
        })

    if activity_points:
        start_dt = datetime.fromisoformat(activity_points[0]['timestamp_iso']) - BG_CONTEXT_WINDOW
        end_dt = datetime.fromisoformat(activity_points[-1]['timestamp_iso']) + BG_CONTEXT_WINDOW
        walk_start_ts = activity_points[0]['timestamp']
        walk_end_ts = activity_points[-1]['timestamp']
        walk_start_unix = datetime.fromisoformat(activity_points[0]['timestamp_iso']).timestamp()
        walk_end_unix = datetime.fromisoformat(activity_points[-1]['timestamp_iso']).timestamp()
    else:
        start_dt = None
        end_dt = None
        walk_start_ts = None
        walk_end_ts = None
        walk_start_unix = None
        walk_end_unix = None

    chart_distance_values = [
        p['distance_m'] / 1000.0 for p in activity_points if p.get('distance_m') is not None
    ]
    walk_distance_end = max(chart_distance_values) if chart_distance_values else None
    walk_duration_secs = (walk_end_ts - walk_start_ts) if walk_start_ts is not None and walk_end_ts is not None else None
    bg_buffer_secs = int(BG_CONTEXT_WINDOW.total_seconds())
    bg_buffer_km = (
        walk_distance_end * (bg_buffer_secs / walk_duration_secs)
        if walk_distance_end is not None and walk_duration_secs and walk_duration_secs > 0
        else 0.0
    )
    chart_distance_start = -bg_buffer_km if chart_distance_values else None
    chart_distance_end = (walk_distance_end + bg_buffer_km) if walk_distance_end is not None else None

    bg_points: list[dict] = []
    basal_points: list[dict] = []
    bolus_events: list[dict] = []
    smartguard_events: list[dict] = []
    for csv_path in csv_files:
        parsed = _parse_carelink_csv(csv_path)
        bg_points.extend(parsed['bg'])
        basal_points.extend(parsed['basal'])
        bolus_events.extend(parsed['bolus'])
        smartguard_events.extend(parsed.get('smartguard', []))

    bg_points.sort(key=lambda x: x['timestamp'])
    basal_points.sort(key=lambda x: x['timestamp'])
    bolus_events.sort(key=lambda x: x['timestamp'])
    smartguard_events.sort(key=lambda x: x['timestamp'])
    bolus_events_all = list(bolus_events)

    # Keep unfiltered basal samples so we can carry the last known rate into chart start.
    basal_points_all = list(basal_points)
    # Keep full BG list for extended recovery and risk-profile analytics.
    bg_points_all = list(bg_points)

    bg_points = _window_filter(bg_points, start_dt, end_dt, 'timestamp')
    basal_points = _window_filter(basal_points, start_dt, end_dt, 'timestamp')
    bolus_events = _window_filter(bolus_events, start_dt, end_dt, 'timestamp')
    smartguard_events = _window_filter(smartguard_events, start_dt, end_dt, 'timestamp')

    # Build (posix_ts, distance_km) lookup for interpolation
    dist_pairs: list[tuple[float, float]] = [
        (datetime.fromisoformat(p['timestamp_iso']).timestamp(), p['distance_m'] / 1000.0)
        for p in activity_points if p.get('distance_m') is not None
    ]

    for pt in bg_points:
        point_ts = datetime.fromisoformat(pt['timestamp']).timestamp()
        if walk_start_unix is not None and point_ts < walk_start_unix:
            pt['distance_km'] = max(
                -bg_buffer_km,
                ((point_ts - walk_start_unix) / bg_buffer_secs) * bg_buffer_km,
            )
        elif walk_end_unix is not None and walk_distance_end is not None and point_ts > walk_end_unix:
            pt['distance_km'] = min(
                walk_distance_end + bg_buffer_km,
                walk_distance_end + ((point_ts - walk_end_unix) / bg_buffer_secs) * bg_buffer_km,
            )
        else:
            pt['distance_km'] = _ts_to_dist_km(point_ts, dist_pairs)
    def _ts_to_chart_dist(ts_secs: float) -> float | None:
        if walk_start_unix is not None and ts_secs < walk_start_unix:
            return max(
                -bg_buffer_km,
                ((ts_secs - walk_start_unix) / bg_buffer_secs) * bg_buffer_km,
            )
        if walk_end_unix is not None and walk_distance_end is not None and ts_secs > walk_end_unix:
            return min(
                walk_distance_end + bg_buffer_km,
                walk_distance_end + ((ts_secs - walk_end_unix) / bg_buffer_secs) * bg_buffer_km,
            )
        return _ts_to_dist_km(ts_secs, dist_pairs)

    if dist_pairs and walk_start_unix is not None and walk_end_unix is not None and basal_points_all:
        chart_start_unix = walk_start_unix - bg_buffer_secs
        chart_end_unix = walk_end_unix + bg_buffer_secs

        basal_rows: list[tuple[float, float]] = []
        for point in basal_points_all:
            try:
                ts_secs = datetime.fromisoformat(point['timestamp']).timestamp()
                rate = float(point['rate'])
            except (KeyError, TypeError, ValueError):
                continue
            basal_rows.append((ts_secs, rate))
        basal_rows.sort(key=lambda row: row[0])

        carry_rate = None
        for ts_secs, rate in basal_rows:
            if ts_secs <= chart_start_unix:
                carry_rate = rate
            else:
                break

        chart_basal: list[dict] = []
        if carry_rate is not None:
            chart_basal.append({
                'timestamp': datetime.fromtimestamp(chart_start_unix, tz=timezone.utc).isoformat(),
                'rate': carry_rate,
                'distance_km': chart_distance_start,
            })

        for ts_secs, rate in basal_rows:
            if ts_secs < chart_start_unix or ts_secs > chart_end_unix:
                continue
            dist = _ts_to_chart_dist(ts_secs)
            if dist is None:
                continue
            chart_basal.append({
                'timestamp': datetime.fromtimestamp(ts_secs, tz=timezone.utc).isoformat(),
                'rate': rate,
                'distance_km': dist,
            })

        if chart_basal:
            compressed: list[dict] = [chart_basal[0]]
            for point in chart_basal[1:]:
                prev = compressed[-1]
                if abs(float(point['rate']) - float(prev['rate'])) > 1e-9:
                    compressed.append(point)
            last_rate = compressed[-1]['rate']
            compressed.append({
                'timestamp': datetime.fromtimestamp(chart_end_unix, tz=timezone.utc).isoformat(),
                'rate': last_rate,
                'distance_km': chart_distance_end,
            })
            basal_points = compressed
        else:
            basal_points = []
    else:
        for pt in basal_points:
            pt['distance_km'] = _ts_to_dist_km(datetime.fromisoformat(pt['timestamp']).timestamp(), dist_pairs)

    for pt in bolus_events:
        pt['distance_km'] = _ts_to_dist_km(datetime.fromisoformat(pt['timestamp']).timestamp(), dist_pairs)

    for pt in smartguard_events:
        point_ts = datetime.fromisoformat(pt['timestamp']).timestamp()
        pt['distance_km'] = _ts_to_chart_dist(point_ts)

    # Time markers: every 30 min from walk start
    time_markers: list[dict] = []
    map_hour_markers: list[dict] = []
    if activity_points and dist_pairs:
        start_ts = activity_points[0]['timestamp']
        end_ts = activity_points[-1]['timestamp']
        t = start_ts + 1800
        while t <= end_ts:
            elapsed_min = (t - start_ts) // 60
            dist = _ts_to_dist_km(
                (FIT_EPOCH + timedelta(seconds=t)).timestamp(), dist_pairs
            )
            if dist is not None:
                h, m = divmod(int(elapsed_min), 60)
                label = f'{h}h{m:02d}' if h else f'{int(elapsed_min)}min'
                time_markers.append({'distance_km': dist, 'label': label})
            t += 1800

        activity_points_with_coords = [
            point for point in activity_points
            if point.get('lat') is not None and point.get('lon') is not None
        ]
        t = start_ts + 3600
        while t <= end_ts and activity_points_with_coords:
            nearest_point = min(
                activity_points_with_coords,
                key=lambda point: abs(point['timestamp'] - t),
            )
            elapsed_hours = int((t - start_ts) // 3600)
            map_hour_markers.append({
                'lat': nearest_point['lat'],
                'lon': nearest_point['lon'],
                'label': f'{elapsed_hours}h',
            })
            t += 3600

    bg_pairs_for_map: list[tuple[float, float]] = []
    for point in bg_points:
        try:
            ts_unix = datetime.fromisoformat(point['timestamp']).timestamp()
            bg_val = float(point['bg'])
        except (KeyError, TypeError, ValueError):
            continue
        bg_pairs_for_map.append((ts_unix, bg_val))
    bg_pairs_for_map.sort(key=lambda x: x[0])

    map_track: list[list[float | None]] = []
    for point in activity_points:
        if point.get('lat') is None or point.get('lon') is None:
            continue
        try:
            point_ts = datetime.fromisoformat(point['timestamp_iso']).timestamp()
        except (KeyError, ValueError):
            point_ts = None
        bg_val = _interp_bg(point_ts, bg_pairs_for_map) if point_ts is not None and bg_pairs_for_map else None
        map_track.append([
            point['lat'],
            point['lon'],
            point.get('hr'),
            bg_val,
        ])

    if not map_track:
        if fit_track:
            map_track = fit_track
        elif gpx_files:
            map_track = _parse_gpx_track(gpx_files[0])
        else:
            map_track = []

    weather_points: list[dict] = []
    if activity_points and dist_pairs:
        if map_track:
            weather_lat = map_track[0][0]
            weather_lon = map_track[0][1]
        else:
            coord_points = [p for p in activity_points if p.get('lat') is not None and p.get('lon') is not None]
            weather_lat = coord_points[0]['lat'] if coord_points else None
            weather_lon = coord_points[0]['lon'] if coord_points else None

        if weather_lat is not None and weather_lon is not None:
            activity_start_dt = datetime.fromisoformat(activity_points[0]['timestamp_iso'])
            activity_end_dt = datetime.fromisoformat(activity_points[-1]['timestamp_iso'])
            weather_rows = _fetch_open_meteo_weather(weather_lat, weather_lon, activity_start_dt, activity_end_dt)
            for row in weather_rows:
                try:
                    ts = datetime.fromisoformat(row['timestamp'])
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                except (KeyError, ValueError):
                    continue
                distance_km = _ts_to_dist_km(ts.timestamp(), dist_pairs)
                if distance_km is None:
                    continue
                weather_points.append({
                    'timestamp': row['timestamp'],
                    'distance_km': distance_km,
                    'temp_c': row.get('temp_c'),
                    'apparent_c': row.get('apparent_c'),
                    'wind_kph': row.get('wind_kph'),
                    'wind_dir_deg': row.get('wind_dir_deg'),
                })

    metrics = _summary_metrics(activity_points, bg_points, bolus_events)
    sensor_gaps = _sensor_gap_detection(activity_points, bg_points_all)
    recovery_phases = _extended_recovery_phases(activity_points, bg_points_all)
    risk_profile = _exercise_risk_profile(activity_points, bg_points_all, metrics, recovery_phases, bolus_events_all)
    smartguard_summary = _smartguard_summary(activity_points, smartguard_events)
    weather_effort = _weather_effort_analytics(activity_points, weather_points, metrics.get('duration_h'))
    stress_analytics = _stress_decoupling_analytics(activity_points, weather_points)
    insulin_stress_effect = _insulin_stress_effect_analytics(activity_points, bolus_events_all, weather_points)

    trend_rows: list[dict] = []
    current_summary = stress_analytics.get('summary') or {}
    if current_summary.get('score') is not None:
        trend_rows.append({
            'walk_id': walk_meta.get('id'),
            'date': walk_meta.get('date'),
            'name': walk_name or walk_meta.get('date') or walk_meta.get('id'),
            'start_time': walk_meta.get('start_time') or (activity_points[0]['timestamp_iso'] if activity_points else None),
            'score': current_summary.get('score'),
            'band': current_summary.get('band'),
            'elevated_minutes': current_summary.get('elevated_minutes'),
            'distance_km': metrics.get('distance_km'),
        })

    for other_walk_dir in _iter_walk_dirs(data_root):
        if other_walk_dir == walk_dir:
            continue
        other_summary = _stress_summary_for_walk_dir(other_walk_dir)
        if other_summary is not None:
            trend_rows.append(other_summary)

    trend_rows.sort(key=lambda r: (r.get('start_time') or '', r.get('date') or '', r.get('walk_id') or ''))

    payload = {
        'activity': activity_series,
        'bg': bg_points,
        'basal': basal_points,
        'bolus': bolus_events,
        'phaseAnalytics': _phase_glucose_analytics(activity_points, bg_points),
        'intensityAnalytics': _intensity_glucose_analytics(activity_points, bg_points),
        'sensorGaps': sensor_gaps,
        'recoveryPhases': recovery_phases,
        'riskProfile': risk_profile,
        'smartguard': {
            'events': smartguard_events,
            'summary': smartguard_summary,
        },
        'weather': weather_points,
        'windProfile': weather_effort['wind_profile'],
        'windRose': weather_effort['wind_rose'],
        'stressAnalytics': stress_analytics,
        'insulinStressEffect': insulin_stress_effect,
        'stressTrend': trend_rows,
        'mapTrack': map_track,
        'mapHourMarkers': map_hour_markers,
        'timeMarkers': time_markers,
        'walkDistanceStart': 0.0 if chart_distance_values else None,
        'walkDistanceEnd': walk_distance_end,
        'chartDistanceStart': chart_distance_start,
        'chartDistanceEnd': chart_distance_end,
    }
    metrics['bg_slope_during_h'] = payload['phaseAnalytics'].get('during_slope_per_hour')
    metrics.update(_weather_metrics(weather_points))
    metrics.update({
        'headwind_exposure_pct': weather_effort.get('headwind_exposure_pct'),
        'headwind_avg_kph': weather_effort.get('headwind_avg_kph'),
        'tailwind_avg_kph': weather_effort.get('tailwind_avg_kph'),
        'weather_stress_score': weather_effort.get('weather_stress_score'),
        'weather_stress_band': weather_effort.get('weather_stress_band'),
        'hr_decoupling_score': stress_analytics['summary'].get('score'),
        'hr_decoupling_band': stress_analytics['summary'].get('band'),
        'hr_elevated_minutes': stress_analytics['summary'].get('elevated_minutes'),
        'insulin_stress_multiplier': insulin_stress_effect['summary'].get('stress_multiplier'),
        'insulin_stress_band': insulin_stress_effect['summary'].get('band'),
        'insulin_iob_overlap_minutes': insulin_stress_effect['summary'].get('overlap_minutes'),
        'insulin_iob_weighted_overlap_minutes': insulin_stress_effect['summary'].get('weighted_overlap_minutes'),
        'smartguard_event_count': smartguard_summary.get('event_count'),
        'smartguard_predicted_low_suspend_count': smartguard_summary.get('predicted_low_suspend_count'),
        'smartguard_suspend_count': smartguard_summary.get('suspend_count'),
        'smartguard_resume_count': smartguard_summary.get('resume_count'),
        'smartguard_active': smartguard_summary.get('any_activity'),
    })

    route_fingerprint = _compute_route_fingerprint(map_track, metrics.get("distance_km"))
    route_series = (
        _compute_route_series(activity_points, bg_points, weather_points, metrics["distance_km"])
        if route_fingerprint and metrics.get("distance_km")
        else None
    )

    if persist_analytics:
        persist_walk_analytics(walk_dir, walk_meta, payload, metrics,
                               route_fingerprint=route_fingerprint, route_series=route_series)

    return date, walk_name, payload, metrics


@app.get('/api/analytics/walks')
def list_walk_analytics(user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    return list_cached_walk_analytics(
        iter_walk_dirs=lambda: _iter_walk_dirs(data_root),
        load_walk_meta=_load_walk_meta,
    )


@app.post('/api/analytics/{walk_id}/refresh')
def refresh_walk_analytics(walk_id: str, force: bool = False, user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    return refresh_walk_analytics_if_needed(
        walk_id,
        force=force,
        find_walk_dir=lambda wid: _find_walk_dir(wid, data_root),
        get_walk_analysis_data=lambda wid, persist_analytics=False: _get_walk_analysis_data(
            wid,
            persist_analytics=persist_analytics,
            data_root=data_root,
        ),
    )


@app.post('/api/analytics/backfill')
def backfill_walk_analytics(force: bool = False, user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    results: list[dict] = []
    for walk_dir in _iter_walk_dirs(data_root):
        results.append(
            refresh_walk_analytics_if_needed(
                walk_dir.name,
                force=force,
                find_walk_dir=lambda wid: _find_walk_dir(wid, data_root),
                get_walk_analysis_data=lambda wid, persist_analytics=False: _get_walk_analysis_data(
                    wid,
                    persist_analytics=persist_analytics,
                    data_root=data_root,
                ),
            )
        )
    return {
        'analytics_version': ANALYTICS_VERSION,
        'walk_count': len(results),
        'results': results,
    }


@app.get('/api/walks/{walk_id}/analysis-data')
def walk_analysis_data(walk_id: str, request: Request, user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    date, walk_name, payload, metrics = _get_walk_analysis_data(walk_id, persist_analytics=True, data_root=data_root)
    walk_dir = _find_walk_dir(walk_id, data_root)
    files = sorted(f.name for f in walk_dir.iterdir() if f.is_file() and f.name != 'meta.json') if walk_dir else []
    meta = _load_walk_meta(walk_dir) if walk_dir else {}
    settings = get_user_settings(user['sub'])
    result = {
        'walk_id': walk_id,
        'date': date,
        'name': walk_name,
        'rating': meta.get('rating', 3) if walk_dir else 3,
        'payload': payload,
        'metrics': metrics,
        'files': files,
        'user_settings': settings,
    }
    return _json_with_cache(result, request, public=False)


@app.get('/api/walks/{walk_id}/analysis')
def walk_analysis(walk_id: str, request: Request, user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    date, walk_name, payload, metrics = _get_walk_analysis_data(walk_id, persist_analytics=True, data_root=data_root)
    walk_dir = _find_walk_dir(walk_id, data_root)
    meta = _load_walk_meta(walk_dir) if walk_dir else {}
    settings = get_user_settings(user['sub'])
    result = {
        'walk_id': walk_id,
        'date': date,
        'name': walk_name,
        'rating': meta.get('rating', 3) if walk_dir else 3,
        'payload': payload,
        'metrics': metrics,
        'user_settings': settings,
    }
    return _json_with_cache(result, request, public=False)


@app.post('/api/walks/{walk_id}/share-links')
def create_walk_share_link(walk_id: str, user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    walk_dir = _find_walk_dir(walk_id, data_root)
    if walk_dir is None:
        raise HTTPException(status_code=404, detail='Walk not found')

    token = secrets.token_urlsafe(18)
    created_at = datetime.now(timezone.utc).isoformat()
    create_share_link(token, user['sub'], walk_id, created_at)
    return {
        'token': token,
        'walk_id': walk_id,
        'created_at': created_at,
        'share_path': f'/api/share/{token}/analysis-data',
    }


@app.get('/api/walks/{walk_id}/share-links')
def list_walk_share_links(walk_id: str, user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    walk_dir = _find_walk_dir(walk_id, data_root)
    if walk_dir is None:
        raise HTTPException(status_code=404, detail='Walk not found')
    rows = list_share_links_for_walk(user['sub'], walk_id)
    return {
        'walk_id': walk_id,
        'links': rows,
    }


def _get_user_walk_windows_and_summaries(user_sub: str, data_root: Path) -> tuple[list[dict], dict[str, list[dict]]]:
    """Build walk time windows (for 15-min epoch deduplication) and per-date linked walk summaries."""
    walk_windows: list[dict] = []
    linked_by_date: dict[str, list[dict]] = {}
    if not data_root.exists() or not data_root.is_dir():
        return walk_windows, linked_by_date

    for walk_dir in _iter_walk_dirs(data_root):
        meta = _load_walk_meta(walk_dir)
        walk_id = meta.get("id") or walk_dir.name
        walk_date = meta.get("date") or walk_dir.parent.name
        walk_name = meta.get("name") or "Walk"
        start_iso = meta.get("start_time")

        fit_files = list(walk_dir.glob("*.fit"))
        start_dt = None
        end_dt = None
        if fit_files:
            try:
                start_dt, end_dt = _fit_time_bounds(fit_files[0].read_bytes())
            except Exception:
                start_dt, end_dt = None, None

        if start_dt is None and start_iso:
            try:
                start_dt = datetime.fromisoformat(str(start_iso).replace("Z", "+00:00"))
                if start_dt.tzinfo is None:
                    start_dt = start_dt.replace(tzinfo=timezone.utc)
            except ValueError:
                start_dt = None

        cached = get_cached_analytics_row(walk_id)
        metrics = (cached or {}).get("metrics") or {}
        if end_dt is None and start_dt is not None:
            dur_h = metrics.get("duration_h")
            dur_s = float(dur_h) * 3600.0 if dur_h else 3600.0
            end_dt = start_dt + timedelta(seconds=dur_s)

        if start_dt and end_dt:
            walk_windows.append({
                "walk_id": walk_id,
                "date": walk_date,
                "start_utc": start_dt,
                "end_utc": end_dt,
            })

        linked_by_date.setdefault(walk_date, []).append({
            "id": walk_id,
            "name": walk_name,
            "start_time": start_dt.isoformat() if start_dt else start_iso,
            "distance_km": metrics.get("distance_km"),
            "tir_pct": metrics.get("tir_pct"),
        })

    return walk_windows, linked_by_date


def _sanitize_shared_settings(settings: dict | None) -> dict:
    """Strip private emails/usernames from settings before returning on public share links."""
    if not settings:
        return {}
    sanitized = dict(settings)
    for k in ("user_sub", "garmin_email", "carelink_email", "dexcom_username"):
        sanitized.pop(k, None)
    return sanitized


@app.get('/api/daily/range')
def daily_range_analysis(
    request: Request,
    start_date: str | None = None,
    end_date: str | None = None,
    days: int = 7,
    force: bool = False,
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    try:
        date_list = resolve_sync_date_range(
            start_date=start_date,
            end_date=end_date,
            default_days=min(max(1, days), 90),
            max_days=90,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    data_root = _user_data_dir(user_sub)
    _, linked_by_date = _get_user_walk_windows_and_summaries(user_sub, data_root)
    result = get_or_compute_daily_range_analytics(
        user_sub=user_sub,
        start_date=date_list[0],
        end_date=date_list[-1],
        force=force,
        linked_walks_by_date=linked_by_date,
    )
    return _json_with_cache(result, request, public=False)


@app.get('/api/daily/share-links')
def list_daily_share_links_endpoint(
    start_date: str | None = None,
    end_date: str | None = None,
    share_type: str | None = None,
    user: dict = Depends(_require_user),
):
    rows = list_share_links_for_target(
        owner_sub=user['sub'],
        start_date=start_date,
        end_date=end_date,
        share_type=share_type,
    )
    return {"links": rows}


@app.post('/api/daily/share-links')
def create_daily_share_link_endpoint(
    payload: dict = Body(...),
    user: dict = Depends(_require_user),
):
    share_type = payload.get("share_type", "daily")
    if share_type not in ("daily", "range"):
        raise HTTPException(status_code=400, detail="share_type must be 'daily' or 'range'")

    start_date = (payload.get("start_date") or payload.get("date") or "").strip()[:10]
    end_date = (payload.get("end_date") or start_date).strip()[:10]
    if not start_date:
        raise HTTPException(status_code=400, detail="start_date is required")

    try:
        s_dt = date_type.fromisoformat(start_date)
        e_dt = date_type.fromisoformat(end_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Dates must be in YYYY-MM-DD format")
    if e_dt < s_dt:
        raise HTTPException(status_code=400, detail="end_date must be >= start_date")

    expires_in_days = payload.get("expires_in_days")
    now_dt = datetime.now(timezone.utc)
    created_at = now_dt.isoformat()
    expires_at = None
    if expires_in_days is not None:
        try:
            expires_at = (now_dt + timedelta(days=int(expires_in_days))).isoformat()
        except (TypeError, ValueError):
            expires_at = None

    token = secrets.token_urlsafe(18)
    synthetic_target_id = f"{share_type}:{start_date}:{end_date}"
    create_share_link(
        token=token,
        owner_sub=user['sub'],
        walk_id=synthetic_target_id,
        created_at=created_at,
        expires_at=expires_at,
        share_type=share_type,
        target_start_date=start_date,
        target_end_date=end_date,
    )
    return {
        "token": token,
        "share_type": share_type,
        "start_date": start_date,
        "end_date": end_date,
        "created_at": created_at,
        "expires_at": expires_at,
        "share_path": f"/api/shared/{token}/analysis",
    }


@app.get('/api/daily/{date}/analysis')
def daily_analysis_endpoint(
    date: str,
    request: Request,
    force: bool = False,
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    data_root = _user_data_dir(user_sub)
    _, linked_by_date = _get_user_walk_windows_and_summaries(user_sub, data_root)
    result = get_or_compute_daily_analytics(
        user_sub=user_sub,
        local_date=date,
        force=force,
        linked_walks=linked_by_date.get(date, []),
    )
    return _json_with_cache(result, request, public=False)


@app.delete('/api/share-links/{token}')
def revoke_walk_share_link(token: str, user: dict = Depends(_require_user)):
    revoked = revoke_share_link(token, user['sub'], datetime.now(timezone.utc).isoformat())
    if not revoked:
        raise HTTPException(status_code=404, detail='Share link not found')
    return {'revoked': token}


@app.get('/api/share/{token}/analysis-data')
@app.get('/api/shared/{token}/analysis')
def shared_walk_analysis_data(token: str, request: Request):
    share = get_share_link(token)
    if share is None:
        raise HTTPException(status_code=404, detail='Share link not found')
    if share.get('revoked_at'):
        raise HTTPException(status_code=404, detail='Share link revoked')

    expires_at = share.get('expires_at')
    if expires_at:
        try:
            expiry_dt = datetime.fromisoformat(expires_at)
            if expiry_dt.tzinfo is None:
                expiry_dt = expiry_dt.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) > expiry_dt:
                raise HTTPException(status_code=404, detail='Share link expired')
        except ValueError:
            raise HTTPException(status_code=500, detail='Invalid share link expiry')

    owner_sub = share['owner_sub']
    share_type = share.get('share_type') or 'walk'
    data_root = _user_data_dir(owner_sub)
    settings = _sanitize_shared_settings(get_user_settings(owner_sub))

    if share_type == 'daily':
        target_date = share.get('target_start_date') or share['walk_id'].split(':')[-1]
        _, linked_by_date = _get_user_walk_windows_and_summaries(owner_sub, data_root)
        daily_payload = get_or_compute_daily_analytics(
            user_sub=owner_sub,
            local_date=target_date,
            linked_walks=linked_by_date.get(target_date, []),
        )
        result = {
            **daily_payload,
            'share_type': 'daily',
            'user_settings': settings,
            'shared': True,
        }
        return _json_with_cache(result, request, public=True)

    if share_type == 'range':
        start_d = share.get('target_start_date')
        end_d = share.get('target_end_date') or start_d
        _, linked_by_date = _get_user_walk_windows_and_summaries(owner_sub, data_root)
        range_payload = get_or_compute_daily_range_analytics(
            user_sub=owner_sub,
            start_date=start_d,
            end_date=end_d,
            linked_walks_by_date=linked_by_date,
        )
        result = {
            **range_payload,
            'share_type': 'range',
            'user_settings': settings,
            'shared': True,
        }
        return _json_with_cache(result, request, public=True)

    walk_id = share['walk_id']
    date, walk_name, payload, metrics = _get_walk_analysis_data(walk_id, persist_analytics=True, data_root=data_root)
    walk_dir = _find_walk_dir(walk_id, data_root)
    files = sorted(f.name for f in walk_dir.iterdir() if f.is_file() and f.name != 'meta.json') if walk_dir else []
    meta = _load_walk_meta(walk_dir) if walk_dir else {}
    result = {
        'share_type': 'walk',
        'walk_id': walk_id,
        'date': date,
        'name': walk_name,
        'rating': meta.get('rating', 3) if walk_dir else 3,
        'payload': payload,
        'metrics': metrics,
        'files': files,
        'user_settings': settings,
        'shared': True,
    }
    return _json_with_cache(result, request, public=True)


def _walk_summary_row(r: dict) -> dict:
    m = r.get('metrics') or {}
    return {
        'walk_id': r['walk_id'],
        'date': r['date'],
        'name': r['name'],
        'start_time': r['start_time'],
        'distance_km': r['distance_km'],
        'avg_hr': r['avg_hr'],
        'bg_delta': r['bg_delta'],
        'tir_pct': r['tir_pct'],
        'temp_avg_c': m.get('temp_avg_c'),
        'wind_avg_kph': m.get('wind_avg_kph'),
        'weather_stress_band': m.get('weather_stress_band'),
    }


@app.get('/api/analytics/routes')
def list_route_groups(user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    own_ids = {walk_dir.name for walk_dir in _iter_walk_dirs(data_root)}
    rows = [r for r in list_all_analytics_rows() if r.get('route_fingerprint') and r.get('walk_id') in own_ids]
    groups = _build_route_groups(rows)
    result = []
    for group_walks in groups:
        # group_id = oldest walk's walk_id; stable as long as that walk exists
        group_id = group_walks[0]['walk_id']
        fp = group_walks[0]['route_fingerprint']
        dists = [r['route_fingerprint']['distance_km'] for r in group_walks if r['route_fingerprint'].get('distance_km')]
        avg_dist = sum(dists) / len(dists) if dists else None
        result.append({
            'group_id': group_id,
            'walk_count': len(group_walks),
            'avg_distance_km': round(avg_dist, 2) if avg_dist else None,
            'simplified_track': fp.get('simplified_track'),
            'walks': [_walk_summary_row(r) for r in group_walks],
        })
    result.sort(key=lambda g: (-g['walk_count'], g.get('avg_distance_km') or 0))
    return result


@app.get('/api/analytics/routes/{group_id}/compare')
def route_compare(group_id: str, user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    own_ids = {walk_dir.name for walk_dir in _iter_walk_dirs(data_root)}
    rows = [r for r in list_all_analytics_rows() if r.get('route_fingerprint') and r.get('walk_id') in own_ids]
    groups = _build_route_groups(rows)
    group_walks = next((g for g in groups if g[0]['walk_id'] == group_id), None)
    if group_walks is None:
        raise HTTPException(status_code=404, detail='Route group not found')
    return {
        'group_id': group_id,
        'walks': [
            {
                **_walk_summary_row(r),
                'metrics': {
                    'distance_km': r['distance_km'],
                    'avg_hr': r['avg_hr'],
                    'bg_delta': r['bg_delta'],
                    'tir_pct': r['tir_pct'],
                    'temp_avg_c': (r.get('metrics') or {}).get('temp_avg_c'),
                    'wind_avg_kph': (r.get('metrics') or {}).get('wind_avg_kph'),
                    'weather_stress_band': (r.get('metrics') or {}).get('weather_stress_band'),
                },
                'route_series': r.get('route_series'),
                'track': (r.get('route_fingerprint') or {}).get('simplified_track'),
            }
            for r in group_walks
        ],
    }


@app.get('/api/user/settings')
@app.get('/api/settings')
def get_settings(user: dict = Depends(_require_user)):
    settings = get_user_settings(user['sub'])
    if settings is None:
        default_settings = {
            "user_type": "non_diabetic",
            "insulin_type": "None",
            "pump_brand_model": None,
            "basal_rates": None,
            "carb_ratios": None,
            "insulin_sensitivity_factor": None,
            "correction_threshold": None,
            "exercise_correction_factor": None,
            "iob_calculation_method": None,
            "insulin_profile": None,
        }
        upsert_user_settings(user['sub'], default_settings)
        settings = get_user_settings(user['sub'])
    return settings


@app.post('/api/user/settings')
@app.post('/api/settings')
def update_settings(settings: dict, user: dict = Depends(_require_user)):
    try:
        upsert_user_settings(user['sub'], settings)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"status": "ok"}


@app.delete('/api/user/account')
def delete_account(user: dict = Depends(_require_user)):
    data_root = _user_data_dir(user['sub'])
    walk_ids = []
    if data_root.exists() and data_root.is_dir():
        walk_ids = [walk_dir.name for walk_dir in _iter_walk_dirs(data_root)]
    # Clear DB records
    delete_user_data(user['sub'], walk_ids)
    # Clear directory from disk
    if data_root.exists() and data_root.is_dir():
        shutil.rmtree(data_root, ignore_errors=True)
    return {"status": "ok"}


@app.post("/api/walks/{walk_id}/rename")
def rename_walk(
    walk_id: str,
    payload: dict,
    user: dict = Depends(_require_user),
):
    new_name = payload.get("name", "").strip()
    data_root = _user_data_dir(user['sub'])
    walk_dir = _find_walk_dir(walk_id, data_root)
    if walk_dir is None or not walk_dir.exists():
        raise HTTPException(status_code=404, detail="Walk not found")

    # 1. Update meta.json
    meta_file = walk_dir / "meta.json"
    meta = {}
    if meta_file.exists():
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    meta["name"] = new_name
    meta_file.write_text(json.dumps(meta), encoding="utf-8")

    # 2. Update the walk_analytics table (if cached)
    from analytics.db import analytics_conn
    with analytics_conn() as conn:
        conn.execute(
            "UPDATE walk_analytics SET walk_name = ? WHERE walk_id = ?",
            (new_name, walk_id)
        )

    return {"status": "ok", "name": new_name}


@app.post("/api/walks/{walk_id}/rate")
def rate_walk(
    walk_id: str,
    payload: dict,
    user: dict = Depends(_require_user),
):
    rating = payload.get("rating")
    if rating is None or not isinstance(rating, int) or not (1 <= rating <= 5):
        raise HTTPException(status_code=400, detail="Rating must be an integer between 1 and 5")

    data_root = _user_data_dir(user['sub'])
    walk_dir = _find_walk_dir(walk_id, data_root)
    if walk_dir is None or not walk_dir.exists():
        raise HTTPException(status_code=404, detail="Walk not found")

    # 1. Update meta.json
    meta_file = walk_dir / "meta.json"
    meta = {}
    if meta_file.exists():
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    meta["rating"] = rating
    meta_file.write_text(json.dumps(meta), encoding="utf-8")

    # 2. Update the walk_analytics table (if cached)
    from analytics.db import analytics_conn
    with analytics_conn() as conn:
        conn.execute(
            "UPDATE walk_analytics SET rating = ? WHERE walk_id = ?",
            (rating, walk_id)
        )

    return {"status": "ok", "rating": rating}


import queue
import threading

class GarminAuthState:
    def __init__(self):
        self.mfa_code_queue = queue.Queue(maxsize=1)
        self.mfa_requested = threading.Event()
        self.mfa_received = threading.Event()
        self.auth_completed = threading.Event()
        self.error = None

active_auth_attempts: dict[str, GarminAuthState] = {}


@app.post("/api/garmin/login")
def garmin_login(
    payload: dict,
    user: dict = Depends(_require_user),
):
    email = payload.get("email", "").strip()
    password = payload.get("password", "")
    mfa_code = payload.get("mfa_code", "").strip()

    user_sub = user['sub']
    data_root = _user_data_dir(user_sub)
    token_store = str(data_root / ".garminconnect")
    os.makedirs(token_store, exist_ok=True)

    if mfa_code:
        logger.info(f"Received MFA code submission for user sub: {user_sub}")
        state = active_auth_attempts.get(user_sub)
        if not state:
            logger.warning(f"MFA submission failed: No active Garmin login session found for user sub: {user_sub}")
            raise HTTPException(status_code=400, detail="No active login session. Please restart connect.")
        try:
            state.mfa_code_queue.put(mfa_code, timeout=5)
            state.mfa_received.set()
        except queue.Full:
            logger.warning(f"MFA submission failed: queue full for user sub: {user_sub}")
            raise HTTPException(status_code=400, detail="MFA code already submitted or queue full.")

        completed = state.auth_completed.wait(timeout=15)
        if not completed:
            active_auth_attempts.pop(user_sub, None)
            logger.error(f"Garmin MFA login timed out completing MFA authentication for user sub: {user_sub}")
            raise HTTPException(status_code=504, detail="Garmin login timed out completing MFA authentication.")

        if state.error:
            err = state.error
            active_auth_attempts.pop(user_sub, None)
            logger.error(f"Garmin MFA login worker finished with error for user sub: {user_sub}. Error: {err}")
            raise HTTPException(status_code=400, detail=f"Login failed: {err}")

        # Update garmin_email on success
        settings = get_user_settings(user_sub) or {}
        settings["garmin_email"] = email
        upsert_user_settings(user_sub, settings)

        active_auth_attempts.pop(user_sub, None)
        logger.info(f"Garmin MFA authentication succeeded for user sub: {user_sub}")
        return {"status": "success", "message": "Successfully authenticated with Garmin!"}

    # Initial Auth Trigger
    # Set up auth state
    logger.info(f"Initiating Garmin login connection trigger for user sub: {user_sub}, email: {email}")
    state = GarminAuthState()
    active_auth_attempts[user_sub] = state

    def garmin_login_worker():
        try:
            from garminconnect import Garmin

            def mfa_callback():
                state.mfa_requested.set()
                logger.info(f"Garmin login triggered MFA request for user sub: {user_sub}")
                try:
                    code = state.mfa_code_queue.get(timeout=120)  # Wait up to 2 mins
                    logger.info(f"MFA code received from queue for user sub: {user_sub}, completing login")
                    return code
                except queue.Empty:
                    logger.warning(f"Garmin MFA code input timed out for user sub: {user_sub}")
                    raise Exception("MFA validation timed out.")

            client = Garmin(
                email=email,
                password=password,
                prompt_mfa=mfa_callback,
            )
            client.login(tokenstore=token_store)
            state.auth_completed.set()
        except Exception as e:
            state.error = str(e)
            logger.error(f"Garmin login worker failed for user sub: {user_sub}. Error: {e}", exc_info=True)
            state.auth_completed.set()

    thread = threading.Thread(target=garmin_login_worker)
    thread.start()

    # Poll until either MFA is explicitly requested, auth completes, or timeout
    timeout_s = 20.0
    start_t = time.time()
    while time.time() - start_t < timeout_s:
        if state.mfa_requested.is_set():
            logger.info(f"Garmin login requires MFA challenge for user sub: {user_sub}")
            return {"status": "mfa_required"}
        if state.auth_completed.is_set():
            break
        time.sleep(0.1)

    if state.auth_completed.is_set():
        if state.error:
            err = state.error
            active_auth_attempts.pop(user_sub, None)
            logger.error(f"Garmin login failed on initial connection for user sub: {user_sub}. Error: {err}")
            clean_err = err
            if any(term in err.lower() for term in ["401", "authentication", "credentials", "invalid", "password", "login failed"]):
                clean_err = "Invalid Garmin email or password. Please verify your credentials and try again."
            raise HTTPException(status_code=400, detail=clean_err)

        # Immediate success (no MFA needed or cached session reused)
        settings = get_user_settings(user_sub) or {}
        settings["garmin_email"] = email
        upsert_user_settings(user_sub, settings)
        active_auth_attempts.pop(user_sub, None)
        logger.info(f"Garmin connection established successfully (no MFA required) for user sub: {user_sub}")
        return {"status": "success"}

    if state.mfa_requested.is_set():
        logger.info(f"Garmin login requires MFA challenge for user sub: {user_sub}")
        return {"status": "mfa_required"}

    active_auth_attempts.pop(user_sub, None)
    logger.error(f"Garmin login timed out connecting for user sub: {user_sub}")
    raise HTTPException(status_code=504, detail="Garmin login timed out. Please check your connection and credentials and try again.")


def _import_single_garmin_activity(
    client,
    activity_id: str | int,
    act_data: dict | None,
    data_root: Path,
    user_sub: str,
    override_name: str | None = None,
    override_date: str | None = None,
    carelink_bytes: bytes | None = None,
    carelink_filename: str | None = None,
    gpx_bytes: bytes | None = None,
    gpx_filename: str | None = None,
) -> dict:
    activity_id_str = str(activity_id)
    if not act_data:
        try:
            act_data = client.get_activity(activity_id)
        except Exception:
            act_data = {}

    start_time_str = act_data.get("startTimeLocal") or ""
    act_date = override_date.strip() if override_date and override_date.strip() else (start_time_str.split(" ")[0] if start_time_str else datetime.now(timezone.utc).strftime("%Y-%m-%d"))

    # Download FIT data
    fit_data = client.download_activity(activity_id, dl_fmt=client.ActivityDownloadFormat.FIT)
    fit_start_dt, fit_end_dt = _fit_time_bounds(fit_data)
    fit_identity = _fit_file_identity(fit_data)

    date_dir = data_root / act_date
    date_dir.mkdir(exist_ok=True)
    act_name = override_name.strip() if override_name and override_name.strip() else act_data.get("activityName", "Walk").strip()
    walk_id = _make_walk_id(act_date, fit_start_dt, act_name, date_dir, user_sub=user_sub)
    walk_dir = date_dir / walk_id
    walk_dir.mkdir(exist_ok=False)

    logger.info(f"Importing Garmin activity {activity_id_str} as Walk ID {walk_id} in {walk_dir}")
    (walk_dir / f"activity_{activity_id_str}.fit").write_bytes(fit_data)

    reused_carelink_from = None
    carelink_start_time = None
    carelink_end_time = None

    if carelink_bytes and carelink_filename:
        safe_carelink_name = Path(carelink_filename).name
        if not safe_carelink_name.lower().endswith(".csv"):
            safe_carelink_name += ".csv"
        (walk_dir / safe_carelink_name).write_bytes(carelink_bytes)
    else:
        # Autolink Carelink file (if uploaded date covers this range)
        reused_carelink = _find_reusable_carelink_csv(fit_start_dt, fit_end_dt, data_root=data_root)
        if reused_carelink is not None:
            reused_csv_path, reused_meta = reused_carelink
            reused_carelink_from = reused_meta["id"]
            carelink_start_time = reused_meta.get("carelink_start_time")
            carelink_end_time = reused_meta.get("carelink_end_time")
            (walk_dir / reused_csv_path.name).write_bytes(reused_csv_path.read_bytes())

    if gpx_bytes and gpx_filename:
        safe_gpx_name = Path(gpx_filename).name
        if not safe_gpx_name.lower().endswith(".gpx"):
            safe_gpx_name += ".gpx"
        (walk_dir / safe_gpx_name).write_bytes(gpx_bytes)

    meta = {
        "id": walk_id,
        "date": act_date,
        "name": act_name,
        "start_time": fit_start_dt.isoformat() if fit_start_dt is not None else None,
        "fit_identity": fit_identity,
        "garmin_activity_id": activity_id_str,
        "rating": 3,
    }
    if reused_carelink_from is not None:
        meta["carelink_reused_from"] = reused_carelink_from
        meta["carelink_start_time"] = carelink_start_time
        meta["carelink_end_time"] = carelink_end_time

    (walk_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

    refresh_walk_analytics_if_needed(
        walk_id,
        force=True,
        find_walk_dir=lambda wid: _find_walk_dir(wid, data_root),
        get_walk_analysis_data=lambda wid, persist_analytics=False: _get_walk_analysis_data(
            wid,
            persist_analytics=persist_analytics,
            data_root=data_root,
        ),
    )
    return {"id": walk_id, "name": act_name, "date": act_date}


@app.get("/api/garmin/activities")
def get_garmin_activities(
    limit: int = 20,
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    settings = get_user_settings(user_sub)
    if not settings or not settings.get("garmin_email"):
        raise HTTPException(status_code=400, detail="Garmin Connect not configured.")

    garmin_email = settings["garmin_email"]
    data_root = _user_data_dir(user_sub)
    token_store = str(data_root / ".garminconnect")

    from garminconnect import Garmin
    try:
        client = Garmin(email=garmin_email, password="")
        client.login(tokenstore=token_store)
    except Exception as e:
        logger.error(f"Garmin session authentication failed: {e}", exc_info=True)
        raise HTTPException(status_code=401, detail=f"Garmin session invalid: {e}. Please reconnect in Settings.")

    try:
        activities = client.get_activities(0, min(max(1, limit), 50))
    except Exception as e:
        logger.error(f"Garmin API activities fetch failed: {e}", exc_info=True)
        raise HTTPException(status_code=502, detail=f"Garmin API call failed: {e}")

    imported_activity_ids = set()
    imported_start_times = set()
    for walk_dir in _iter_walk_dirs(data_root):
        meta = _load_walk_meta(walk_dir)
        if meta.get("garmin_activity_id"):
            imported_activity_ids.add(str(meta["garmin_activity_id"]))
        if meta.get("start_time"):
            imported_start_times.add(meta["start_time"])

    activity_list = []
    for act in activities:
        act_id = str(act.get("activityId"))
        act_type = act.get("activityType", {}).get("typeKey", "unknown")
        start_time_str = act.get("startTimeLocal") or ""
        start_time_gmt = act.get("startTimeGMT")
        act_date = start_time_str.split(" ")[0] if start_time_str else ""

        already_imported = (act_id in imported_activity_ids) or (bool(start_time_gmt) and start_time_gmt in imported_start_times)

        dist_m = act.get("distance")
        dist_km = round(dist_m / 1000.0, 2) if dist_m is not None else None
        dur_s = act.get("duration")
        dur_min = round(dur_s / 60.0, 1) if dur_s is not None else None

        activity_list.append({
            "activity_id": act_id,
            "name": act.get("activityName") or "Walk",
            "activity_type": act_type,
            "start_time_local": start_time_str,
            "start_time_gmt": start_time_gmt,
            "date": act_date,
            "distance_km": dist_km,
            "duration_min": dur_min,
            "avg_hr": act.get("averageHR"),
            "max_hr": act.get("maxHR"),
            "already_imported": already_imported,
        })

    return {"activities": activity_list, "garmin_email": garmin_email}


@app.post("/api/garmin/activities/import")
async def import_selected_garmin_activity(
    activity_id: str = Form(...),
    name: Optional[str] = Form(None),
    date: Optional[str] = Form(None),
    carelink_file: Optional[UploadFile] = File(None),
    gpx_file: Optional[UploadFile] = File(None),
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    settings = get_user_settings(user_sub)
    if not settings or not settings.get("garmin_email"):
        raise HTTPException(status_code=400, detail="Garmin Connect not configured.")

    garmin_email = settings["garmin_email"]
    data_root = _user_data_dir(user_sub)
    token_store = str(data_root / ".garminconnect")

    from garminconnect import Garmin
    try:
        client = Garmin(email=garmin_email, password="")
        client.login(tokenstore=token_store)
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Garmin session invalid: {e}. Please reconnect.")

    carelink_bytes = None
    carelink_filename = None
    if carelink_file:
        carelink_bytes = await carelink_file.read()
        carelink_filename = carelink_file.filename

    gpx_bytes = None
    gpx_filename = None
    if gpx_file:
        gpx_bytes = await gpx_file.read()
        gpx_filename = gpx_file.filename

    try:
        imported_walk = _import_single_garmin_activity(
            client=client,
            activity_id=activity_id,
            act_data={"activityId": activity_id, "activityName": name or "Walk", "startTimeLocal": f"{date} 00:00:00" if date else None},
            data_root=data_root,
            user_sub=user_sub,
            override_name=name,
            override_date=date,
            carelink_bytes=carelink_bytes,
            carelink_filename=carelink_filename,
            gpx_bytes=gpx_bytes,
            gpx_filename=gpx_filename,
        )
    except Exception as e:
        logger.error(f"Failed to import Garmin activity {activity_id}: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Failed to import Garmin activity: {e}")

    return {"status": "ok", "walk": imported_walk}


@app.post("/api/garmin/sync")
def sync_garmin_walks(
    payload: dict | None = Body(default=None),
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    req = payload or {}
    start_date_req = req.get("start_date")
    end_date_req = req.get("end_date")
    include_epochs = bool(req.get("include_epochs", True))
    include_walks = bool(req.get("include_walks", True))

    try:
        sync_dates = resolve_sync_date_range(
            start_date=start_date_req,
            end_date=end_date_req,
            default_days=7,
            max_days=30,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    logger.info(
        f"Starting Garmin sync for user sub: {user_sub} "
        f"({sync_dates[0]} to {sync_dates[-1]}, walks={include_walks}, epochs={include_epochs})"
    )
    settings = get_user_settings(user_sub)
    if not settings or not settings.get("garmin_email"):
        logger.warning(f"Garmin sync aborted: no Garmin email connected for user sub: {user_sub}")
        raise HTTPException(status_code=400, detail="Garmin Connect not configured.")

    garmin_email = settings["garmin_email"]
    data_root = _user_data_dir(user_sub)
    token_store = str(data_root / ".garminconnect")

    from garminconnect import Garmin
    try:
        logger.info(f"Logging into Garmin using tokenstore at {token_store} for user sub: {user_sub}")
        client = Garmin(email=garmin_email, password="")
        client.login(tokenstore=token_store)
    except Exception as e:
        logger.error(f"Garmin session authentication failed for user sub: {user_sub}. Error: {e}", exc_info=True)
        raise HTTPException(status_code=401, detail=f"Garmin session invalid: {e}. Please reconnect.")

    imported_walks = []
    if include_walks and hasattr(client, "get_activities"):
        try:
            logger.info(f"Fetching recent Garmin activities list for user sub: {user_sub}")
            activities = client.get_activities(0, 10)
            logger.info(f"Successfully fetched {len(activities)} activities from Garmin API")
        except Exception as e:
            logger.error(f"Garmin API activities fetch failed for user sub: {user_sub}. Error: {e}", exc_info=True)
            raise HTTPException(status_code=502, detail=f"Garmin API call failed: {e}")

        for act in activities:
            act_type = act.get("activityType", {}).get("typeKey")
            activity_id = act["activityId"]
            start_time_str = act.get("startTimeLocal")

            logger.debug(f"Evaluating Garmin activity {activity_id} ({act_type}) start time: {start_time_str}")
            if act_type not in ("walking", "hiking"):
                continue

            if not start_time_str:
                continue
            act_date = start_time_str.split(" ")[0]
            if (start_date_req or end_date_req) and act_date not in sync_dates:
                continue

            # Duplicate checking
            already_imported = False
            for walk_dir in _iter_walk_dirs(data_root):
                meta = _load_walk_meta(walk_dir)
                if meta.get("garmin_activity_id") == str(activity_id) or meta.get("start_time") == act.get("startTimeGMT"):
                    already_imported = True
                    break

            if already_imported:
                logger.debug(f"Garmin activity {activity_id} already imported. Skipping.")
                continue

            try:
                walk_info = _import_single_garmin_activity(
                    client=client,
                    activity_id=activity_id,
                    act_data=act,
                    data_root=data_root,
                    user_sub=user_sub,
                )
                imported_walks.append(walk_info)
            except Exception as e:
                logger.error(f"Failed to import activity {activity_id}: {e}", exc_info=True)
                continue

    epochs_upserted = 0
    if include_epochs:
        walk_windows, linked_by_date = _get_user_walk_windows_and_summaries(user_sub, data_root)
        epochs_rows, summary_rows = fetch_garmin_epochs_for_dates(
            client=client,
            dates=sync_dates,
            walk_windows=walk_windows,
        )
        if epochs_rows:
            epochs_upserted = upsert_activity_epochs(user_sub, epochs_rows)
        for s_row in summary_rows:
            upsert_daily_wellness_summary(user_sub, s_row["local_date"], s_row)

        touched_dates = sorted({r["local_date"] for r in epochs_rows} | {r["local_date"] for r in summary_rows})
        for d_str in touched_dates:
            try:
                get_or_compute_daily_analytics(
                    user_sub=user_sub,
                    local_date=d_str,
                    force=True,
                    linked_walks=linked_by_date.get(d_str, []),
                )
            except Exception as exc:
                logger.warning(f"Failed to recompute daily analytics for {d_str}: {exc}")

    settings["last_garmin_sync_at"] = datetime.now(timezone.utc).isoformat()
    upsert_user_settings(user_sub, settings)

    return {
        "status": "ok",
        "start_date": sync_dates[0],
        "end_date": sync_dates[-1],
        "days_synced": len(sync_dates),
        "epochs_upserted": epochs_upserted,
        "imported": imported_walks,
        "walks_imported": imported_walks,
    }



@app.post("/api/carelink/upload-token")
async def carelink_upload_token(
    file: UploadFile = File(...),
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    logger.info(f"Received CareLink login tokens upload request for user sub: {user_sub}")
    data_root = _user_data_dir(user_sub)
    carelink_dir = data_root / ".carelink"
    carelink_dir.mkdir(exist_ok=True)
    
    try:
        content = await file.read()
        token_data = json.loads(content.decode("utf-8"))
    except Exception as e:
        logger.error(f"CareLink token upload failed: Invalid JSON format for user sub: {user_sub}. Error: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Invalid JSON file: {e}")
        
    required_fields = ["access_token", "refresh_token", "scope", "client_id"]
    for f in required_fields:
        if f not in token_data:
            logger.warning(f"CareLink token upload failed: Missing required field '{f}' for user sub: {user_sub}")
            raise HTTPException(status_code=400, detail=f"Missing required field in token file: {f}")
            
    token_path = carelink_dir / "logindata.json"
    token_path.write_bytes(content)
    logger.info(f"Wrote CareLink tokens to {token_path}. Running connection test...")
    
    from carelink_client2 import CareLinkClient
    try:
        client = CareLinkClient(tokenFile=str(token_path))
        success = client.init()
        if not success:
            raise Exception("CareLinkClient initialization failed.")
        logger.info(f"CareLink connection test completed successfully for user sub: {user_sub}")
    except Exception as e:
        logger.error(f"CareLink connection test failed for user sub: {user_sub}. Error: {e}", exc_info=True)
        if token_path.exists():
            token_path.unlink()
        raise HTTPException(status_code=400, detail=f"CareLink connection test failed: {e}")
        
    payload = client._get_access_token_payload(token_data)
    username = None
    if payload and "token_details" in payload:
        username = payload["token_details"].get("preferred_username")
    if not username:
        username = "Connected CareLink Account"
        
    settings = get_user_settings(user_sub) or {}
    settings["carelink_email"] = username
    upsert_user_settings(user_sub, settings)
    
    logger.info(f"Successfully configured CareLink integration for username: {username}, user sub: {user_sub}")
    return {"status": "success", "username": username}


def _sync_glycemic_events_to_overlapping_walks(
    user_sub: str,
    data_root: Path,
    events: list[dict],
    csv_filename: str = "carelink_sync.csv",
) -> list[dict]:
    """Write overlapping normalized glycemic_events into walk directories so individual walk charts update."""
    if not events or not data_root.exists():
        return []

    parsed_events: list[tuple[datetime, dict]] = []
    for ev in events:
        try:
            dt = datetime.fromisoformat(str(ev["timestamp_utc"]).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            parsed_events.append((dt, ev))
        except Exception:
            continue

    if not parsed_events:
        return []

    walks_updated: list[dict] = []
    headers = [
        "Index",
        "Date",
        "Time",
        "Sensor Glucose (mmol/L)",
        "Basal Rate (U/h)",
        "Bolus Volume Delivered (U)",
        "Suspend",
        "Auto Mode",
    ]

    for walk_dir in _iter_walk_dirs(data_root):
        meta = _load_walk_meta(walk_dir)
        start_time_iso = meta.get("start_time")
        if not start_time_iso:
            continue
        try:
            walk_start = datetime.fromisoformat(str(start_time_iso).replace("Z", "+00:00"))
            if walk_start.tzinfo is None:
                walk_start = walk_start.replace(tzinfo=timezone.utc)
        except ValueError:
            continue

        fit_files = list(walk_dir.glob("*.fit"))
        if not fit_files:
            walk_end = walk_start + timedelta(hours=1)
        else:
            try:
                fit_start, fit_end = _fit_time_bounds(fit_files[0].read_bytes())
            except Exception:
                fit_start, fit_end = None, None
            walk_start = fit_start if fit_start is not None else walk_start
            walk_end = fit_end if fit_end is not None else (walk_start + timedelta(hours=1))

        pad_start = walk_start - timedelta(minutes=30)
        pad_end = walk_end + timedelta(minutes=60)

        window_events = [(dt, ev) for dt, ev in parsed_events if pad_start <= dt <= pad_end]
        if not window_events:
            continue

        window_events.sort(key=lambda pair: pair[0])
        csv_path = walk_dir / csv_filename
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for idx, (dt, ev) in enumerate(window_events, start=1):
                ev_type = ev.get("event_type")
                writer.writerow([
                    idx,
                    dt.strftime("%Y/%m/%d"),
                    dt.strftime("%H:%M:%S"),
                    ev.get("bg_mmol") if ev_type == "cgm" and ev.get("bg_mmol") is not None else "",
                    ev.get("basal_rate_u_hr") if ev_type == "basal" and ev.get("basal_rate_u_hr") is not None else "",
                    ev.get("bolus_units") if ev_type == "bolus" and ev.get("bolus_units") is not None else "",
                    ev.get("pump_state") if ev_type == "pump_state" and ev.get("pump_state") else "",
                    "",
                ])

        meta["carelink_reused_from"] = None
        meta["carelink_start_time"] = walk_start.isoformat()
        meta["carelink_end_time"] = walk_end.isoformat()
        (walk_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")

        refresh_walk_analytics_if_needed(
            meta["id"],
            force=True,
            find_walk_dir=lambda wid: _find_walk_dir(wid, data_root),
            get_walk_analysis_data=lambda wid, persist_analytics=False: _get_walk_analysis_data(
                wid,
                persist_analytics=persist_analytics,
                data_root=data_root,
            ),
        )
        walks_updated.append({"id": meta["id"], "name": meta.get("name"), "date": meta.get("date")})

    return walks_updated


@app.post("/api/carelink/sync")
def sync_carelink_data(
    payload: dict | None = Body(default=None),
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    req = payload or {}
    allowed_dates: set[str] | None = None
    if req.get("start_date") or req.get("end_date"):
        try:
            date_list = resolve_sync_date_range(
                start_date=req.get("start_date"),
                end_date=req.get("end_date"),
                default_days=7,
                max_days=30,
            )
            allowed_dates = set(date_list)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc))

    logger.info(f"Starting CareLink automated sync for user sub: {user_sub}")
    settings = get_user_settings(user_sub)
    if not settings or not settings.get("carelink_email"):
        logger.warning(f"CareLink sync aborted: CareLink not connected for user sub: {user_sub}")
        raise HTTPException(status_code=400, detail="CareLink not configured.")
        
    data_root = _user_data_dir(user_sub)
    token_path = data_root / ".carelink" / "logindata.json"
    if not token_path.exists():
        logger.error(f"CareLink sync failed: token file not found at {token_path} for user sub: {user_sub}")
        raise HTTPException(status_code=400, detail="CareLink token file not found. Please reconnect.")
        
    from carelink_client2 import CareLinkClient
    try:
        logger.info(f"Connecting to CareLink using tokens file for user sub: {user_sub}")
        client = CareLinkClient(tokenFile=str(token_path))
        if not client.init():
            raise Exception("Failed to initialize CareLink client.")
        logger.info(f"Fetching recent data from CareLink API for user sub: {user_sub}")
        recent_data = client.getRecentData()
        if not recent_data:
            raise Exception("No data returned from CareLink.")
    except Exception as e:
        logger.error(f"CareLink connection or data fetch failed for user sub: {user_sub}. Error: {e}", exc_info=True)
        raise HTTPException(status_code=401, detail=f"CareLink sync failed: {e}. Please re-authenticate.")
        
    if "patientData" in recent_data:
        recent_data = recent_data["patientData"]
        
    sgs = recent_data.get("sgs", [])
    markers = recent_data.get("markers", [])
    logger.info(f"Retrieved {len(sgs)} glucose readings and {len(markers)} markers from CareLink API")

    # Persist 24-hour glycemic_events for daily & range correlation views
    normalized_events = normalize_carelink_api_payload(
        recent_data,
        allowed_dates=allowed_dates,
    )
    events_upserted = 0
    if normalized_events:
        events_upserted = upsert_glycemic_events(user_sub, normalized_events)
        _, linked_by_date = _get_user_walk_windows_and_summaries(user_sub, data_root)
        for d_str in sorted({ev["local_date"] for ev in normalized_events}):
            try:
                get_or_compute_daily_analytics(
                    user_sub=user_sub,
                    local_date=d_str,
                    force=True,
                    linked_walks=linked_by_date.get(d_str, []),
                )
            except Exception as exc:
                logger.warning(f"Failed to recompute daily analytics for {d_str}: {exc}")

    walks_updated = []
    
    for walk_dir in _iter_walk_dirs(data_root):
        meta = _load_walk_meta(walk_dir)
        start_time_iso = meta.get("start_time")
        if not start_time_iso:
            continue
        try:
            walk_start = datetime.fromisoformat(start_time_iso)
        except ValueError:
            continue
            
        fit_files = list(walk_dir.glob("activity_*.fit"))
        if not fit_files:
            walk_end = walk_start + timedelta(hours=1)
        else:
            fit_path = fit_files[0]
            fit_start, fit_end = _fit_time_bounds(fit_path.read_bytes())
            walk_start = fit_start if fit_start is not None else walk_start
            walk_end = fit_end if fit_end is not None else (walk_start + timedelta(hours=1))
            
        pad_start = walk_start - timedelta(minutes=30)
        pad_end = walk_end + timedelta(minutes=60)
        
        walk_sgs = []
        for sg in sgs:
            ts_str = sg.get("timestamp")
            if not ts_str:
                continue
            try:
                sg_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except ValueError:
                continue
            if pad_start <= sg_ts <= pad_end:
                walk_sgs.append((sg_ts, sg))
                
        if not walk_sgs:
            continue
            
        walk_markers = []
        for marker in markers:
            ts_str = marker.get("timestamp")
            if not ts_str:
                continue
            try:
                m_ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            except ValueError:
                continue
            if pad_start <= m_ts <= pad_end:
                walk_markers.append((m_ts, marker))
                
        csv_rows = []
        
        for idx, (sg_ts, sg) in enumerate(walk_sgs):
            val_mgdl = sg.get("sg")
            if val_mgdl is None or val_mgdl <= 0:
                continue
            val_mmol = round(val_mgdl * 0.0555, 2)
            date_str = sg_ts.strftime("%Y/%m/%d")
            time_str = sg_ts.strftime("%H:%M:%S")
            csv_rows.append({
                "Index": idx + 1,
                "Date": date_str,
                "Time": time_str,
                "Sensor Glucose (mmol/L)": val_mmol,
                "Basal Rate (U/h)": "",
                "Bolus Volume Delivered (U)": "",
                "Suspend": "",
                "Auto Mode": ""
            })
            
        for (m_ts, marker) in walk_markers:
            if marker.get("type") == "INSULIN":
                amount = marker.get("amount")
                if amount is not None and amount > 0:
                    date_str = m_ts.strftime("%Y/%m/%d")
                    time_str = m_ts.strftime("%H:%M:%S")
                    csv_rows.append({
                        "Index": len(csv_rows) + 1,
                        "Date": date_str,
                        "Time": time_str,
                        "Sensor Glucose (mmol/L)": "",
                        "Basal Rate (U/h)": "",
                        "Bolus Volume Delivered (U)": amount,
                        "Suspend": "",
                        "Auto Mode": ""
                    })
                    
        def parse_row_ts(r):
            return datetime.strptime(f"{r['Date']} {r['Time']}", "%Y/%m/%d %H:%M:%S").replace(tzinfo=timezone.utc)
            
        csv_rows.sort(key=parse_row_ts)
        
        csv_path = walk_dir / "carelink_sync.csv"
        headers = ["Index", "Date", "Time", "Sensor Glucose (mmol/L)", "Basal Rate (U/h)", "Bolus Volume Delivered (U)", "Suspend", "Auto Mode"]
        
        logger.info(f"Writing {len(csv_rows)} synchronized CareLink records to {csv_path} for Walk ID: {meta['id']}")
        with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for idx, r in enumerate(csv_rows):
                writer.writerow([
                    idx + 1,
                    r["Date"],
                    r["Time"],
                    r["Sensor Glucose (mmol/L)"],
                    r["Basal Rate (U/h)"],
                    r["Bolus Volume Delivered (U)"],
                    r["Suspend"],
                    r["Auto Mode"]
                ])
                
        meta["carelink_reused_from"] = None
        meta["carelink_start_time"] = walk_start.isoformat()
        meta["carelink_end_time"] = walk_end.isoformat()
        (walk_dir / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
        
        logger.info(f"Refreshing analytics overlay for synced Walk ID: {meta['id']}")
        refresh_walk_analytics_if_needed(
            meta["id"],
            force=True,
            find_walk_dir=lambda wid: _find_walk_dir(wid, data_root),
            get_walk_analysis_data=lambda wid, persist_analytics=False: _get_walk_analysis_data(
                wid,
                persist_analytics=persist_analytics,
                data_root=data_root,
            ),
        )
        walks_updated.append({"id": meta["id"], "name": meta.get("name"), "date": meta.get("date")})

    settings["last_cgm_sync_at"] = datetime.now(timezone.utc).isoformat()
    settings["cgm_provider"] = "carelink"
    upsert_user_settings(user_sub, settings)

    logger.info(f"CareLink automated sync completed successfully. Synced {len(walks_updated)} walk(s) and {events_upserted} event(s).")
    return {
        "status": "ok",
        "events_upserted": events_upserted,
        "synced_walks": walks_updated,
    }


@app.post("/api/dexcom/login")
def dexcom_login_endpoint(
    payload: dict = Body(...),
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    username = (payload.get("username") or "").strip()
    password = payload.get("password") or ""
    region = (payload.get("region") or "ous").strip().lower()
    if region not in ("us", "ous"):
        raise HTTPException(status_code=400, detail="region must be 'us' or 'ous'")
    if not username or not password:
        raise HTTPException(status_code=400, detail="Dexcom username and password are required.")

    data_root = _user_data_dir(user_sub)
    token_path = data_root / ".dexcom" / "session.json"

    try:
        client = DexcomShareClient(
            username=username,
            password=password,
            region=region,
            token_path=token_path,
        )
        session_id = client.authenticate()
    except Exception as exc:
        logger.error(f"Dexcom login failed for user {user_sub}: {exc}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Dexcom authentication failed: {exc}")

    settings = get_user_settings(user_sub) or {}
    settings["dexcom_username"] = username
    settings["dexcom_region"] = region
    settings["cgm_provider"] = "dexcom"
    upsert_user_settings(user_sub, settings)

    return {
        "status": "success",
        "username": username,
        "region": region,
        "session_id": session_id,
    }


@app.post("/api/dexcom/sync")
def dexcom_sync_endpoint(
    payload: dict | None = Body(default=None),
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    req = payload or {}
    try:
        sync_dates = resolve_sync_date_range(
            start_date=req.get("start_date"),
            end_date=req.get("end_date"),
            default_days=7,
            max_days=30,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    settings = get_user_settings(user_sub) or {}
    username = settings.get("dexcom_username")
    region = settings.get("dexcom_region") or "ous"
    if not username:
        raise HTTPException(status_code=400, detail="Dexcom Share not configured. Please connect in Settings.")

    data_root = _user_data_dir(user_sub)
    token_path = data_root / ".dexcom" / "session.json"
    if not token_path.exists():
        raise HTTPException(status_code=400, detail="Dexcom session file not found. Please reconnect.")

    try:
        client = DexcomShareClient(
            username=username,
            region=region,
            token_path=token_path,
        )
        minutes = len(sync_dates) * 1440
        max_count = len(sync_dates) * 288
        events = client.fetch_glucose_readings(
            minutes=minutes,
            max_count=max_count,
            allowed_dates=set(sync_dates) if (req.get("start_date") or req.get("end_date")) else None,
        )
    except Exception as exc:
        logger.error(f"Dexcom sync failed for user {user_sub}: {exc}", exc_info=True)
        raise HTTPException(status_code=401, detail=f"Dexcom sync failed: {exc}")

    events_upserted = upsert_glycemic_events(user_sub, events) if events else 0
    walks_updated = _sync_glycemic_events_to_overlapping_walks(
        user_sub,
        data_root,
        events,
        csv_filename="dexcom_sync.csv",
    )

    _, linked_by_date = _get_user_walk_windows_and_summaries(user_sub, data_root)
    touched_dates = sorted({ev["local_date"] for ev in events})
    for d_str in touched_dates:
        try:
            get_or_compute_daily_analytics(
                user_sub=user_sub,
                local_date=d_str,
                force=True,
                linked_walks=linked_by_date.get(d_str, []),
            )
        except Exception as exc:
            logger.warning(f"Failed to recompute daily analytics for {d_str}: {exc}")

    settings["last_cgm_sync_at"] = datetime.now(timezone.utc).isoformat()
    upsert_user_settings(user_sub, settings)

    return {
        "status": "ok",
        "start_date": sync_dates[0],
        "end_date": sync_dates[-1],
        "events_upserted": events_upserted,
        "days_updated": len(touched_dates),
        "synced_walks": walks_updated,
    }


@app.post("/api/telemetry/import")
async def import_telemetry_csv_endpoint(
    file: UploadFile = File(...),
    provider: str = Form("auto"),
    tz_offset_mins: int = Form(0),
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    raw_bytes = await file.read()
    try:
        csv_text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        csv_text = raw_bytes.decode("latin-1", errors="replace")

    try:
        detected_provider, source_unit, events = detect_and_parse_telemetry_csv(
            csv_text=csv_text,
            provider=provider,
            tz_offset_mins=tz_offset_mins,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if not events:
        raise HTTPException(
            status_code=400,
            detail="No valid glucose, insulin, or carbohydrate records found in the uploaded CSV file.",
        )

    events_ingested = upsert_glycemic_events(user_sub, events)
    data_root = _user_data_dir(user_sub)
    walks_updated = _sync_glycemic_events_to_overlapping_walks(
        user_sub,
        data_root,
        events,
        csv_filename=f"{detected_provider}_import.csv",
    )

    _, linked_by_date = _get_user_walk_windows_and_summaries(user_sub, data_root)
    touched_dates = sorted({ev["local_date"] for ev in events})
    for d_str in touched_dates:
        try:
            get_or_compute_daily_analytics(
                user_sub=user_sub,
                local_date=d_str,
                force=True,
                linked_walks=linked_by_date.get(d_str, []),
            )
        except Exception as exc:
            logger.warning(f"Failed to recompute daily analytics for {d_str}: {exc}")

    settings = get_user_settings(user_sub) or {}
    settings["last_cgm_sync_at"] = datetime.now(timezone.utc).isoformat()
    if not settings.get("cgm_provider") or settings.get("cgm_provider") == "none":
        settings["cgm_provider"] = detected_provider
    upsert_user_settings(user_sub, settings)

    return {
        "status": "ok",
        "detected_provider": detected_provider,
        "source_unit": source_unit,
        "events_ingested": events_ingested,
        "date_range": {
            "start_date": touched_dates[0] if touched_dates else None,
            "end_date": touched_dates[-1] if touched_dates else None,
        },
        "days_updated": len(touched_dates),
        "walks_updated": len(walks_updated),
    }


@app.get("/api/system/logs")
def get_system_logs(
    category: str | None = None,
    auth_only: bool = False,
    exclude_connection: bool = False,
    is_connection: bool | None = None,
    level: str | None = None,
    service: str | None = None,
    search: str | None = None,
    limit: int = 100,
    offset: int = 0,
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    logs, total = query_system_logs(
        category=category,
        auth_only=auth_only,
        exclude_connection=exclude_connection,
        is_connection=is_connection,
        level=level,
        service=service,
        search=search,
        user_sub=user_sub,
        limit=min(max(1, limit), 500),
        offset=max(0, offset),
    )
    summary = get_logs_summary(user_sub=user_sub)
    return {
        "logs": logs,
        "total": total,
        "summary": summary,
    }


@app.get("/api/system/logs/summary")
def get_system_logs_summary(
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    return get_logs_summary(user_sub=user_sub)


@app.post("/api/system/logs/clear")
def clear_system_logs_endpoint(
    payload: dict = Body(default={}),
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    before_ts = payload.get("before_timestamp")
    cleared = clear_system_logs(user_sub=user_sub, before_timestamp=before_ts)
    return {"status": "ok", "cleared_count": cleared}


@app.post("/api/system/logs/test")
def emit_test_system_log(
    payload: dict = Body(...),
    user: dict = Depends(_require_user),
):
    user_sub = user['sub']
    log_id = log_system_event(
        level=payload.get("level", "INFO"),
        message=payload.get("message", "Test system log event"),
        category=payload.get("category"),
        service=payload.get("service", "test"),
        details=payload.get("details"),
        user_sub=user_sub,
        is_auth_issue=payload.get("is_auth_issue"),
        is_connection_issue=payload.get("is_connection_issue"),
    )
    return {"status": "ok", "log_id": log_id}





