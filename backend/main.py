import json
import csv
import math
import re
import shutil
import struct
import urllib.parse
import urllib.request
from datetime import date as date_type, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional
import xml.etree.ElementTree as ET

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

from analytics import (
    ANALYTICS_VERSION,
    delete_walk_analytics,
    init_analytics_db,
    list_walk_analytics as list_cached_walk_analytics,
    persist_walk_analytics,
    refresh_walk_analytics_if_needed,
)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

FIT_EPOCH = datetime(1989, 12, 31, tzinfo=timezone.utc)
BG_CONTEXT_WINDOW = timedelta(minutes=30)
init_analytics_db()


def _parse_date(date_str: str) -> str:
    try:
        date_type.fromisoformat(date_str)
        return date_str
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format, expected YYYY-MM-DD")


def _slugify(text: str) -> str:
    slug = re.sub(r'[^a-z0-9]+', '-', text.lower()).strip('-')
    return slug or 'walk'


def _walk_date_for_dir(walk_dir: Path) -> str:
    return walk_dir.parent.name if walk_dir.parent != DATA_DIR else walk_dir.name


def _iter_walk_dirs() -> list[Path]:
    walk_dirs: list[Path] = []
    for date_dir in sorted(DATA_DIR.iterdir(), reverse=True):
        if not date_dir.is_dir():
            continue
        child_dirs = sorted((child for child in date_dir.iterdir() if child.is_dir()), reverse=True)
        has_files = any(child.is_file() for child in date_dir.iterdir())
        if has_files:
            walk_dirs.append(date_dir)
        walk_dirs.extend(child_dirs)
    return walk_dirs


def _find_walk_dir(walk_id: str) -> Path | None:
    for walk_dir in _iter_walk_dirs():
        if walk_dir.name == walk_id:
            return walk_dir
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
    date = _walk_date_for_dir(walk_dir)
    meta_file = walk_dir / "meta.json"
    meta: dict = {}
    if meta_file.exists():
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
        except Exception:
            meta = {}
    meta = _backfill_walk_meta(walk_dir, meta)
    files = sorted(f.name for f in walk_dir.iterdir() if f.is_file() and f.name != "meta.json")
    return {
        "id": walk_dir.name,
        "date": meta.get("date", date),
        "name": meta.get("name", ""),
        "start_time": meta.get("start_time"),
        "fit_identity": meta.get("fit_identity"),
        "carelink_start_time": meta.get("carelink_start_time"),
        "carelink_end_time": meta.get("carelink_end_time"),
        "files": files,
    }


def _make_walk_id(date: str, fit_start_dt: datetime | None, name: str, date_dir: Path) -> str:
    time_part = (fit_start_dt or datetime.now(timezone.utc)).strftime("%H%M%S")
    base = f"{date}-{time_part}-{_slugify(name)}"
    candidate = base
    suffix = 2
    while (date_dir / candidate).exists():
        candidate = f"{base}-{suffix}"
        suffix += 1
    return candidate


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

    bg_points.sort(key=lambda x: x['timestamp'])
    basal_points.sort(key=lambda x: x['timestamp'])
    bolus_events.sort(key=lambda x: x['timestamp'])

    # Keep unfiltered basal samples so we can carry the last known rate into chart start.
    basal_points_all = list(basal_points)
    return {'bg': bg_points, 'basal': basal_points, 'bolus': bolus_events}


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


def _find_reusable_carelink_csv(activity_start_dt: datetime | None, activity_end_dt: datetime | None) -> tuple[Path, dict] | None:
    if activity_start_dt is None or activity_end_dt is None:
        return None

    for walk_dir in _iter_walk_dirs():
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


@app.post("/api/walks/parse-fit-date")
async def parse_fit_date(file: UploadFile = File(...)):
    data = await file.read()
    date_str = _fit_start_date(data)
    if not date_str:
        raise HTTPException(status_code=422, detail="Could not extract date from FIT file")
    return {"date": date_str}


@app.get("/api/walks")
def list_walks():
    walks = []
    for walk_dir in _iter_walk_dirs():
        walks.append(_load_walk_meta(walk_dir))
    walks.sort(key=lambda walk: (walk.get("start_time") or "", walk["date"], walk["id"]), reverse=True)
    return walks


@app.post("/api/walks/upload")
async def upload_files(
    date: str = Form(...),
    name: Optional[str] = Form(""),
    files: List[UploadFile] = File(...),
):
    date = _parse_date(date)
    date_dir = DATA_DIR / date
    date_dir.mkdir(exist_ok=True)

    prepared_files: list[tuple[str, bytes]] = []
    fit_start_dt: datetime | None = None
    fit_end_dt: datetime | None = None
    fit_identity: str | None = None
    has_uploaded_carelink_csv = False
    for file in files:
        if not file.filename:
            continue
        data = await file.read()
        prepared_files.append((file.filename, data))
        if fit_start_dt is None and file.filename.lower().endswith('.fit'):
            fit_start_dt, fit_end_dt = _fit_time_bounds(data)
        if fit_identity is None and file.filename.lower().endswith('.fit'):
            fit_identity = _fit_file_identity(data)
        if file.filename.lower().endswith('.csv'):
            has_uploaded_carelink_csv = True

    if fit_identity is not None:
        for existing_walk_dir in _iter_walk_dirs():
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
        reused_carelink = _find_reusable_carelink_csv(fit_start_dt, fit_end_dt)
        if reused_carelink is None:
            raise HTTPException(
                status_code=400,
                detail="No Carelink CSV uploaded, and no existing loaded CSV covers this walk's activity window",
            )

    walk_name = (name or "").strip()
    walk_id = _make_walk_id(date, fit_start_dt, walk_name, date_dir)
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
def delete_walk(walk_id: str):
    walk_dir = _find_walk_dir(walk_id)
    if walk_dir is None or not walk_dir.exists():
        raise HTTPException(status_code=404, detail="Walk not found")
    shutil.rmtree(walk_dir)
    delete_walk_analytics(walk_id)
    parent_dir = walk_dir.parent
    if parent_dir != DATA_DIR and parent_dir.exists() and not any(parent_dir.iterdir()):
        parent_dir.rmdir()
    return {"deleted": walk_id}


def _get_walk_analysis_data(walk_id: str, persist_analytics: bool = False) -> tuple[str, str | None, dict, dict]:
    walk_dir = _find_walk_dir(walk_id)
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
    for csv_path in csv_files:
        parsed = _parse_carelink_csv(csv_path)
        bg_points.extend(parsed['bg'])
        basal_points.extend(parsed['basal'])
        bolus_events.extend(parsed['bolus'])

    bg_points.sort(key=lambda x: x['timestamp'])
    basal_points.sort(key=lambda x: x['timestamp'])
    bolus_events.sort(key=lambda x: x['timestamp'])

    # Keep unfiltered basal samples so we can carry the last known rate into chart start.
    basal_points_all = list(basal_points)

    bg_points = _window_filter(bg_points, start_dt, end_dt, 'timestamp')
    basal_points = _window_filter(basal_points, start_dt, end_dt, 'timestamp')
    bolus_events = _window_filter(bolus_events, start_dt, end_dt, 'timestamp')

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
    weather_effort = _weather_effort_analytics(activity_points, weather_points, metrics.get('duration_h'))
    stress_analytics = _stress_decoupling_analytics(activity_points, weather_points)

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

    for other_walk_dir in _iter_walk_dirs():
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
        'weather': weather_points,
        'windProfile': weather_effort['wind_profile'],
        'windRose': weather_effort['wind_rose'],
        'stressAnalytics': stress_analytics,
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
    })

    if persist_analytics:
        persist_walk_analytics(walk_dir, walk_meta, payload, metrics)

    return date, walk_name, payload, metrics


@app.get('/api/analytics/walks')
def list_walk_analytics():
    return list_cached_walk_analytics(
        iter_walk_dirs=_iter_walk_dirs,
        load_walk_meta=_load_walk_meta,
    )


@app.post('/api/analytics/{walk_id}/refresh')
def refresh_walk_analytics(walk_id: str, force: bool = False):
    return refresh_walk_analytics_if_needed(
        walk_id,
        force=force,
        find_walk_dir=_find_walk_dir,
        get_walk_analysis_data=_get_walk_analysis_data,
    )


@app.post('/api/analytics/backfill')
def backfill_walk_analytics(force: bool = False):
    results: list[dict] = []
    for walk_dir in _iter_walk_dirs():
        results.append(
            refresh_walk_analytics_if_needed(
                walk_dir.name,
                force=force,
                find_walk_dir=_find_walk_dir,
                get_walk_analysis_data=_get_walk_analysis_data,
            )
        )
    return {
        'analytics_version': ANALYTICS_VERSION,
        'walk_count': len(results),
        'results': results,
    }


@app.get('/api/walks/{walk_id}/analysis-data')
def walk_analysis_data(walk_id: str):
    date, walk_name, payload, metrics = _get_walk_analysis_data(walk_id, persist_analytics=True)
    return {
        'walk_id': walk_id,
        'date': date,
        'name': walk_name,
        'payload': payload,
        'metrics': metrics,
    }


@app.get('/api/walks/{walk_id}/analysis')
def walk_analysis(walk_id: str):
    date, walk_name, payload, metrics = _get_walk_analysis_data(walk_id, persist_analytics=True)
    # Backward-compatible alias for clients that used /analysis.
    return {
        'walk_id': walk_id,
        'date': date,
        'name': walk_name,
        'payload': payload,
        'metrics': metrics,
    }



