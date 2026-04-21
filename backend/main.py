import json
import csv
import html
import math
import shutil
import struct
from datetime import date as date_type, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional
import xml.etree.ElementTree as ET

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

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


def _parse_date(date_str: str) -> str:
    try:
        date_type.fromisoformat(date_str)
        return date_str
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format, expected YYYY-MM-DD")


def _fit_start_date(data: bytes) -> str | None:
    """Return the start date (YYYY-MM-DD) from a FIT file, or None."""
    try:
        if len(data) < 12 or data[8:12] != b'.FIT':
            return None

        header_size = data[0]
        data_size = struct.unpack_from('<I', data, 4)[0]
        pos = header_size
        end = min(header_size + data_size, len(data))

        # local_num -> [(field_num, field_size, little_endian)]
        definitions: dict = {}

        while pos < end:
            header_byte = data[pos]
            pos += 1

            if header_byte & 0x80:  # compressed timestamp header
                local_num = (header_byte >> 5) & 0x03
                if local_num not in definitions:
                    break
                pos += sum(fs for _, fs, _ in definitions[local_num])
            else:
                local_num = header_byte & 0x0F
                if header_byte & 0x40:  # definition message
                    pos += 1  # reserved
                    little_endian = data[pos] == 0
                    pos += 3  # architecture + global message number
                    num_fields = data[pos]; pos += 1
                    fields = []
                    for _ in range(num_fields):
                        fields.append((data[pos], data[pos + 1], little_endian))
                        pos += 3
                    definitions[local_num] = fields
                else:  # data message
                    if local_num not in definitions:
                        break
                    ts_value = None
                    for fnum, fsize, little_endian in definitions[local_num]:
                        if fnum == 253 and fsize == 4:
                            fmt = '<I' if little_endian else '>I'
                            ts_value = struct.unpack_from(fmt, data, pos)[0]
                        pos += fsize
                    if ts_value is not None:
                        dt = FIT_EPOCH + timedelta(seconds=ts_value)
                        return dt.strftime('%Y-%m-%d')
    except Exception:
        pass
    return None


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
            pos += 1  # reserved
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
    return {'bg': bg_points, 'basal': basal_points, 'bolus': bolus_events}


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


def _fmt(value: float | None, digits: int = 2, suffix: str = '') -> str:
    if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
        return 'n/a'
    return f'{value:.{digits}f}{suffix}'


def _build_analysis_html(date: str, walk_name: str, payload: dict, metrics: dict) -> str:
    title = walk_name or f'Walk {date}'
    json_payload = json.dumps(payload)
    safe_title = html.escape(title)

    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Walkies Analysis - {safe_title}</title>
  <link rel=\"preconnect\" href=\"https://fonts.googleapis.com\" />
  <link rel=\"preconnect\" href=\"https://fonts.gstatic.com\" crossorigin />
  <link href=\"https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&family=Fraunces:opsz,wght@9..144,700&display=swap\" rel=\"stylesheet\" />
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" integrity=\"sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=\" crossorigin=\"\" />
  <style>
    :root {{
      --ink: #102539;
      --bg: #f7fbff;
      --panel: #ffffff;
      --accent: #0a8f8f;
      --accent-soft: #dcf4f4;
      --grid: #d8e4ef;
      --warn: #c64200;
      --ok: #2f7c4f;
      --shadow: 0 10px 28px rgba(16, 37, 57, 0.12);
      --radius: 14px;
    }}

    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      font-family: 'Manrope', sans-serif;
      background: radial-gradient(circle at top right, #e4f7f7 0%, var(--bg) 45%), var(--bg);
      padding: 18px;
    }}

    .container {{ max-width: 1200px; margin: 0 auto; display: grid; gap: 14px; }}
    .hero {{
      background: linear-gradient(135deg, #12344a, #0a8f8f);
      color: #fff;
      padding: 18px 22px;
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      animation: fade-in 500ms ease;
    }}

    .hero h1 {{
      margin: 0;
      font-family: 'Fraunces', serif;
      font-size: clamp(1.35rem, 2.2vw, 2rem);
      letter-spacing: 0.01em;
    }}

    .hero p {{ margin: 6px 0 0; opacity: 0.9; }}

    .stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
      gap: 10px;
      animation: fade-in 700ms ease;
    }}

    .stat {{
      background: var(--panel);
      border: 1px solid #e4edf6;
      border-radius: 12px;
      padding: 10px 12px;
      box-shadow: 0 6px 18px rgba(16, 37, 57, 0.08);
    }}

    .label {{ font-size: 0.78rem; opacity: 0.75; text-transform: uppercase; letter-spacing: 0.05em; }}
    .value {{ margin-top: 4px; font-size: 1.2rem; font-weight: 800; }}

    .panel {{
      background: var(--panel);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      overflow: hidden;
      animation: fade-in 900ms ease;
    }}

    .panel-head {{
      padding: 10px 14px;
      border-bottom: 1px solid #ebf1f7;
      font-weight: 700;
      background: linear-gradient(90deg, #f8fdff, #f2fbfb);
    }}

    #timeline {{ min-height: 520px; }}
    #map {{ height: 440px; }}

    @keyframes fade-in {{
      from {{ opacity: 0; transform: translateY(8px); }}
      to {{ opacity: 1; transform: translateY(0); }}
    }}

    @media (max-width: 768px) {{
      body {{ padding: 10px; }}
      #timeline {{ min-height: 440px; }}
      #map {{ height: 360px; }}
    }}
  </style>
</head>
<body>
  <div class=\"container\">
    <section class=\"hero\">
      <h1>{safe_title}</h1>
      <p>{html.escape(date)} | Distance, HR, BG, insulin delivery, altitude, and route context</p>
    </section>

    <section class=\"stats\">
      <article class=\"stat\"><div class=\"label\">Distance</div><div class=\"value\">{_fmt(metrics['distance_km'], 2, ' km')}</div></article>
      <article class=\"stat\"><div class=\"label\">Duration</div><div class=\"value\">{_fmt(metrics['duration_h'], 2, ' h')}</div></article>
      <article class=\"stat\"><div class=\"label\">Avg HR</div><div class=\"value\">{_fmt(metrics['avg_hr'], 0, ' bpm')}</div></article>
      <article class=\"stat\"><div class=\"label\">BG Delta (Walk)</div><div class=\"value\">{_fmt(metrics['bg_delta'], 2, ' mmol/L')}</div></article>
      <article class=\"stat\"><div class=\"label\">Time In Range</div><div class=\"value\">{_fmt(metrics['tir_pct'], 0, '%')}</div></article>
      <article class=\"stat\"><div class=\"label\">Hypos During Walk</div><div class=\"value\">{metrics['hypos']}</div></article>
      <article class=\"stat\"><div class=\"label\">Bolus During Walk</div><div class=\"value\">{_fmt(metrics['bolus_units'], 2, ' U')}</div></article>
    </section>

    <section class=\"panel\">
      <div class=\"panel-head\">Timeline</div>
      <div id=\"timeline\"></div>
    </section>

    <section class=\"panel\">
      <div class=\"panel-head\">Map</div>
      <div id=\"map\"></div>
    </section>
  </div>

  <script src=\"https://cdn.plot.ly/plotly-2.35.2.min.js\"></script>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\" integrity=\"sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=\" crossorigin=\"\"></script>
  <script>
    const payload = {json_payload};

        const toSeries = (items, yKey) => ({{
      x: items.map(p => p.distance_km),
      y: items.map(p => p[yKey]),
        }});

    const hrSeries = toSeries(payload.activity, 'hr');
    const altitudeSeries = toSeries(payload.activity, 'altitude_m');
    const bgSeries = toSeries(payload.bg, 'bg');
    const basalSeries = toSeries(payload.basal, 'rate');
    const bolusSeries = toSeries(payload.bolus, 'units');

    const traces = [
            // Altitude baseline (invisible) for fill reference — avoids tozeroy forcing y-axis to include 0
            {{
                x: altitudeSeries.x,
                y: (() => {{ const vals = altitudeSeries.y.filter(v => v != null); const base = vals.length ? Math.min(...vals) : 0; return altitudeSeries.x.map(() => base); }})(),
                name: 'alt_base',
                mode: 'lines',
                line: {{ width: 0, color: 'transparent' }},
                showlegend: false,
                hoverinfo: 'skip',
                yaxis: 'y',
            }},
            {{
                x: altitudeSeries.x,
                y: altitudeSeries.y,
                name: 'Altitude (m)',
                mode: 'lines',
                fill: 'tonexty',
                fillcolor: 'rgba(90,111,133,0.15)',
                line: {{ width: 1.6, color: '#5a6f85' }},
                                hovertemplate: 'Altitude: %{{y:.1f}} m<extra></extra>',
                yaxis: 'y',
            }},
            {{
                x: hrSeries.x,
                y: hrSeries.y,
                name: 'HR (bpm)',
                mode: 'lines',
                line: {{ width: 2, color: '#ef5b0c' }},
                                hovertemplate: 'HR: %{{y:.0f}} bpm<extra></extra>',
                yaxis: 'y2',
            }},
      {{
        x: bgSeries.x,
        y: bgSeries.y,
        name: 'BG (mmol/L)',
        mode: 'lines+markers',
        marker: {{ size: 5, color: '#7d00b8' }},
        line: {{ width: 2, color: '#7d00b8' }},
                hovertemplate: 'BG: %{{y:.1f}} mmol/L<extra></extra>',
        yaxis: 'y3',
      }},
      {{
        x: basalSeries.x,
        y: basalSeries.y,
        name: 'Basal (U/h)',
        mode: 'lines',
        line: {{ width: 1.6, color: '#00745a' }},
                hovertemplate: 'Basal: %{{y:.2f}} U/h<extra></extra>',
        yaxis: 'y4',
      }},
      {{
        x: bolusSeries.x,
        y: bolusSeries.y,
        name: 'Bolus (U)',
        type: 'bar',
        marker: {{ color: '#0057b8' }},
                hovertemplate: 'Bolus: %{{y:.2f}} U<extra></extra>',
        yaxis: 'y4',
        opacity: 0.55,
      }},
    ];

    const layout = {{
      margin: {{ l: 58, r: 68, t: 24, b: 42 }},
      paper_bgcolor: '#ffffff',
      plot_bgcolor: '#ffffff',
        hovermode: 'x',
        hoverlabel: {{ bgcolor: '#ffffff', bordercolor: '#c9d6e3', font: {{ color: '#102539' }} }},
      legend: {{ orientation: 'h', y: 1.14, x: 0 }},
            xaxis: {{ showgrid: true, gridcolor: '#edf3f9', title: 'Distance (km)', side: 'bottom', anchor: 'y3', automargin: true, showspikes: true, spikemode: 'across', spikesnap: 'cursor', spikecolor: 'rgba(44,62,80,0.45)', spikethickness: 1.2, range: payload.chartDistanceStart != null && payload.chartDistanceEnd != null ? [payload.chartDistanceStart, payload.chartDistanceEnd] : undefined }},
      annotations: (payload.timeMarkers || []).map(m => ({{
        xref: 'x', yref: 'paper', x: m.distance_km, y: 1.03,
        text: m.label, showarrow: false,
        font: {{ size: 9, color: '#667788' }}, xanchor: 'center',
            }})).concat(
                payload.walkDistanceStart != null && payload.walkDistanceEnd != null
                    ? [
                            {{
                                xref: 'x', yref: 'paper', x: payload.walkDistanceStart, y: -0.12,
                                text: 'Walk start', showarrow: false,
                                font: {{ size: 10, color: '#5b6c7d' }}, xanchor: 'left',
                            }},
                            {{
                                xref: 'x', yref: 'paper', x: payload.walkDistanceEnd, y: -0.12,
                                text: 'Walk end', showarrow: false,
                                font: {{ size: 10, color: '#5b6c7d' }}, xanchor: 'right',
                            }},
                        ]
                    : []
            ),

    yaxis: {{ domain: [0.70, 1.0], title: 'Altitude (m)', showgrid: true, gridcolor: '#edf3f9' }},
    yaxis2: {{ domain: [0.38, 0.64], title: 'HR (bpm)', showgrid: true, gridcolor: '#edf3f9' }},
      yaxis3: {{ domain: [0.0, 0.30], title: 'BG (mmol/L)', showgrid: true, gridcolor: '#edf3f9' }},
      yaxis4: {{ overlaying: 'y3', side: 'right', title: 'Insulin', showgrid: false }},
      shapes: [
                ...(payload.walkDistanceStart != null && payload.walkDistanceEnd != null ? [{{
                    type: 'rect', xref: 'x', yref: 'paper',
                    x0: payload.walkDistanceStart, x1: payload.walkDistanceEnd, y0: 0, y1: 1,
                    fillcolor: 'rgba(16, 37, 57, 0.03)', line: {{ width: 0 }}, layer: 'below',
                }}, {{
                    type: 'line', xref: 'x', yref: 'paper',
                    x0: payload.walkDistanceStart, x1: payload.walkDistanceStart, y0: 0, y1: 1,
                    line: {{ color: 'rgba(16, 37, 57, 0.25)', width: 1.5, dash: 'solid' }},
                }}, {{
                    type: 'line', xref: 'x', yref: 'paper',
                    x0: payload.walkDistanceEnd, x1: payload.walkDistanceEnd, y0: 0, y1: 1,
                    line: {{ color: 'rgba(16, 37, 57, 0.25)', width: 1.5, dash: 'solid' }},
                }}] : []),
        {{ type: 'rect', xref: 'paper', yref: 'y3', x0: 0, x1: 1, y0: 4.0, y1: 8.0, fillcolor: 'rgba(47, 124, 79, 0.12)', line: {{ width: 0 }} }},
        ...(payload.timeMarkers || []).map(m => ({{
          type: 'line', xref: 'x', yref: 'paper',
          x0: m.distance_km, x1: m.distance_km, y0: 0, y1: 1,
          line: {{ color: 'rgba(100,110,130,0.3)', width: 1, dash: m.label.endsWith('00') ? 'solid' : 'dot' }},
        }})),
      ],
    }};

    Plotly.newPlot('timeline', traces, layout, {{ responsive: true, displaylogo: false }});

    const map = L.map('map');
    const mapTrack = payload.mapTrack;
    const hasTrack = Array.isArray(mapTrack) && mapTrack.length > 1;

    L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      attribution: '&copy; OpenStreetMap contributors',
      maxZoom: 18,
    }}).addTo(map);

    if (hasTrack) {{
      const bounds = [];
      const colorForHr = (hr) => {{
        if (hr == null) return '#1e5f99';
        if (hr < 110) return '#1f8a70';
        if (hr < 130) return '#94b447';
        if (hr < 150) return '#e6a700';
        return '#d64545';
      }};

      for (let i = 1; i < mapTrack.length; i++) {{
        const a = mapTrack[i - 1];
        const b = mapTrack[i];
        const segment = [[a[0], a[1]], [b[0], b[1]]];
        const hr = a.length > 2 ? a[2] : null;
        L.polyline(segment, {{ color: colorForHr(hr), weight: 5, opacity: 0.85 }}).addTo(map);
        bounds.push(segment[0], segment[1]);
      }}

      L.circleMarker(mapTrack[0], {{ radius: 6, color: '#114b5f', fillColor: '#fff', fillOpacity: 1, weight: 2 }}).addTo(map).bindTooltip('Start');
      L.circleMarker(mapTrack[mapTrack.length - 1], {{ radius: 6, color: '#8a1c7c', fillColor: '#fff', fillOpacity: 1, weight: 2 }}).addTo(map).bindTooltip('Finish');

            (payload.mapHourMarkers || []).forEach(marker => {{
                L.circleMarker([marker.lat, marker.lon], {{
                    radius: 5,
                    color: '#3c4f65',
                    fillColor: '#ffffff',
                    fillOpacity: 1,
                    weight: 2,
                }}).addTo(map).bindTooltip(marker.label, {{ direction: 'top', offset: [0, -4] }});
            }});

      map.fitBounds(bounds, {{ padding: [24, 24] }});

      const legend = L.control({{ position: 'bottomright' }});
      legend.onAdd = () => {{
        const div = L.DomUtil.create('div');
        div.style.cssText = 'background:#fff;padding:8px 10px;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.18);font:12px Manrope,sans-serif;line-height:1.7;min-width:120px';
        div.innerHTML = `
          <div style="font-weight:700;margin-bottom:4px">Heart Rate</div>
          <div><span style="display:inline-block;width:14px;height:4px;background:#1f8a70;border-radius:2px;vertical-align:middle;margin-right:6px"></span>&lt; 110 bpm</div>
          <div><span style="display:inline-block;width:14px;height:4px;background:#94b447;border-radius:2px;vertical-align:middle;margin-right:6px"></span>110–129 bpm</div>
          <div><span style="display:inline-block;width:14px;height:4px;background:#e6a700;border-radius:2px;vertical-align:middle;margin-right:6px"></span>130–149 bpm</div>
          <div><span style="display:inline-block;width:14px;height:4px;background:#d64545;border-radius:2px;vertical-align:middle;margin-right:6px"></span>≥ 150 bpm</div>
          <div><span style="display:inline-block;width:14px;height:4px;background:#1e5f99;border-radius:2px;vertical-align:middle;margin-right:6px"></span>No HR data</div>
          <div style="margin-top:6px;font-weight:700">Markers</div>
          <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;border:2px solid #114b5f;background:#fff;vertical-align:middle;margin-right:6px"></span>Start</div>
          <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;border:2px solid #8a1c7c;background:#fff;vertical-align:middle;margin-right:6px"></span>Finish</div>
                    <div><span style="display:inline-block;width:10px;height:10px;border-radius:50%;border:2px solid #3c4f65;background:#fff;vertical-align:middle;margin-right:6px"></span>Hourly marker</div>
        `;
        return div;
      }};
      legend.addTo(map);
    }} else {{
      map.setView([54.43, -2.96], 11);
    }}
  </script>
</body>
</html>
"""


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
    for folder in sorted(DATA_DIR.iterdir(), reverse=True):
        if not folder.is_dir():
            continue
        meta_file = folder / "meta.json"
        meta = json.loads(meta_file.read_text()) if meta_file.exists() else {}
        files = sorted(f.name for f in folder.iterdir() if f.is_file() and f.name != "meta.json")
        walks.append({"date": folder.name, "name": meta.get("name", ""), "files": files})
    return walks


@app.post("/api/walks/upload")
async def upload_files(
    date: str = Form(...),
    name: Optional[str] = Form(""),
    files: List[UploadFile] = File(...),
):
    date = _parse_date(date)
    walk_dir = DATA_DIR / date
    walk_dir.mkdir(exist_ok=True)

    if name and name.strip():
        (walk_dir / "meta.json").write_text(json.dumps({"name": name.strip()}))

    saved = []
    for file in files:
        dest = walk_dir / file.filename
        with dest.open("wb") as f:
            shutil.copyfileobj(file.file, f)
        saved.append(file.filename)

    return {"date": date, "uploaded": saved}


@app.delete("/api/walks/{date}")
def delete_walk(date: str):
    date = _parse_date(date)
    walk_dir = DATA_DIR / date
    if not walk_dir.exists():
        raise HTTPException(status_code=404, detail="Walk not found")
    shutil.rmtree(walk_dir)
    return {"deleted": date}


@app.get('/api/walks/{date}/analysis', response_class=HTMLResponse)
def walk_analysis(date: str):
    date = _parse_date(date)
    walk_dir = DATA_DIR / date
    if not walk_dir.exists() or not walk_dir.is_dir():
        raise HTTPException(status_code=404, detail='Walk not found')

    meta_file = walk_dir / 'meta.json'
    walk_name = ''
    if meta_file.exists():
        try:
            walk_name = json.loads(meta_file.read_text(encoding='utf-8')).get('name', '')
        except Exception:
            walk_name = ''

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
        start_dt = datetime.fromisoformat(activity_points[0]['timestamp_iso']) - timedelta(hours=2)
        end_dt = datetime.fromisoformat(activity_points[-1]['timestamp_iso']) + timedelta(hours=2)
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
    bg_buffer_secs = 15 * 60
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

    payload = {
        'activity': activity_series,
        'bg': bg_points,
        'basal': basal_points,
        'bolus': bolus_events,
        'mapTrack': map_track,
        'mapHourMarkers': map_hour_markers,
        'timeMarkers': time_markers,
        'walkDistanceStart': 0.0 if chart_distance_values else None,
        'walkDistanceEnd': walk_distance_end,
        'chartDistanceStart': chart_distance_start,
        'chartDistanceEnd': chart_distance_end,
    }
    metrics = _summary_metrics(activity_points, bg_points, bolus_events)
    return _build_analysis_html(date, walk_name, payload, metrics)
