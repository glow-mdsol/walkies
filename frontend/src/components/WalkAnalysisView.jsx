import { useEffect, useMemo, useState } from 'react'
import { buildIobFn } from './InsulinSetup'
import {
  ResponsiveContainer,
  BarChart,
  LineChart,
  Line,
  Area,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  Bar,
  ComposedChart,
  Cell,
} from 'recharts'
import {
  MapContainer,
  TileLayer,
  Polyline,
  CircleMarker,
  Tooltip as LeafletTooltip,
} from 'react-leaflet'

function fmt(value, digits = 1, suffix = '') {
  if (value == null || Number.isNaN(value)) return 'n/a'
  return `${Number(value).toFixed(digits)}${suffix}`
}

function Stat({ label, value }) {
  return (
    <article className="stat-card">
      <div className="stat-label">{label}</div>
      <div className="stat-value">{value}</div>
    </article>
  )
}

function ChartCard({ title, children, empty }) {
  return (
    <section className="card chart-card">
      <h2>{title}</h2>
      {empty ? <p className="empty">{empty}</p> : <div className="chart-wrap">{children}</div>}
    </section>
  )
}

function num(value, digits = 1) {
  if (value == null || Number.isNaN(value)) return ''
  return Number(value).toFixed(digits)
}

function avgOrNull(values) {
  const clean = values.filter((v) => v != null && !Number.isNaN(Number(v))).map(Number)
  if (!clean.length) return null
  return clean.reduce((a, b) => a + b, 0) / clean.length
}

function bucketSeries(rows, maxPoints) {
  if (!Array.isArray(rows) || rows.length <= maxPoints) return rows
  const bucketSize = Math.ceil(rows.length / maxPoints)
  const reduced = []
  for (let i = 0; i < rows.length; i += bucketSize) {
    const chunk = rows.slice(i, i + bucketSize)
    reduced.push({
      distance_km: avgOrNull(chunk.map((r) => r.distance_km)),
      temp_c: avgOrNull(chunk.map((r) => r.temp_c)),
      apparent_c: avgOrNull(chunk.map((r) => r.apparent_c)),
      wind_kph: avgOrNull(chunk.map((r) => r.wind_kph)),
      headwind_kph: avgOrNull(chunk.map((r) => r.headwind_kph)),
    })
  }
  return reduced
}

function buildDistanceDomain(distances, fallback = [0, 1]) {
  const clean = distances.filter((v) => v != null)
  if (!clean.length) return fallback

  const min = Math.min(...clean)
  const max = Math.max(...clean)
  const span = Math.max(max - min, 0.2)
  const buffer = span * 0.05
  return [min - buffer, max + buffer]
}

function buildDistanceTicks(domain) {
  const [start, end] = domain
  if (end <= start) return [start]

  const count = 7
  const step = (end - start) / (count - 1)
  const ticks = Array.from({ length: count }, (_, i) => Number((start + (step * i)).toFixed(2)))
  if (start < 0 && end > 0 && !ticks.some((v) => Math.abs(v) < 0.01)) {
    ticks.push(0)
  }
  return [...new Set(ticks)].sort((a, b) => a - b)
}

function formatDistanceTick(value) {
  if (value < 0) return ''
  return num(value, 1)
}

const INSULIN_AXIS_MAX = 10

function BolusBarShape(props) {
  const {
    x,
    y,
    width,
    height,
    fill,
    payload,
    activeDistance,
  } = props

  const bolus = payload?.bolus
  const overLimit = bolus != null && Number(bolus) > INSULIN_AXIS_MAX
  const centerX = x + width / 2
  const baseY = y + height
  const isActive =
    activeDistance != null
    && payload?.distance_km != null
    && Math.abs(Number(payload.distance_km) - Number(activeDistance)) < 0.0001

  if (overLimit) {
    return (
      <g>
        <line x1={centerX} y1={baseY} x2={centerX} y2={y} stroke={fill} strokeWidth={3} />
        <circle cx={centerX} cy={y} r={4} fill={fill} />
        {isActive ? (
          <text x={centerX + 5} y={Math.max(12, y - 8)} fill={fill} fontSize={10} fontWeight={700}>
            {`${num(bolus, 1)}U`}
          </text>
        ) : null}
      </g>
    )
  }

  return <rect x={x} y={y} width={width} height={height} fill={fill} />
}

function hrColor(hr) {
  if (hr == null || Number.isNaN(Number(hr))) return '#5f7a96'
  const h = Number(hr)
  if (h < 110) return '#2f6fb0'
  if (h < 130) return '#1f8a5b'
  if (h < 150) return '#d16a00'
  return '#be2f2f'
}

function RouteMap({ track, hourMarkers }) {
  const cleanTrack = useMemo(
    () => (track || [])
      .filter((p) => Array.isArray(p) && p.length >= 2 && p[0] != null && p[1] != null)
      .map((p) => ({ lat: Number(p[0]), lon: Number(p[1]), hr: p[2] == null ? null : Number(p[2]) })),
    [track],
  )

  const sampledTrack = useMemo(() => {
    const maxPoints = 1200
    if (cleanTrack.length <= maxPoints) return cleanTrack
    const step = Math.ceil(cleanTrack.length / maxPoints)
    return cleanTrack.filter((_, idx) => idx % step === 0)
  }, [cleanTrack])

  const latLngs = useMemo(
    () => sampledTrack.map((p) => [p.lat, p.lon]),
    [sampledTrack],
  )

  const segments = useMemo(
    () => sampledTrack.slice(1).map((pt, idx) => ({
      coords: [
        [sampledTrack[idx].lat, sampledTrack[idx].lon],
        [pt.lat, pt.lon],
      ],
      hr: pt.hr,
    })),
    [sampledTrack],
  )

  const hourMarkerRows = useMemo(
    () => (Array.isArray(hourMarkers) ? hourMarkers : [])
      .filter((m) => m?.lat != null && m?.lon != null)
      .map((m) => ({ lat: Number(m.lat), lon: Number(m.lon), label: m.label || '' })),
    [hourMarkers],
  )

  if (!sampledTrack.length) {
    return <p className="empty">No GPS track available for this walk.</p>
  }

  const start = sampledTrack[0]
  const end = sampledTrack[sampledTrack.length - 1]

  return (
    <div className="route-map-shell">
      <MapContainer
        className="route-map"
        bounds={latLngs}
        boundsOptions={{ padding: [20, 20] }}
        scrollWheelZoom
      >
        <TileLayer
          attribution="&copy; OpenStreetMap contributors"
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {segments.map((seg, idx) => (
          <Polyline
            key={`seg-${idx}`}
            positions={seg.coords}
            pathOptions={{ color: hrColor(seg.hr), weight: 4, opacity: 0.9 }}
          />
        ))}
        {hourMarkerRows.map((m, idx) => (
          <CircleMarker key={`hour-${idx}`} center={[m.lat, m.lon]} radius={4} pathOptions={{ color: '#2b4b73', weight: 2, fillColor: '#2b4b73', fillOpacity: 0.9 }}>
            <LeafletTooltip direction="top" permanent offset={[0, -6]} className="route-hour-badge">{m.label || `${idx + 1}h`}</LeafletTooltip>
          </CircleMarker>
        ))}
        <CircleMarker center={[start.lat, start.lon]} radius={6} pathOptions={{ color: '#ffffff', weight: 2, fillColor: '#1f8a5b', fillOpacity: 1 }}>
          <LeafletTooltip direction="top" offset={[0, -6]}>Start</LeafletTooltip>
        </CircleMarker>
        <CircleMarker center={[end.lat, end.lon]} radius={6} pathOptions={{ color: '#ffffff', weight: 2, fillColor: '#be2f2f', fillOpacity: 1 }}>
          <LeafletTooltip direction="top" offset={[0, -6]}>Finish</LeafletTooltip>
        </CircleMarker>
      </MapContainer>
      <div className="route-legend">
        <span><i style={{ background: '#2f6fb0' }} />Easy HR</span>
        <span><i style={{ background: '#1f8a5b' }} />Steady HR</span>
        <span><i style={{ background: '#d16a00' }} />Hard HR</span>
        <span><i style={{ background: '#be2f2f' }} />Peak HR</span>
      </div>
    </div>
  )
}

const DISTANCE_CHART_MARGIN = { top: 8, right: 8, left: 8, bottom: 8 }
const DISTANCE_LEFT_Y_WIDTH = 62
const DISTANCE_RIGHT_Y_WIDTH = 64
const EFFORT_HR_REFERENCE_DEFAULT = 120
const EFFORT_HR_REFERENCE_MIN = 90
const EFFORT_HR_REFERENCE_MAX = 150
const EFFORT_SCALE_MIN_DEFAULT = 0.6
const EFFORT_SCALE_MIN_MIN = 0.3
const EFFORT_SCALE_MIN_MAX = 1.0
const EFFORT_SCALE_MAX_DEFAULT = 1.8
const EFFORT_SCALE_MAX_MIN = 1.1
const EFFORT_SCALE_MAX_MAX = 2.5
const ANALYTICS_PREFS_KEY = 'walkies.analyticsPrefs.v1'

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function loadAnalyticsPrefs() {
  if (typeof window === 'undefined') {
    return {
      effortHrReference: EFFORT_HR_REFERENCE_DEFAULT,
      effortScaleMin: EFFORT_SCALE_MIN_DEFAULT,
      effortScaleMax: EFFORT_SCALE_MAX_DEFAULT,
    }
  }

  try {
    const raw = window.localStorage.getItem(ANALYTICS_PREFS_KEY)
    const parsed = raw ? JSON.parse(raw) : {}
    const effortScaleMin = clamp(Number(parsed.effortScaleMin) || EFFORT_SCALE_MIN_DEFAULT, EFFORT_SCALE_MIN_MIN, EFFORT_SCALE_MIN_MAX)
    const effortScaleMax = clamp(Number(parsed.effortScaleMax) || EFFORT_SCALE_MAX_DEFAULT, EFFORT_SCALE_MAX_MIN, EFFORT_SCALE_MAX_MAX)
    return {
      effortHrReference: clamp(Number(parsed.effortHrReference) || EFFORT_HR_REFERENCE_DEFAULT, EFFORT_HR_REFERENCE_MIN, EFFORT_HR_REFERENCE_MAX),
      effortScaleMin: Math.min(effortScaleMin, effortScaleMax - 0.1),
      effortScaleMax: Math.max(effortScaleMax, effortScaleMin + 0.1),
    }
  } catch {
    return {
      effortHrReference: EFFORT_HR_REFERENCE_DEFAULT,
      effortScaleMin: EFFORT_SCALE_MIN_DEFAULT,
      effortScaleMax: EFFORT_SCALE_MAX_DEFAULT,
    }
  }
}

export default function WalkAnalysisView({ walkId, insulinProfile, onBack }) {
  const initialPrefs = useMemo(() => loadAnalyticsPrefs(), [])
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)
  const [activeDistance, setActiveDistance] = useState(null)
  const [effortHrReference, setEffortHrReference] = useState(initialPrefs.effortHrReference)
  const [effortScaleMin, setEffortScaleMin] = useState(initialPrefs.effortScaleMin)
  const [effortScaleMax, setEffortScaleMax] = useState(initialPrefs.effortScaleMax)

  useEffect(() => {
    if (!walkId) return
    setLoading(true)
    setError(null)

    fetch(`/api/walks/${encodeURIComponent(walkId)}/analysis-data`)
      .then(async (res) => {
        if (!res.ok) {
          const payload = await res.json().catch(() => ({}))
          throw new Error(payload.detail || 'Failed to load walk analysis')
        }
        return res.json()
      })
      .then(setData)
      .catch((err) => setError(err.message || 'Failed to load walk analysis'))
      .finally(() => setLoading(false))
  }, [walkId])

  useEffect(() => {
    if (typeof window === 'undefined') return
    window.localStorage.setItem(ANALYTICS_PREFS_KEY, JSON.stringify({
      effortHrReference,
      effortScaleMin,
      effortScaleMax,
    }))
  }, [effortHrReference, effortScaleMin, effortScaleMax])

  const metrics = data?.metrics || {}
  const payload = data?.payload || {}
  const stressSummary = data?.payload?.stressAnalytics?.summary || {}
  const trendRows = useMemo(() => (data?.payload?.stressTrend || []).slice(-8), [data])

  const activityRows = useMemo(
    () => (payload.activity || []).filter((r) => r.distance_km != null),
    [payload.activity],
  )
  const glucoseRows = useMemo(
    () => (payload.bg || []).filter((r) => r.distance_km != null),
    [payload.bg],
  )
  const basalRows = useMemo(
    () => (payload.basal || []).filter((r) => r.distance_km != null),
    [payload.basal],
  )
  const bolusRows = useMemo(
    () => (payload.bolus || []).filter((r) => r.distance_km != null),
    [payload.bolus],
  )
  const weatherRows = useMemo(
    () => (payload.weather || []).filter((r) => r.distance_km != null),
    [payload.weather],
  )
  const windRows = useMemo(
    () => (payload.windProfile || []).filter((r) => r.distance_km != null),
    [payload.windProfile],
  )
  const stressRows = useMemo(
    () => (payload.stressAnalytics?.series || []).filter((r) => r.distance_km != null),
    [payload.stressAnalytics],
  )
  const mapTrack = payload.mapTrack || []
  const mapHourMarkers = payload.mapHourMarkers || []

  const glucoseChartRows = useMemo(() => {
    const byDistance = new Map()
    activityRows.forEach((row) => {
      if (row.distance_km == null) return
      byDistance.set(row.distance_km, {
        distance_km: row.distance_km,
        timestamp: row.timestamp_iso || null,
        hr: row.hr ?? null,
        bg: null,
        basal: null,
        bolus: null,
      })
    })
    glucoseRows.forEach((row) => {
      byDistance.set(row.distance_km, {
        distance_km: row.distance_km,
        timestamp: row.timestamp || null,
        hr: null,
        bg: row.bg,
        basal: null,
        bolus: null,
      })
    })
    basalRows.forEach((row) => {
      const key = row.distance_km
      const existing = byDistance.get(key) || { distance_km: key, timestamp: row.timestamp || null, hr: null, bg: null, basal: null, bolus: null }
      if (!existing.timestamp && row.timestamp) existing.timestamp = row.timestamp
      existing.basal = row.rate
      byDistance.set(key, existing)
    })
    bolusRows.forEach((row) => {
      const key = row.distance_km
      const existing = byDistance.get(key) || { distance_km: key, timestamp: row.timestamp || null, hr: null, bg: null, basal: null, bolus: null }
      if (!existing.timestamp && row.timestamp) existing.timestamp = row.timestamp
      existing.bolus = row.units
      byDistance.set(key, existing)
    })
    return Array.from(byDistance.values())
      .map((row) => ({
        ...row,
        bolus_display: row.bolus == null ? null : Math.min(Number(row.bolus), INSULIN_AXIS_MAX),
      }))
      .sort((a, b) => a.distance_km - b.distance_km)
  }, [activityRows, glucoseRows, basalRows, bolusRows])

  const glucoseInsulinChartRows = useMemo(() => {
    if (!glucoseChartRows.length) return []

    // Build IOB function from insulin profile
    const iob = insulinProfile ? buildIobFn(insulinProfile) : null
    if (!iob) return glucoseChartRows.map((row) => ({ ...row, bolus_decay: null }))

    const durationSecs = ((insulinProfile.durationMinHours + insulinProfile.durationMaxHours) / 2) * 3600

    const bolusEvents = bolusRows
      .map((row) => ({

        ts: Date.parse(row.timestamp || ''),
        units: Number(row.units),
      }))
      .filter((row) => Number.isFinite(row.ts) && Number.isFinite(row.units) && row.units > 0)
      .sort((a, b) => a.ts - b.ts)

    if (!bolusEvents.length) {
      return glucoseChartRows.map((row) => ({ ...row, bolus_decay: null }))
    }

    const effortScale = (hrValue) => {
      if (!Number.isFinite(hrValue)) return 1
      const scaled = hrValue / effortHrReference
      return Math.max(effortScaleMin, Math.min(effortScaleMax, scaled))
    }

    const effortSamples = activityRows
      .map((row) => ({
        ts: Date.parse(row.timestamp_iso || ''),
        hr: Number(row.hr),
      }))
      .filter((row) => Number.isFinite(row.ts))
      .sort((a, b) => a.ts - b.ts)

    const cumulativeEffortSeconds = []
    if (effortSamples.length > 0) {
      cumulativeEffortSeconds.push(0)
      for (let i = 1; i < effortSamples.length; i += 1) {
        const dtSecs = Math.max(0, (effortSamples[i].ts - effortSamples[i - 1].ts) / 1000)
        const scaleAvg = (effortScale(effortSamples[i - 1].hr) + effortScale(effortSamples[i].hr)) / 2
        cumulativeEffortSeconds.push(cumulativeEffortSeconds[i - 1] + dtSecs * scaleAvg)
      }
    }

    const effortSecondsAt = (ts) => {
      if (!Number.isFinite(ts) || effortSamples.length === 0) return null
      if (effortSamples.length === 1) {
        return ((ts - effortSamples[0].ts) / 1000) * effortScale(effortSamples[0].hr)
      }

      if (ts <= effortSamples[0].ts) {
        return ((ts - effortSamples[0].ts) / 1000) * effortScale(effortSamples[0].hr)
      }

      const lastIdx = effortSamples.length - 1
      if (ts >= effortSamples[lastIdx].ts) {
        return cumulativeEffortSeconds[lastIdx]
          + ((ts - effortSamples[lastIdx].ts) / 1000) * effortScale(effortSamples[lastIdx].hr)
      }

      let lo = 0
      let hi = lastIdx
      while (lo < hi - 1) {
        const mid = (lo + hi) >> 1
        if (effortSamples[mid].ts <= ts) lo = mid
        else hi = mid
      }

      const segStartTs = effortSamples[lo].ts
      const segDt = Math.max(1, (effortSamples[hi].ts - segStartTs) / 1000)
      const frac = Math.max(0, Math.min(1, (ts - segStartTs) / 1000 / segDt))
      const segScale = (effortScale(effortSamples[lo].hr) + effortScale(effortSamples[hi].hr)) / 2
      return cumulativeEffortSeconds[lo] + (segDt * frac * segScale)
    }

    return glucoseChartRows.map((row) => {
      const rowTs = Date.parse(row.timestamp || '')
      if (!Number.isFinite(rowTs)) return { ...row, bolus_decay: null }

      const rowEffortSecs = effortSecondsAt(rowTs)
      if (rowEffortSecs == null) return { ...row, bolus_decay: null }

      let activeUnits = 0
      for (const event of bolusEvents) {
        const eventEffortSecs = effortSecondsAt(event.ts)
        if (eventEffortSecs == null) continue
        const dtEffortSecs = rowEffortSecs - eventEffortSecs
        if (dtEffortSecs < 0 || dtEffortSecs > durationSecs) continue
        activeUnits += event.units * iob(dtEffortSecs)
      }

      return {
        ...row,
        bolus_decay: activeUnits > 0.01 ? Math.min(activeUnits, INSULIN_AXIS_MAX) : null,
      }
    })
  }, [glucoseChartRows, bolusRows, insulinProfile, activityRows, effortHrReference, effortScaleMin, effortScaleMax])

  const weatherChartRows = useMemo(() => {
    const byDistance = new Map()
    weatherRows.forEach((row) => {
      byDistance.set(row.distance_km, {
        distance_km: row.distance_km,
        temp_c: row.temp_c,
        apparent_c: row.apparent_c,
        wind_kph: row.wind_kph,
        headwind_kph: null,
      })
    })
    windRows.forEach((row) => {
      const key = row.distance_km
      const existing = byDistance.get(key) || {
        distance_km: key,
        temp_c: null,
        apparent_c: null,
        wind_kph: null,
        headwind_kph: null,
      }
      existing.headwind_kph = row.headwind_kph
      byDistance.set(key, existing)
    })
    return Array.from(byDistance.values()).sort((a, b) => a.distance_km - b.distance_km)
  }, [weatherRows, windRows])

  const weatherChartRowsSmoothed = useMemo(
    () => bucketSeries(weatherChartRows, 120),
    [weatherChartRows],
  )

  const distanceDomain = useMemo(() => {
    if (payload.chartDistanceStart != null && payload.chartDistanceEnd != null) {
      return [payload.chartDistanceStart, payload.chartDistanceEnd]
    }

    return buildDistanceDomain([
      ...activityRows.map((row) => row.distance_km),
      ...glucoseChartRows.map((row) => row.distance_km),
      ...weatherChartRows.map((row) => row.distance_km),
      ...stressRows.map((row) => row.distance_km),
    ])
  }, [payload.chartDistanceStart, payload.chartDistanceEnd, activityRows, glucoseChartRows, weatherChartRows, stressRows])

  const distanceTicks = useMemo(() => buildDistanceTicks(distanceDomain), [distanceDomain])

  const trendChartRows = useMemo(
    () => trendRows.map((row, index) => ({ ...row, x_index: index })),
    [trendRows],
  )

  const trendDomain = useMemo(() => {
    if (!trendChartRows.length) return [0, 1]
    const min = trendChartRows[0].x_index
    const max = trendChartRows[trendChartRows.length - 1].x_index
    const span = Math.max(max - min, 1)
    const buffer = span * 0.05
    return [min - buffer, max + buffer]
  }, [trendChartRows])

  const trendTicks = useMemo(() => trendChartRows.map((row) => row.x_index), [trendChartRows])

  const trendTickLabelByIndex = useMemo(
    () => new Map(trendChartRows.map((row) => [row.x_index, row.date || 'n/a'])),
    [trendChartRows],
  )

  const phaseRows = useMemo(() => payload?.phaseAnalytics?.phases || [], [payload?.phaseAnalytics])
  const intensityRows = useMemo(() => payload?.intensityAnalytics || [], [payload?.intensityAnalytics])
  const hasBgInsights = glucoseRows.length > 0

  const insightLines = useMemo(() => {
    const lines = []

    if (metrics.weather_stress_band) {
      lines.push(`Weather stress is ${metrics.weather_stress_band.toLowerCase()} (${fmt(metrics.weather_stress_score, 0)}).`)
    }

    if (stressSummary.band) {
      lines.push(`Cardiac decoupling signal is ${String(stressSummary.band).toLowerCase()} (${fmt(stressSummary.score, 0)}).`)
    }

    if (hasBgInsights) {
      const during = phaseRows.find((p) => p.key === 'during')
      if (during?.slope_per_hour != null) {
        const trend = Number(during.slope_per_hour) < -0.2 ? 'falling' : Number(during.slope_per_hour) > 0.2 ? 'rising' : 'stable'
        lines.push(`During-walk glucose trend is ${trend} (${fmt(during.slope_per_hour, 2)} mmol/L per hour).`)
      }

      const topZone = [...intensityRows]
        .filter((row) => row?.minutes != null)
        .sort((a, b) => Number(b.minutes) - Number(a.minutes))[0]
      if (topZone?.zone) {
        lines.push(`Most time was spent in HR zone ${topZone.zone} (${fmt(topZone.minutes, 0, ' min')}).`)
      }
    }

    return lines
  }, [metrics.weather_stress_band, metrics.weather_stress_score, stressSummary.band, stressSummary.score, hasBgInsights, phaseRows, intensityRows])

  if (loading) {
    return (
      <section className="card">
        <div className="analysis-toolbar">
          <button className="btn-secondary" onClick={onBack}>Back to walks</button>
        </div>
        <p className="empty">Loading analysis...</p>
      </section>
    )
  }

  if (error) {
    return (
      <section className="card">
        <div className="analysis-toolbar">
          <button className="btn-secondary" onClick={onBack}>Back to walks</button>
        </div>
        <p className="msg-error">{error}</p>
      </section>
    )
  }

  if (!data) {
    return null
  }

  const walkTitle = data.name || data.date || walkId

  return (
    <section className="analysis-view">
      <section className="card analysis-header-card">
        <div className="analysis-toolbar">
          <button className="btn-secondary" onClick={onBack}>Back to walks</button>
        </div>
        <h2>{walkTitle}</h2>
        <p className="analysis-subtitle">{data.date}</p>
      </section>

      <section className="analysis-stats-grid">
        <Stat label="Distance" value={fmt(metrics.distance_km, 2, ' km')} />
        <Stat label="Duration" value={fmt(metrics.duration_h, 2, ' h')} />
        <Stat label="Average HR" value={fmt(metrics.avg_hr, 0, ' bpm')} />
        <Stat label="BG Delta" value={fmt(metrics.bg_delta, 2, ' mmol/L')} />
        <Stat label="Time in range" value={fmt(metrics.tir_pct, 0, '%')} />
        <Stat
          label="Weather stress"
          value={`${fmt(metrics.weather_stress_score, 0)}${metrics.weather_stress_band ? ` (${metrics.weather_stress_band})` : ''}`}
        />
      </section>

      <ChartCard title="Route Map" empty={!mapTrack.length ? 'No route map data available.' : null}>
        <RouteMap track={mapTrack} hourMarkers={mapHourMarkers} />
      </ChartCard>

      <section className="analysis-chart-grid">
        <ChartCard title="Activity Timeline" empty={!activityRows.length ? 'No activity series available.' : null}>
          <ResponsiveContainer width="100%" height={290}>
            <LineChart data={activityRows} syncId="distanceSync" syncMethod="value" margin={DISTANCE_CHART_MARGIN}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5ebf1" />
              <XAxis dataKey="distance_km" type="number" domain={distanceDomain} ticks={distanceTicks} allowDataOverflow tickFormatter={formatDistanceTick} unit=" km" />
              <YAxis yAxisId="alt" orientation="left" width={DISTANCE_LEFT_Y_WIDTH} tickFormatter={(v) => num(v, 0)} unit="m" />
              <YAxis yAxisId="hr" orientation="right" width={DISTANCE_RIGHT_Y_WIDTH} tickFormatter={(v) => num(v, 0)} unit=" bpm" />
              <Tooltip labelFormatter={(v) => `${num(v, 2)} km`} />
              <Legend />
              <Line yAxisId="alt" type="monotone" dataKey="altitude_m" name="Altitude" stroke="#5a6f85" dot={false} strokeWidth={2} />
              <Line yAxisId="hr" type="monotone" dataKey="hr" name="HR" stroke="#ef5b0c" dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Glucose and Insulin" empty={!glucoseChartRows.length ? 'No BG/insulin points available.' : null}>
          <div className="insulin-controls-panel">
            {insulinProfile && (
              <div className="insulin-profile-badge">
                <span className="insulin-profile-name">{insulinProfile.name}</span>
                <span className="insulin-profile-detail">
                  onset {insulinProfile.onsetMins} min · early action {insulinProfile.earlyActionMins} min ·
                  peak {num(insulinProfile.peakMinHours, 1)}–{num(insulinProfile.peakMaxHours, 1)} h ·
                  duration {num(insulinProfile.durationMinHours, 1)}–{num(insulinProfile.durationMaxHours, 1)} h
                </span>
              </div>
            )}
            <div className="insulin-decay-control">
              <label htmlFor="effort-hr-reference-range">HR reference: {num(effortHrReference, 0)} bpm</label>
              <input
                id="effort-hr-reference-range"
                type="range"
                min={EFFORT_HR_REFERENCE_MIN}
                max={EFFORT_HR_REFERENCE_MAX}
                step={5}
                value={effortHrReference}
                onChange={(e) => setEffortHrReference(Number(e.target.value))}
              />
            </div>
            <div className="insulin-decay-control">
              <label htmlFor="effort-scale-min-range">Effort floor: {num(effortScaleMin, 2)}x</label>
              <input
                id="effort-scale-min-range"
                type="range"
                min={EFFORT_SCALE_MIN_MIN}
                max={Math.min(EFFORT_SCALE_MIN_MAX, effortScaleMax - 0.1)}
                step={0.05}
                value={effortScaleMin}
                onChange={(e) => setEffortScaleMin(Number(e.target.value))}
              />
            </div>
            <div className="insulin-decay-control">
              <label htmlFor="effort-scale-max-range">Effort ceiling: {num(effortScaleMax, 2)}x</label>
              <input
                id="effort-scale-max-range"
                type="range"
                min={Math.max(EFFORT_SCALE_MAX_MIN, effortScaleMin + 0.1)}
                max={EFFORT_SCALE_MAX_MAX}
                step={0.05}
                value={effortScaleMax}
                onChange={(e) => setEffortScaleMax(Number(e.target.value))}
              />
            </div>
            <p className="insulin-controls-note">These analytics controls are saved in this browser. Derived analytics remain computed from source data on demand.</p>
          </div>
          <ResponsiveContainer width="100%" height={290}>
            <ComposedChart
              data={glucoseInsulinChartRows}
              syncId="distanceSync"
              syncMethod="value"
              margin={DISTANCE_CHART_MARGIN}
              onMouseMove={(state) => {
                if (state?.activePayload?.[0]?.payload?.distance_km != null) {
                  setActiveDistance(state.activePayload[0].payload.distance_km)
                }
              }}
              onMouseLeave={() => setActiveDistance(null)}
            >
              <CartesianGrid strokeDasharray="3 3" stroke="#e5ebf1" />
              <XAxis dataKey="distance_km" type="number" domain={distanceDomain} ticks={distanceTicks} allowDataOverflow tickFormatter={formatDistanceTick} unit=" km" />
              <YAxis yAxisId="bg" orientation="left" width={DISTANCE_LEFT_Y_WIDTH} tickFormatter={(v) => num(v, 1)} unit=" mmol/L" />
              <YAxis yAxisId="ins" orientation="right" width={DISTANCE_RIGHT_Y_WIDTH} domain={[0, INSULIN_AXIS_MAX]} tickFormatter={(v) => num(v, 1)} unit=" U/h" />
              <Tooltip
                labelFormatter={(v) => `${num(v, 2)} km`}
                formatter={(value, name, item) => {
                  if (name === 'Bolus') {
                    return [`${num(item?.payload?.bolus, 1)} U`, name]
                  }
                  if (name === 'Bolus decay') {
                    return [`${num(value, 2)} U`, name]
                  }
                  if (name === 'Basal rate') {
                    return [`${num(value, 2)} U/h`, name]
                  }
                  return [value, name]
                }}
              />
              <Legend />
              <Line yAxisId="bg" type="monotone" dataKey="bg" name="BG" stroke="#7d00b8" dot={false} strokeWidth={2} connectNulls />
              <Area yAxisId="ins" type="stepAfter" dataKey="basal" name="Basal rate" stroke="#00745a" fill="rgba(0,116,90,0.22)" connectNulls strokeWidth={1.8} />
              <Line yAxisId="ins" type="monotone" dataKey="bolus_decay" name="Bolus decay" stroke="#1f6feb" dot={false} strokeWidth={1.8} connectNulls />
              <Bar
                yAxisId="ins"
                dataKey="bolus_display"
                name="Bolus"
                fill="#003f91"
                stroke="#002f6b"
                strokeWidth={1}
                barSize={11}
                shape={(props) => <BolusBarShape {...props} activeDistance={activeDistance} />}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Weather Along Route" empty={!weatherChartRows.length ? 'No weather points available.' : null}>
          <ResponsiveContainer width="100%" height={290}>
            <ComposedChart data={weatherChartRowsSmoothed} syncId="distanceSync" syncMethod="value" margin={DISTANCE_CHART_MARGIN}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5ebf1" />
              <XAxis dataKey="distance_km" type="number" domain={distanceDomain} ticks={distanceTicks} allowDataOverflow tickFormatter={formatDistanceTick} unit=" km" />
              <YAxis yAxisId="temp" orientation="left" width={DISTANCE_LEFT_Y_WIDTH} domain={['dataMin - 1', 'dataMax + 1']} tickFormatter={(v) => num(v, 1)} unit="°C" />
              <YAxis yAxisId="wind" orientation="right" width={DISTANCE_RIGHT_Y_WIDTH} tickFormatter={(v) => num(v, 1)} unit=" km/h" />
              <Tooltip labelFormatter={(v) => `${num(v, 2)} km`} />
              <Legend />
              <Line yAxisId="temp" type="monotone" dataKey="temp_c" name="Temp" stroke="#007f8a" dot={false} connectNulls strokeWidth={2.6} />
              <Line yAxisId="temp" type="monotone" dataKey="apparent_c" name="Apparent" stroke="#7b8ea2" dot={false} connectNulls strokeDasharray="5 4" strokeWidth={1.8} />
              <Bar yAxisId="wind" dataKey="wind_kph" name="Wind" fill="rgba(239,91,12,0.45)" barSize={8} />
              <Line yAxisId="wind" type="monotone" dataKey="headwind_kph" name="Headwind" stroke="#c64200" dot={false} strokeWidth={2} />
            </ComposedChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Stress and Decoupling" empty={!stressRows.length ? 'No stress analytics series available.' : null}>
          <ResponsiveContainer width="100%" height={290}>
            <LineChart data={stressRows} syncId="distanceSync" syncMethod="value" margin={DISTANCE_CHART_MARGIN}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5ebf1" />
              <XAxis dataKey="distance_km" type="number" domain={distanceDomain} ticks={distanceTicks} allowDataOverflow tickFormatter={formatDistanceTick} unit=" km" />
              <YAxis yAxisId="hr" orientation="left" width={DISTANCE_LEFT_Y_WIDTH} tickFormatter={(v) => num(v, 0)} unit=" bpm" />
              <YAxis yAxisId="res" orientation="right" width={DISTANCE_RIGHT_Y_WIDTH} tickFormatter={(v) => num(v, 1)} unit=" bpm" />
              <Tooltip labelFormatter={(v) => `${num(v, 2)} km`} />
              <Legend />
              <Line yAxisId="hr" type="monotone" dataKey="hr" name="Observed HR" stroke="#ef5b0c" dot={false} strokeWidth={2} />
              <Line yAxisId="hr" type="monotone" dataKey="expected_hr" name="Expected HR" stroke="#5f7a96" dot={false} strokeDasharray="5 4" strokeWidth={2} />
              <Line yAxisId="res" type="monotone" dataKey="residual_bpm" name="Residual" stroke="#0a8f8f" dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </ChartCard>

        <ChartCard title="Decoupling Trend" empty={!trendRows.length ? 'No trend rows available yet.' : null}>
          <ResponsiveContainer width="100%" height={290}>
            <ComposedChart data={trendChartRows} margin={DISTANCE_CHART_MARGIN}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e5ebf1" />
              <XAxis
                dataKey="x_index"
                type="number"
                domain={trendDomain}
                ticks={trendTicks}
                allowDataOverflow
                tickFormatter={(v) => trendTickLabelByIndex.get(v) || ''}
              />
              <YAxis yAxisId="score" orientation="left" domain={[0, 100]} />
              <YAxis yAxisId="mins" orientation="right" />
              <Tooltip labelFormatter={(v) => trendTickLabelByIndex.get(v) || String(v)} />
              <Legend />
              <Line yAxisId="score" type="monotone" dataKey="score" name="Score" stroke="#0a8f8f" strokeWidth={2} />
              <Bar yAxisId="mins" dataKey="elevated_minutes" name="Elevated minutes" fill="rgba(239,91,12,0.45)" barSize={18} />
            </ComposedChart>
          </ResponsiveContainer>
        </ChartCard>
      </section>

      {hasBgInsights && (phaseRows.length > 0 || intensityRows.length > 0) && (
        <section className="card">
          <h2>Training Insights</h2>
          <div className="insights-charts-row">
            {phaseRows.length > 0 && (
              <div className="insights-chart-half">
                <ResponsiveContainer width="100%" height={220}>
                  <BarChart data={phaseRows} margin={{ top: 20, right: 16, left: 8, bottom: 8 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#2a2a2a" />
                    <XAxis dataKey="label" tick={{ fontSize: 11, fill: '#aaa' }} />
                    <YAxis tick={{ fontSize: 11, fill: '#aaa' }} unit=" mmol/L/h" width={70} />
                    <Tooltip formatter={(v) => [`${fmt(v, 2)} mmol/L per hour`, 'Slope']} />
                    <Bar dataKey="slope_per_hour" name="BG slope" label={{ position: 'top', fontSize: 10, fill: '#ccc', formatter: (v) => fmt(v, 2) }}>
                      {phaseRows.map((row, i) => {
                        const colors = ['#7986cb', '#9c27b0', '#26a69a']
                        return <Cell key={i} fill={colors[i % colors.length]} />
                      })}
                    </Bar>
                    <text x="50%" y={12} textAnchor="middle" fill="#ccc" fontSize={12}>Glucose Response By Phase</text>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            )}
            {intensityRows.length > 0 && (
              <div className="insights-chart-half">
                <ResponsiveContainer width="100%" height={220}>
                  <ComposedChart data={intensityRows} margin={{ top: 20, right: 50, left: 8, bottom: 8 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#2a2a2a" />
                    <XAxis dataKey="zone" tick={{ fontSize: 11, fill: '#aaa' }} />
                    <YAxis yAxisId="sd" orientation="left" tick={{ fontSize: 11, fill: '#aaa' }} label={{ value: 'BG SD (mmol/L)', angle: -90, position: 'insideLeft', offset: 10, style: { fontSize: 10, fill: '#aaa' } }} />
                    <YAxis yAxisId="slope" orientation="right" tick={{ fontSize: 11, fill: '#aaa' }} label={{ value: 'BG slope (mmol/L/h)', angle: 90, position: 'insideRight', offset: 12, style: { fontSize: 10, fill: '#aaa' } }} />
                    <Tooltip />
                    <Legend wrapperStyle={{ fontSize: 11 }} />
                    <Bar yAxisId="sd" dataKey="bg_std" name="BG SD" fill="#ef5b0c" barSize={24} />
                    <Line yAxisId="slope" type="monotone" dataKey="bg_slope_per_hour" name="BG Slope" stroke="#0a8f8f" strokeWidth={2} dot={{ r: 3 }} />
                    <text x="50%" y={12} textAnchor="middle" fill="#ccc" fontSize={12}>HR Zone vs Glucose Stability</text>
                  </ComposedChart>
                </ResponsiveContainer>
              </div>
            )}
          </div>
          {insightLines.length > 0 && (
            <div className="trend-table-wrap" style={{ marginTop: '0.8rem' }}>
              <table className="trend-table">
                <tbody>
                  {insightLines.map((line, idx) => (
                    <tr key={`insight-${idx}`}><td>{line}</td></tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {hasBgInsights ? (
        <section className="card">
          <h2>Phase And Intensity Breakdown</h2>
          {phaseRows.length ? (
            <div className="trend-table-wrap">
              <table className="trend-table">
                <thead>
                  <tr>
                    <th>Phase</th>
                    <th>Samples</th>
                    <th>Delta</th>
                    <th>Slope / h</th>
                  </tr>
                </thead>
                <tbody>
                  {phaseRows.map((row) => (
                    <tr key={row.key || row.label}>
                      <td>{row.label || row.key || 'n/a'}</td>
                      <td>{fmt(row.count, 0)}</td>
                      <td>{fmt(row.delta, 2, ' mmol/L')}</td>
                      <td>{fmt(row.slope_per_hour, 2, ' mmol/L')}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}

          {intensityRows.length ? (
            <div className="trend-table-wrap" style={{ marginTop: phaseRows.length ? '0.9rem' : 0 }}>
              <table className="trend-table">
                <thead>
                  <tr>
                    <th>HR Zone</th>
                    <th>Minutes</th>
                    <th>BG Volatility</th>
                    <th>Slope / h</th>
                  </tr>
                </thead>
                <tbody>
                  {intensityRows.map((row) => (
                    <tr key={row.zone || 'zone'}>
                      <td>{row.zone || 'n/a'}</td>
                      <td>{fmt(row.minutes, 0)}</td>
                      <td>{fmt(row.bg_std, 2)}</td>
                      <td>{fmt(row.bg_slope_per_hour, 2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : null}
        </section>
      ) : (
        <section className="card">
          <h2>Phase And Intensity Breakdown</h2>
          <p className="empty">BG-linked training insights are hidden because glucose data is missing for this walk.</p>
        </section>
      )}

      <section className="card">
        <h2>Stress Summary</h2>
        <div className="analysis-summary-row">
          <span>Decoupling score:</span>
          <strong>{fmt(stressSummary.score, 0)}{stressSummary.band ? ` (${stressSummary.band})` : ''}</strong>
        </div>
        <div className="analysis-summary-row">
          <span>Elevated minutes:</span>
          <strong>{fmt(stressSummary.elevated_minutes, 0, ' min')}</strong>
        </div>
        <div className="analysis-summary-row">
          <span>Max residual:</span>
          <strong>{fmt(stressSummary.max_residual_bpm, 1, ' bpm')}</strong>
        </div>
      </section>

      <section className="card">
        <h2>Recent Stress Trend</h2>
        {!trendRows.length ? (
          <p className="empty">No trend rows available yet.</p>
        ) : (
          <div className="trend-table-wrap">
            <table className="trend-table">
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Walk</th>
                  <th>Score</th>
                  <th>Band</th>
                  <th>Elevated min</th>
                </tr>
              </thead>
              <tbody>
                {trendRows.map((row) => (
                  <tr key={`${row.walk_id}-${row.start_time || row.date || ''}`}>
                    <td>{row.date || 'n/a'}</td>
                    <td>{row.name || row.walk_id || 'n/a'}</td>
                    <td>{fmt(row.score, 0)}</td>
                    <td>{row.band || 'n/a'}</td>
                    <td>{fmt(row.elevated_minutes, 0)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </section>
  )
}
