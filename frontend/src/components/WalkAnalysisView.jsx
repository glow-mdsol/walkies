import { useCallback, useEffect, useMemo, useState } from 'react'
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
  ReferenceLine,
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

function ChartCard({ title, description, children, empty }) {
  return (
    <section className="card chart-card">
      <div className="chart-card-header">
        <h2>{title}</h2>
        {description ? (
          <span className="chart-card-info" tabIndex={0} aria-label={description}>
            ?
            <span className="chart-card-tooltip" role="tooltip">{description}</span>
          </span>
        ) : null}
      </div>
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

function bgColor(bg) {
  if (bg == null || Number.isNaN(Number(bg))) return '#8b97a7'
  const g = Number(bg)
  if (g <= 3.9 || g >= 12.9) return '#be2f2f'
  if (g >= 4.0 && g <= 8.0) return '#1f8a5b'
  return '#d16a00'
}

function RouteMap({ track, hourMarkers, allowBgToggle = true, initialMode = 'hr', modeKey = 'default' }) {
  const [mapColorMode, setMapColorMode] = useState(initialMode === 'bg' ? 'bg' : 'hr')
  const effectiveMode = allowBgToggle ? mapColorMode : 'hr'

  const cleanTrack = useMemo(
    () => (track || [])
      .filter((p) => Array.isArray(p) && p.length >= 2 && p[0] != null && p[1] != null)
      .map((p) => ({
        lat: Number(p[0]),
        lon: Number(p[1]),
        hr: p[2] == null ? null : Number(p[2]),
        bg: p[3] == null ? null : Number(p[3]),
      })),
    [track],
  )

  const hasBgData = useMemo(
    () => cleanTrack.some((p) => p.bg != null && !Number.isNaN(p.bg)),
    [cleanTrack],
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
      bg: pt.bg,
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
      {allowBgToggle ? (
        <div className="route-map-controls" style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap', marginBottom: '0.5rem' }}>
          <>
            <label style={{ display: 'inline-flex', alignItems: 'center', gap: '0.35rem' }}>
              <input
                type="radio"
                name={`map-color-mode-${modeKey}`}
                value="hr"
                checked={mapColorMode === 'hr'}
                onChange={() => setMapColorMode('hr')}
              />
              HR colouring
            </label>
            <label style={{ display: 'inline-flex', alignItems: 'center', gap: '0.35rem' }}>
              <input
                type="radio"
                name={`map-color-mode-${modeKey}`}
                value="bg"
                checked={mapColorMode === 'bg'}
                onChange={() => setMapColorMode('bg')}
              />
              BG colouring
            </label>
          </>
        </div>
      ) : null}
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
            pathOptions={{
              color: effectiveMode === 'bg' ? bgColor(seg.bg) : hrColor(seg.hr),
              weight: 4,
              opacity: 0.9,
              dashArray: effectiveMode === 'bg' && seg.bg == null ? '6 6' : undefined,
            }}
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
        {effectiveMode === 'hr' ? (
          <>
            <span><i style={{ background: '#2f6fb0' }} />Easy HR</span>
            <span><i style={{ background: '#1f8a5b' }} />Steady HR</span>
            <span><i style={{ background: '#d16a00' }} />Hard HR</span>
            <span><i style={{ background: '#be2f2f' }} />Peak HR</span>
          </>
        ) : (
          <>
            <span><i style={{ background: '#1f8a5b' }} />In range BG</span>
            <span><i style={{ background: '#d16a00' }} />Mild deviation</span>
            <span><i style={{ background: '#be2f2f' }} />Hypo/Hyper risk</span>
            {!hasBgData ? <span><i style={{ background: '#8b97a7' }} />No BGM data trace</span> : null}
          </>
        )}
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
const INSULIN_MODEL_NORMAL = 'normal'
const INSULIN_MODEL_SMARTGUARD = 'smartguard'

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value))
}

function loadAnalyticsPrefs() {
  if (typeof window === 'undefined') {
    return {
      effortHrReference: EFFORT_HR_REFERENCE_DEFAULT,
      effortScaleMin: EFFORT_SCALE_MIN_DEFAULT,
      effortScaleMax: EFFORT_SCALE_MAX_DEFAULT,
      insulinModelMode: INSULIN_MODEL_NORMAL,
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
      insulinModelMode: parsed.insulinModelMode === INSULIN_MODEL_SMARTGUARD ? INSULIN_MODEL_SMARTGUARD : INSULIN_MODEL_NORMAL,
    }
  } catch {
    return {
      effortHrReference: EFFORT_HR_REFERENCE_DEFAULT,
      effortScaleMin: EFFORT_SCALE_MIN_DEFAULT,
      effortScaleMax: EFFORT_SCALE_MAX_DEFAULT,
      insulinModelMode: INSULIN_MODEL_NORMAL,
    }
  }
}

export default function WalkAnalysisView({ walkId, insulinProfile, apiFetch }) {
  const initialPrefs = useMemo(() => loadAnalyticsPrefs(), [])
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [loading, setLoading] = useState(false)
  const [activeDistance, setActiveDistance] = useState(null)
  const [effortHrReference, setEffortHrReference] = useState(initialPrefs.effortHrReference)
  const [effortScaleMin, setEffortScaleMin] = useState(initialPrefs.effortScaleMin)
  const [effortScaleMax, setEffortScaleMax] = useState(initialPrefs.effortScaleMax)
  const [analysisTab, setAnalysisTab] = useState('general')
  const [insulinModelMode, setInsulinModelMode] = useState(initialPrefs.insulinModelMode)
  const [showInsulinModelModal, setShowInsulinModelModal] = useState(false)
  const [shareLinks, setShareLinks] = useState([])
  const [shareBusy, setShareBusy] = useState(false)
  const [shareMessage, setShareMessage] = useState(null)

  const loadShareLinks = useCallback(() => {
    if (!walkId) return
    apiFetch(`/api/walks/${encodeURIComponent(walkId)}/share-links`)
      .then(async (res) => {
        if (!res.ok) {
          const payload = await res.json().catch(() => ({}))
          throw new Error(payload.detail || 'Failed to load share links')
        }
        return res.json()
      })
      .then((payload) => setShareLinks(payload.links || []))
      .catch(() => setShareLinks([]))
  }, [walkId, apiFetch])

  useEffect(() => {
    if (!walkId) return
    setLoading(true)
    setError(null)

    apiFetch(`/api/walks/${encodeURIComponent(walkId)}/analysis-data`)
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
  }, [walkId, apiFetch])

  useEffect(() => {
    loadShareLinks()
  }, [loadShareLinks])

  useEffect(() => {
    if (typeof window === 'undefined') return
    window.localStorage.setItem(ANALYTICS_PREFS_KEY, JSON.stringify({
      effortHrReference,
      effortScaleMin,
      effortScaleMax,
      insulinModelMode,
    }))
  }, [effortHrReference, effortScaleMin, effortScaleMax, insulinModelMode])

  const metrics = data?.metrics || {}
  const payload = data?.payload || {}
  const stressSummary = data?.payload?.stressAnalytics?.summary || {}
  const insulinStressSummary = data?.payload?.insulinStressEffect?.summary || {}
  const smartguardSummary = data?.payload?.smartguard?.summary || {}
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
  const smartguardRows = useMemo(
    () => (payload.smartguard?.events || []).filter((r) => r.distance_km != null),
    [payload.smartguard],
  )
  const smartguardMarkerRows = useMemo(
    () => smartguardRows.filter((r) => r.distance_km != null),
    [smartguardRows],
  )
  const predictedLowMarkerCount = useMemo(
    () => smartguardMarkerRows.filter((r) => r.state === 'predicted_low_suspend').length,
    [smartguardMarkerRows],
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

  const elevationHrBgChartRows = useMemo(() => {
    const byDistance = new Map()

    activityRows.forEach((row) => {
      if (row.distance_km == null) return
      byDistance.set(row.distance_km, {
        distance_km: row.distance_km,
        altitude_m: row.altitude_m ?? null,
        hr: row.hr ?? null,
        bg: null,
      })
    })

    glucoseRows.forEach((row) => {
      if (row.distance_km == null) return
      const existing = byDistance.get(row.distance_km) || {
        distance_km: row.distance_km,
        altitude_m: null,
        hr: null,
        bg: null,
      }
      existing.bg = row.bg ?? null
      byDistance.set(row.distance_km, existing)
    })

    return Array.from(byDistance.values()).sort((a, b) => a.distance_km - b.distance_km)
  }, [activityRows, glucoseRows])

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

    const smartguardEvents = smartguardRows
      .map((row) => ({ ts: Date.parse(row.timestamp || ''), state: row.state || 'other' }))
      .filter((row) => Number.isFinite(row.ts))
      .sort((a, b) => a.ts - b.ts)

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

    const smartguardFactorAt = (ts) => {
      if (insulinModelMode !== INSULIN_MODEL_SMARTGUARD || smartguardEvents.length === 0 || !Number.isFinite(ts)) {
        return 1.0
      }

      let latestState = null
      for (let i = 0; i < smartguardEvents.length; i += 1) {
        if (smartguardEvents[i].ts > ts) break
        latestState = smartguardEvents[i].state
      }

      if (latestState === 'predicted_low_suspend') return 0.20
      if (latestState === 'suspend') return 0.35
      if (latestState === 'resume') return 1.0
      return 1.0
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

      activeUnits *= smartguardFactorAt(rowTs)

      return {
        ...row,
        bolus_decay: activeUnits > 0.01 ? Math.min(activeUnits, INSULIN_AXIS_MAX) : null,
      }
    })
  }, [glucoseChartRows, bolusRows, insulinProfile, activityRows, effortHrReference, effortScaleMin, effortScaleMax, smartguardRows, insulinModelMode])

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
        <p className="empty">Loading analysis...</p>
      </section>
    )
  }

  if (error) {
    return (
      <section className="card">
        <p className="msg-error" style={{ padding: '1rem' }}>{error}</p>
      </section>
    )
  }

  if (!data) {
    return null
  }

  const walkTitle = data.name || data.date || walkId

  const distStr = metrics.distance_km != null ? `${fmt(metrics.distance_km, 2)} km` : null
  const durStr = metrics.duration_h != null ? `${fmt(metrics.duration_h, 1)} h` : null
  const subtitleParts = [data.date, distStr, durStr].filter(Boolean)

  const createShareLink = async () => {
    if (!walkId) return
    setShareBusy(true)
    setShareMessage(null)
    try {
      const res = await apiFetch(`/api/walks/${encodeURIComponent(walkId)}/share-links`, { method: 'POST' })
      const payload = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(payload.detail || 'Failed to create share link')
      const absolute = `${window.location.origin}${payload.share_path}`
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(absolute)
        setShareMessage('Share link created and copied to clipboard.')
      } else {
        setShareMessage(`Share link created: ${absolute}`)
      }
      loadShareLinks()
    } catch (err) {
      setShareMessage(err.message || 'Failed to create share link')
    } finally {
      setShareBusy(false)
    }
  }

  const revokeShareLink = async (token) => {
    if (!token) return
    setShareBusy(true)
    setShareMessage(null)
    try {
      const res = await apiFetch(`/api/share-links/${encodeURIComponent(token)}`, { method: 'DELETE' })
      const payload = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(payload.detail || 'Failed to revoke share link')
      setShareMessage('Share link revoked.')
      loadShareLinks()
    } catch (err) {
      setShareMessage(err.message || 'Failed to revoke share link')
    } finally {
      setShareBusy(false)
    }
  }

  const copyShareLink = async (token) => {
    const absolute = `${window.location.origin}/api/share/${token}/analysis-data`
    try {
      if (navigator?.clipboard?.writeText) {
        await navigator.clipboard.writeText(absolute)
        setShareMessage('Share link copied to clipboard.')
      } else {
        setShareMessage(absolute)
      }
    } catch {
      setShareMessage(absolute)
    }
  }

  return (
    <section className="analysis-view">
      <section className="card analysis-header-card">
        <h2 style={{ fontSize: '1.4rem', fontWeight: 700, letterSpacing: 'normal', textTransform: 'none', color: 'var(--text)', marginBottom: '0.35rem' }}>
          {walkTitle}
        </h2>
        <p className="analysis-subtitle">{subtitleParts.join(' · ')}</p>
        <div style={{ marginTop: '0.7rem', display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
          <button className="btn-secondary" type="button" onClick={createShareLink} disabled={shareBusy}>
            {shareBusy ? 'Working…' : 'Create Share Link'}
          </button>
          <span className="analysis-subtitle" style={{ fontSize: '0.8rem' }}>
            Shared links are public and do not require login.
          </span>
        </div>
        {shareMessage && (
          <p className={shareMessage.toLowerCase().includes('failed') ? 'msg-error' : 'analysis-subtitle'} style={{ marginTop: '0.55rem' }}>
            {shareMessage}
          </p>
        )}
        {shareLinks.length > 0 && (
          <div className="trend-table-wrap" style={{ marginTop: '0.7rem' }}>
            <table className="trend-table">
              <thead>
                <tr>
                  <th>Created</th>
                  <th>Status</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {shareLinks.map((link) => {
                  const active = !link.revoked_at
                  return (
                    <tr key={link.token}>
                      <td>{link.created_at ? new Date(link.created_at).toLocaleString() : 'n/a'}</td>
                      <td>{active ? 'Active' : 'Revoked'}</td>
                      <td>
                        <div style={{ display: 'flex', gap: '0.4rem', flexWrap: 'wrap' }}>
                          <button className="btn-secondary" type="button" onClick={() => copyShareLink(link.token)}>
                            Copy
                          </button>
                          {active ? (
                            <button className="btn-danger" type="button" onClick={() => revokeShareLink(link.token)}>
                              Revoke
                            </button>
                          ) : null}
                        </div>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <div className="analysis-tabs home-tabs">
        <button className={`home-tab ${analysisTab === 'general' ? 'home-tab--active' : ''}`} onClick={() => setAnalysisTab('general')} type="button">
          General
        </button>
        <button className={`home-tab ${analysisTab === 'diabetes' ? 'home-tab--active' : ''}`} onClick={() => setAnalysisTab('diabetes')} type="button">
          Diabetes
        </button>
      </div>

      {analysisTab === 'general' ? (
        <>
          <section className="analysis-stats-grid">
            <Stat label="Distance" value={fmt(metrics.distance_km, 2, ' km')} />
            <Stat label="Duration" value={fmt(metrics.duration_h, 2, ' h')} />
            <Stat label="Average HR" value={fmt(metrics.avg_hr, 0, ' bpm')} />
            <Stat
              label="Weather stress"
              value={`${fmt(metrics.weather_stress_score, 0)}${metrics.weather_stress_band ? ` (${metrics.weather_stress_band})` : ''}`}
            />
          </section>

          <ChartCard
            title="Route Map"
            description="Shows the GPS route of the walk, coloured by heart-rate intensity, with hourly position markers where available."
            empty={!mapTrack.length ? 'No route map data available.' : null}
          >
            <RouteMap track={mapTrack} hourMarkers={mapHourMarkers} allowBgToggle={false} modeKey="general" />
          </ChartCard>

          <section className="analysis-chart-grid">
            <ChartCard
              title="Activity Timeline"
              description="Plots altitude and heart rate against distance so you can see how terrain and effort changed across the walk."
              empty={!activityRows.length ? 'No activity series available.' : null}
            >
              <ResponsiveContainer width="100%" height={290}>
                <LineChart data={activityRows} syncId="distanceSync" syncMethod="value" margin={DISTANCE_CHART_MARGIN}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5ebf1" />
                  <XAxis dataKey="distance_km" type="number" domain={distanceDomain} ticks={distanceTicks} allowDataOverflow tickFormatter={formatDistanceTick} unit=" km" />
                  <YAxis yAxisId="alt" orientation="left" width={DISTANCE_LEFT_Y_WIDTH} tickFormatter={(v) => num(v, 0)} unit="m" />
                  <YAxis yAxisId="hr" orientation="right" width={DISTANCE_RIGHT_Y_WIDTH} tickFormatter={(v) => num(v, 0)} unit=" bpm" />
                  <Tooltip
                    labelFormatter={(v) => `${num(v, 2)} km`}
                    formatter={(value, name) => {
                      if (value == null || Number.isNaN(Number(value))) return ['n/a', name]
                      if (name === 'HR') return [`${num(value, 0)} bpm`, name]
                      if (name === 'Altitude') return [`${num(value, 0)} m`, name]
                      return [value, name]
                    }}
                  />
                  <Legend />
                  <Line yAxisId="alt" type="monotone" dataKey="altitude_m" name="Altitude" stroke="#5a6f85" dot={false} strokeWidth={2} />
                  <Line yAxisId="hr" type="monotone" dataKey="hr" name="HR" stroke="#ef5b0c" dot={false} strokeWidth={2} />
                </LineChart>
              </ResponsiveContainer>
            </ChartCard>

            <ChartCard
              title="Weather Along Route"
              description="Shows temperature, apparent temperature, wind speed, and headwind exposure over the course of the route."
              empty={!weatherChartRows.length ? 'No weather points available.' : null}
            >
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

            <ChartCard
              title="Stress and Decoupling"
              description="Compares observed heart rate with expected heart rate and highlights the residual gap as a proxy for physiological strain."
              empty={!stressRows.length ? 'No stress analytics series available.' : null}
            >
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

            <ChartCard
              title="Decoupling Trend"
              description="Summarises how decoupling scores and elevated-strain minutes change across recent walks."
              empty={!trendRows.length ? 'No trend rows available yet.' : null}
            >
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
        </>
      ) : (
        <>
          <section className="analysis-stats-grid">
            <Stat label="BG Delta" value={fmt(metrics.bg_delta, 2, ' mmol/L')} />
            <Stat label="Time in range" value={fmt(metrics.tir_pct, 0, '%')} />
            <Stat
              label="Insulin stress"
              value={`${fmt(insulinStressSummary.stress_multiplier, 2, 'x')}${insulinStressSummary.band ? ` (${insulinStressSummary.band})` : ''}`}
            />
            <Stat
              label="SmartGuard"
              value={smartguardSummary.any_activity ? `${fmt(smartguardSummary.predicted_low_suspend_count, 0)} predicted-low` : 'No events'}
            />
          </section>

          <ChartCard
            title="Route Map"
            description="Shows the GPS route with both heart-rate and glucose colouring modes."
            empty={!mapTrack.length ? 'No route map data available.' : null}
          >
            <RouteMap track={mapTrack} hourMarkers={mapHourMarkers} allowBgToggle modeKey="diabetes" />
          </ChartCard>

          <section className="analysis-chart-grid">
            <ChartCard
              title="Elevation, HR, and BG"
              description="Combines altitude, heart rate, and glucose so you can spot where climbs and effort coincide with glucose movement."
              empty={!elevationHrBgChartRows.length || !elevationHrBgChartRows.some((row) => row.bg != null) ? 'No combined elevation/HR/BG points available.' : null}
            >
              <ResponsiveContainer width="100%" height={290}>
                <LineChart data={elevationHrBgChartRows} syncId="distanceSync" syncMethod="value" margin={DISTANCE_CHART_MARGIN}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e5ebf1" />
                  <XAxis dataKey="distance_km" type="number" domain={distanceDomain} ticks={distanceTicks} allowDataOverflow tickFormatter={formatDistanceTick} unit=" km" />
                  <YAxis yAxisId="alt" orientation="left" width={DISTANCE_LEFT_Y_WIDTH} tickFormatter={(v) => num(v, 0)} unit=" m" />
                  <YAxis yAxisId="hr" orientation="right" width={DISTANCE_RIGHT_Y_WIDTH} tickFormatter={(v) => num(v, 0)} unit=" bpm" />
                  <YAxis yAxisId="bg" orientation="right" width={0} hide tickFormatter={(v) => num(v, 1)} unit=" mmol/L" />
                  <Tooltip labelFormatter={(v) => `${num(v, 2)} km`} />
                  <Legend />
                  <Line yAxisId="alt" type="monotone" dataKey="altitude_m" name="Altitude" stroke="#5a6f85" dot={false} strokeWidth={2} connectNulls />
                  <Line yAxisId="hr" type="monotone" dataKey="hr" name="HR" stroke="#ef5b0c" dot={false} strokeWidth={2} connectNulls />
                  <Line yAxisId="bg" type="monotone" dataKey="bg" name="BG" stroke="#7d00b8" dot={false} strokeWidth={2} connectNulls />
                </LineChart>
              </ResponsiveContainer>
            </ChartCard>

        <ChartCard
          title="Glucose and Insulin"
          description="Overlays glucose readings, basal insulin, recorded boluses, and estimated active bolus insulin across the route distance."
          empty={!glucoseChartRows.length ? 'No BG/insulin points available.' : null}
        >
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
              <label>Insulin model: {insulinModelMode === INSULIN_MODEL_SMARTGUARD ? 'SmartGuard-adjusted' : 'Normal basal/bolus'}</label>
              <button className="btn-secondary" type="button" onClick={() => setShowInsulinModelModal(true)}>Change model</button>
            </div>
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

          {!!smartguardMarkerRows.length && (
            <div className="insulin-marker-legend" aria-label="SmartGuard markers">
              <span><i className="marker marker-predicted" />SmartGuard predicted-low suspend</span>
              <span><i className="marker marker-suspend" />SmartGuard suspend</span>
              <span><i className="marker marker-resume" />SmartGuard resume</span>
            </div>
          )}

          {showInsulinModelModal && (
            <div className="modal-overlay" role="dialog" aria-modal="true" aria-label="Select insulin model">
              <div className="modal-panel">
                <h3>Select Insulin Model</h3>
                <p className="modal-copy">
                  Normal mode uses the effort-adjusted bolus decay model. SmartGuard-adjusted mode applies suspend-state dampening when SmartGuard suspend events are present.
                </p>
                <label className="modal-option">
                  <input
                    type="radio"
                    name="insulin-model-mode"
                    checked={insulinModelMode === INSULIN_MODEL_NORMAL}
                    onChange={() => setInsulinModelMode(INSULIN_MODEL_NORMAL)}
                  />
                  <span>Normal basal/bolus model</span>
                </label>
                <label className="modal-option">
                  <input
                    type="radio"
                    name="insulin-model-mode"
                    checked={insulinModelMode === INSULIN_MODEL_SMARTGUARD}
                    onChange={() => setInsulinModelMode(INSULIN_MODEL_SMARTGUARD)}
                  />
                  <span>SmartGuard-adjusted model</span>
                </label>
                <div className="modal-actions">
                  <button className="btn-primary" type="button" onClick={() => setShowInsulinModelModal(false)}>Apply</button>
                </div>
              </div>
            </div>
          )}

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
              {smartguardMarkerRows.map((row, idx) => {
                const state = row.state || 'other'
                const stroke = state === 'predicted_low_suspend'
                  ? '#be2f2f'
                  : state === 'suspend'
                    ? '#d16a00'
                    : state === 'resume'
                      ? '#1f8a5b'
                      : '#7b8ea2'
                const dash = state === 'predicted_low_suspend' ? '1 0' : '5 4'
                const showPredictedLabel = state === 'predicted_low_suspend' && (
                  predictedLowMarkerCount <= 3
                  || (activeDistance != null && Math.abs(Number(row.distance_km) - Number(activeDistance)) <= 0.08)
                )
                const label = state === 'predicted_low_suspend'
                  && showPredictedLabel
                  ? {
                    value: 'PLGM',
                    position: 'insideTop',
                    fill: '#be2f2f',
                    fontSize: 10,
                    fontWeight: 700,
                  }
                  : undefined
                return (
                  <ReferenceLine
                    key={`smartguard-marker-${idx}`}
                    x={row.distance_km}
                    yAxisId="ins"
                    stroke={stroke}
                    strokeWidth={1.7}
                    strokeDasharray={dash}
                    label={label}
                    ifOverflow="extendDomain"
                  />
                )
              })}
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

      {analysisTab === 'general' ? (
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
      ) : (
        <section className="card">
          <h2>Diabetes Summary</h2>
          <div className="analysis-summary-row">
            <span>Insulin stress multiplier:</span>
            <strong>{fmt(insulinStressSummary.stress_multiplier, 2, 'x')}{insulinStressSummary.band ? ` (${insulinStressSummary.band})` : ''}</strong>
          </div>
          <div className="analysis-summary-row">
            <span>IOB overlap:</span>
            <strong>{fmt(insulinStressSummary.overlap_minutes, 0, ' min')}</strong>
          </div>
          <div className="analysis-summary-row">
            <span>Peak IOB proxy:</span>
            <strong>{fmt(insulinStressSummary.peak_iob_units_proxy, 2, ' U')}</strong>
          </div>
          <div className="analysis-summary-row">
            <span>SmartGuard predicted-low suspend:</span>
            <strong>{fmt(smartguardSummary.predicted_low_suspend_count, 0)}</strong>
          </div>
          <div className="analysis-summary-row">
            <span>SmartGuard suspend/resume:</span>
            <strong>{fmt(smartguardSummary.suspend_count, 0)} / {fmt(smartguardSummary.resume_count, 0)}</strong>
          </div>
        </section>
      )}

        </>
      )}

      {data.files?.length > 0 && (
        <section className="card">
          <h2>Files</h2>
          <ul className="file-list">
            {data.files.map(f => (
              <li key={f}>
                <span className="file-icon">{f.endsWith('.fit') ? '🏃' : f.endsWith('.csv') ? '📊' : f.endsWith('.gpx') ? '🗺️' : '📄'}</span>
                {f}
              </li>
            ))}
          </ul>
        </section>
      )}

      {analysisTab === 'general' && (
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
      )}
    </section>
  )
}
