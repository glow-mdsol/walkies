import { useCallback, useEffect, useMemo, useState } from 'react'
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

function buildIobFn(profile) {
  const onset_s = profile.onsetMins * 60
  const earlyEnd_s = onset_s + profile.earlyActionMins * 60
  const peak_s = ((profile.peakMinHours + profile.peakMaxHours) / 2) * 3600
  const duration_s = ((profile.durationMinHours + profile.durationMaxHours) / 2) * 3600

  function activity(t) {
    if (t <= onset_s || t >= duration_s) return 0
    const earlySpan = earlyEnd_s - onset_s
    const riseSpan = peak_s - earlyEnd_s
    const fallSpan = duration_s - peak_s
    if (earlySpan <= 0 || fallSpan <= 0) {
      if (peak_s <= onset_s) return 0
      if (t <= peak_s) return (t - onset_s) / (peak_s - onset_s)
      return (duration_s - t) / (duration_s - peak_s)
    }
    if (t <= earlyEnd_s) return 0.5 * (t - onset_s) / earlySpan
    if (riseSpan > 0 && t <= peak_s) return 0.5 + 0.5 * (t - earlyEnd_s) / riseSpan
    return (duration_s - t) / fallSpan
  }

  const N = 600
  const dt = duration_s / N
  const acts = Array.from({ length: N + 1 }, (_, i) => activity(i * dt))
  const suffix = new Float64Array(N + 2)
  for (let i = N - 1; i >= 0; i--) {
    suffix[i] = suffix[i + 1] + (acts[i] + acts[i + 1]) * 0.5 * dt
  }
  const totalArea = suffix[0]
  if (totalArea < 1e-9) return () => 0

  return function iob(t_s) {
    if (t_s <= 0) return 1
    if (t_s >= duration_s) return 0
    const idxF = t_s / dt
    const idx = Math.min(N - 1, Math.floor(idxF))
    const frac = idxF - idx
    const partialArea = frac * (acts[idx] + acts[idx + 1]) * 0.5 * dt
    return (suffix[idx] - partialArea) / totalArea
  }
}

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

export default function WalkAnalysisView({ walkId, insulinProfile, apiFetch, shareToken, onRenamed }) {
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
  const [isEditingName, setIsEditingName] = useState(false)
  const [editedName, setEditedName] = useState('')
  const [rating, setRating] = useState(3)

  const loadShareLinks = useCallback(() => {
    if (!walkId || shareToken || !apiFetch) return
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
  }, [walkId, shareToken, apiFetch])

  useEffect(() => {
    if (!walkId && !shareToken) return
    setLoading(true)
    setError(null)

    const fetchPromise = shareToken
      ? fetch(`/api/share/${encodeURIComponent(shareToken)}/analysis-data`)
      : apiFetch(`/api/walks/${encodeURIComponent(walkId)}/analysis-data`)

    fetchPromise
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
  }, [walkId, shareToken, apiFetch])

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
  const userSettings = data?.user_settings || {}
  const userType = userSettings.user_type || (userSettings.insulin_type === 'Pump' ? 'diabetic_pump' : userSettings.insulin_type === 'MDI' ? 'diabetic_mdi' : 'non_diabetic')

  const hasCGM = userType === 'diabetic_pump' || userType === 'diabetic_mdi' || userType === 'non_diabetic_cgm'
  const hasInsulin = userType === 'diabetic_pump' || userType === 'diabetic_mdi'
  const hasPump = userType === 'diabetic_pump'

  useEffect(() => {
    if (!hasCGM && analysisTab !== 'general') {
      setAnalysisTab('general')
    }
  }, [hasCGM, analysisTab])

  useEffect(() => {
    if (data?.name != null) {
      setEditedName(data.name)
    } else if (data?.date != null) {
      setEditedName(data.date)
    }
  }, [data])

  const handleRenameSubmit = async (e) => {
    e.preventDefault()
    if (!walkId || !apiFetch) return
    try {
      const res = await apiFetch(`/api/walks/${encodeURIComponent(walkId)}/rename`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: editedName })
      })
      if (!res.ok) {
        const payload = await res.json().catch(() => ({}))
        throw new Error(payload.detail || 'Failed to rename walk')
      }
      const resData = await res.json()
      setData(prev => prev ? { ...prev, name: resData.name } : prev)
      setIsEditingName(false)
      if (onRenamed) onRenamed()
    } catch (err) {
      alert(err.message || 'Failed to rename walk')
    }
  }

  useEffect(() => {
    if (data?.rating != null) {
      setRating(data.rating)
    }
  }, [data])

  const handleRatingChange = async (newRating) => {
    if (!walkId || !apiFetch || shareToken) return
    try {
      const res = await apiFetch(`/api/walks/${encodeURIComponent(walkId)}/rate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ rating: newRating })
      })
      if (!res.ok) {
        const payload = await res.json().catch(() => ({}))
        throw new Error(payload.detail || 'Failed to update rating')
      }
      setRating(newRating)
      setData(prev => prev ? { ...prev, rating: newRating } : prev)
      if (onRenamed) onRenamed()
    } catch (err) {
      alert(err.message || 'Failed to update rating')
    }
  }

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
  const riskProfile = useMemo(() => payload?.riskProfile || null, [payload])
  const recoveryPhases = useMemo(() => payload?.recoveryPhases || [], [payload])
  const sensorGaps = useMemo(() => payload?.sensorGaps || [], [payload])
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
      const absolute = `${window.location.origin}/?share=${payload.token}`
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
    const absolute = `${window.location.origin}/?share=${token}`
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
        {isEditingName ? (
          <form onSubmit={handleRenameSubmit} style={{ display: 'flex', gap: '0.5rem', alignItems: 'center', marginBottom: '0.5rem' }}>
            <input
              type="text"
              value={editedName}
              onChange={e => setEditedName(e.target.value)}
              style={{
                fontSize: '1.2rem',
                fontWeight: 700,
                padding: '0.3rem 0.6rem',
                borderRadius: '4px',
                border: '1px solid var(--border)',
                background: 'var(--bg)',
                color: 'var(--text)',
                flex: '1',
                maxWidth: '400px'
              }}
              placeholder="Walk name"
              autoFocus
            />
            <button className="btn-primary" type="submit" style={{ padding: '0.3rem 0.8rem', fontSize: '0.9rem' }}>Save</button>
            <button className="btn-secondary" type="button" onClick={() => { setIsEditingName(false); setEditedName(data.name || data.date || '') }} style={{ padding: '0.3rem 0.8rem', fontSize: '0.9rem' }}>Cancel</button>
          </form>
        ) : (
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.35rem' }}>
            <h2 style={{ fontSize: '1.4rem', fontWeight: 700, letterSpacing: 'normal', textTransform: 'none', color: 'var(--text)', margin: 0 }}>
              {walkTitle}
            </h2>
            {!shareToken && (
              <button
                onClick={() => setIsEditingName(true)}
                style={{
                  background: 'none',
                  border: 'none',
                  cursor: 'pointer',
                  padding: '4px',
                  display: 'inline-flex',
                  alignItems: 'center',
                  opacity: 0.6
                }}
                title="Rename walk"
              >
                ✏️
              </button>
            )}
          </div>
        )}
        <p className="analysis-subtitle">{subtitleParts.join(' · ')}</p>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', marginTop: '0.35rem' }}>
          <span className="analysis-subtitle" style={{ fontSize: '0.9rem' }}>Rating:</span>
          <div style={{ display: 'inline-flex', gap: '0.15rem' }}>
            {[1, 2, 3, 4, 5].map((star) => {
              const active = star <= rating
              return (
                <button
                  key={star}
                  type="button"
                  onClick={() => handleRatingChange(star)}
                  disabled={Boolean(shareToken)}
                  style={{
                    background: 'none',
                    border: 'none',
                    cursor: shareToken ? 'default' : 'pointer',
                    padding: '0.1rem',
                    fontSize: '1.25rem',
                    opacity: active ? 1 : 0.25,
                    transition: 'opacity 0.15s ease'
                  }}
                  title={shareToken ? `Rated ${rating}/5` : `Rate ${star} stars`}
                >
                  ⭐
                </button>
              )
            })}
          </div>
        </div>
        {!shareToken && (
          <>
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
          </>
        )}
      </section>

      {hasCGM && (
        <div className="analysis-tabs home-tabs">
          <button className={`home-tab ${analysisTab === 'general' ? 'home-tab--active' : ''}`} onClick={() => setAnalysisTab('general')} type="button">
            General
          </button>
          <button className={`home-tab ${analysisTab === 'diabetes' ? 'home-tab--active' : ''}`} onClick={() => setAnalysisTab('diabetes')} type="button">
            {hasInsulin ? 'Diabetes' : 'Glucose'}
          </button>
        </div>
      )}

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
            {hasInsulin && (
              <Stat
                label="Insulin stress"
                value={`${fmt(insulinStressSummary.stress_multiplier, 2, 'x')}${insulinStressSummary.band ? ` (${insulinStressSummary.band})` : ''}`}
              />
            )}
            {hasPump && (
              <Stat
                label="SmartGuard"
                value={smartguardSummary.any_activity ? `${fmt(smartguardSummary.predicted_low_suspend_count, 0)} predicted-low` : 'No events'}
              />
            )}
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
          title={hasInsulin ? "Glucose and Insulin" : "Glucose Trend"}
          description={hasInsulin ? "Overlays glucose readings, basal insulin, recorded boluses, and estimated active bolus insulin across the route distance." : "Plots glucose readings across the route distance to help analyze fueling and metabolic response."}
          empty={!glucoseChartRows.length ? (hasInsulin ? 'No BG/insulin points available.' : 'No glucose points available.') : null}
        >
          {hasInsulin && (
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
          )}

          {hasPump && !!smartguardMarkerRows.length && (
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
              {hasInsulin && (
                <YAxis yAxisId="ins" orientation="right" width={DISTANCE_RIGHT_Y_WIDTH} domain={[0, INSULIN_AXIS_MAX]} tickFormatter={(v) => num(v, 1)} unit=" U/h" />
              )}
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
              {hasPump && smartguardMarkerRows.map((row, idx) => {
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
              {hasInsulin && (
                <>
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
                </>
              )}
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

      {analysisTab === 'diabetes' && (
        !hasInsulin ? (
          <section className="card">
            <h2>Glucose Summary</h2>
            <div className="analysis-summary-row">
              <span>Time in target (4.0–8.0 mmol/L):</span>
              <strong>{fmt(metrics.tir_pct, 0, '%')}</strong>
            </div>
            <div className="analysis-summary-row">
              <span>Glucose change:</span>
              <strong style={{ color: metrics.bg_delta < 0 ? '#1f8a5b' : metrics.bg_delta > 0 ? '#d16a00' : 'inherit' }}>
                {metrics.bg_delta > 0 ? '+' : ''}{fmt(metrics.bg_delta, 1, ' mmol/L')}
              </strong>
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
            {hasPump && (
              <>
                <div className="analysis-summary-row">
                  <span>SmartGuard predicted-low suspend:</span>
                  <strong>{fmt(smartguardSummary.predicted_low_suspend_count, 0)}</strong>
                </div>
                <div className="analysis-summary-row">
                  <span>SmartGuard suspend/resume:</span>
                  <strong>{fmt(smartguardSummary.suspend_count, 0)} / {fmt(smartguardSummary.resume_count, 0)}</strong>
                </div>
              </>
            )}
          </section>
        )
      )}

      {/* Glucose Risk Profile */}
      <section className="card">
        <h2>{hasInsulin ? "Glucose Risk Profile" : "Glucose Response"}</h2>
        {riskProfile ? (
          <>
            <div className="analysis-summary-row">
              <span>Pre-walk BG:</span>
              {riskProfile.prewalk.bg_at_start != null ? (
                hasInsulin ? (
                  <strong style={{
                    color: riskProfile.prewalk.assessment === 'optimal' ? '#1f8a5b'
                      : riskProfile.prewalk.assessment === 'low' ? '#be2f2f'
                      : riskProfile.prewalk.assessment === 'contraindicated_cl' ? '#be2f2f'
                      : '#d16a00',
                  }}>
                    {fmt(riskProfile.prewalk.bg_at_start, 1)} mmol/L
                    {' '}({riskProfile.prewalk.assessment === 'optimal' ? 'in target 7–10'
                      : riskProfile.prewalk.assessment === 'low' ? 'below target — hypo risk elevated'
                      : riskProfile.prewalk.assessment === 'contraindicated_cl' ? 'CL contraindication — delay walk until BG reduced'
                      : 'above target'})
                  </strong>
                ) : (
                  <strong>{fmt(riskProfile.prewalk.bg_at_start, 1)} mmol/L</strong>
                )
              ) : <strong>n/a</strong>}
            </div>
            {hasInsulin && riskProfile.prewalk.assessment === 'contraindicated_cl' && (
              <div style={{ margin: '0.4rem 0 0.6rem', padding: '0.5rem 0.7rem', background: 'rgba(190,47,47,0.08)', borderRadius: '4px', borderLeft: '3px solid #be2f2f' }}>
                <strong style={{ color: '#be2f2f' }}>Closed-loop contraindication</strong>
                <span style={{ fontSize: '0.85rem', marginLeft: '0.4rem' }}>Do not start walk until BG is below 15.0 mmol/L. Significant hyperglycaemia is a hard stop for closed-loop exercise (Zaharieva et al. 2020, Table 3).</span>
              </div>
            )}
            {hasInsulin && riskProfile.prewalk.iob_at_start != null && (
              <div className="analysis-summary-row">
                <span>IOB at walk start:</span>
                <strong style={{
                  color: riskProfile.prewalk.iob_flag === 'high'
                    ? '#be2f2f'
                    : riskProfile.prewalk.iob_flag === 'moderate'
                      ? '#d16a00'
                      : '#1f8a5b',
                }}>
                  {fmt(riskProfile.prewalk.iob_at_start, 1)} U
                  {riskProfile.prewalk.iob_flag === 'high'
                    ? ' — high, significant hypo risk during activity'
                    : riskProfile.prewalk.iob_flag === 'moderate'
                      ? ' — moderate, consider reducing bolus or taking carbs'
                      : ' — low'}
                </strong>
              </div>
            )}
            <div className="analysis-summary-row">
              <span>During-walk drop rate:</span>
              {riskProfile.during.drop_rate_mmol_h != null ? (
                hasInsulin ? (
                  <strong style={{ color: riskProfile.during.drop_flag === 'rapid' ? '#be2f2f' : riskProfile.during.drop_flag === 'elevated' ? '#d16a00' : '#1f8a5b' }}>
                    {fmt(riskProfile.during.drop_rate_mmol_h, 2)} mmol/L/h
                    {riskProfile.during.drop_flag === 'rapid' ? ' — rapid, high hypo risk' : riskProfile.during.drop_flag === 'elevated' ? ' — elevated, monitor closely' : ' — within normal range'}
                  </strong>
                ) : (
                  <strong>{fmt(riskProfile.during.drop_rate_mmol_h, 2)} mmol/L/h</strong>
                )
              ) : <strong>n/a</strong>}
            </div>
            {/* N-02: First-30-min drop rate breakdown */}
            {hasInsulin && riskProfile.during.first_30_drop_rate != null && (
              <div className="analysis-summary-row" style={{ paddingLeft: '1rem', fontSize: '0.85rem', color: 'var(--text-muted)' }}>
                <span>↳ First 30 min:</span>
                <strong style={{ color: riskProfile.during.first_30_flag === 'rapid' ? '#be2f2f' : riskProfile.during.first_30_flag === 'elevated' ? '#d16a00' : 'inherit' }}>
                  {fmt(riskProfile.during.first_30_drop_rate, 2)} mmol/L/h
                  {riskProfile.during.first_30_flag === 'rapid' ? ' — rapid early descent' : ''}
                </strong>
                {riskProfile.during.rest_drop_rate != null && (
                  <span style={{ marginLeft: '0.5rem', color: 'var(--text-muted)' }}>
                    · rest: {fmt(riskProfile.during.rest_drop_rate, 2)} mmol/L/h
                  </span>
                )}
              </div>
            )}
            {hasInsulin && riskProfile.during.projected_hypo_min != null && riskProfile.during.drop_flag !== 'normal' && (
              <div className="analysis-summary-row">
                <span>Projected hypo (3.9 mmol/L) from walk end:</span>
                <strong style={{ color: '#be2f2f' }}>
                  {riskProfile.during.projected_hypo_min < 60
                    ? `${riskProfile.during.projected_hypo_min} min`
                    : `${fmt(riskProfile.during.projected_hypo_min / 60, 1)} h`}
                  {' '}at current rate
                </strong>
              </div>
            )}
            <div className="analysis-summary-row">
              <span>Post-walk BG:</span>
              <strong>{riskProfile.during.bg_at_end != null ? `${fmt(riskProfile.during.bg_at_end, 1)} mmol/L` : 'n/a'}</strong>
            </div>
            <div className="analysis-summary-row">
              <span>Exercise intensity:</span>
              <strong style={{ textTransform: 'capitalize' }}>{riskProfile.during.intensity}</strong>
            </div>
            {/* N-03: IHE burst detection */}
            {riskProfile.during.ihe_burst_count > 0 && (
              <div className="analysis-summary-row">
                <span>High-intensity bursts (IHE):</span>
                <strong style={{ color: '#1f8a5b' }}>
                  {riskProfile.during.ihe_burst_count} burst{riskProfile.during.ihe_burst_count !== 1 ? 's' : ''} detected — associated with reduced glucose drop
                </strong>
              </div>
            )}
            {/* N-04: Post-exercise hyperglycaemia */}
            {hasInsulin && riskProfile.during.post_exercise_hyper && (
              <div style={{ marginTop: '0.6rem', padding: '0.5rem 0.7rem', background: 'rgba(209,106,0,0.08)', borderRadius: '4px', borderLeft: '3px solid #d16a00' }}>
                <strong style={{ color: '#d16a00' }}>Mild post-exercise hyperglycaemia</strong>
                <span style={{ fontSize: '0.85rem', marginLeft: '0.4rem' }}>
                  BG {fmt(riskProfile.during.bg_at_end, 1)} mmol/L at walk end (8–12 mmol/L range). Consider a 10–15 min low-intensity cool-down to reduce BG gradually before stopping (Scott et al. 2021).
                </span>
              </div>
            )}
            {hasInsulin && riskProfile.recovery.early_carb_trigger_met && (
              <div style={{ marginTop: '0.6rem', padding: '0.5rem 0.7rem', background: 'rgba(209,106,0,0.08)', borderRadius: '4px', borderLeft: '3px solid #d16a00' }}>
                <strong style={{ color: '#d16a00' }}>Early-recovery carb trigger met</strong>
                <span style={{ fontSize: '0.85rem', marginLeft: '0.4rem' }}>BG fell below 8.0 mmol/L within 2 h — carb intake recommended (Scott et al. 2021)</span>
              </div>
            )}
            {sensorGaps.length > 0 && (
              <div style={{ marginTop: '0.8rem' }}>
                <p style={{ fontWeight: 600, color: '#d16a00', marginBottom: '0.3rem' }}>Sensor gaps during walk ({sensorGaps.length})</p>
                <div className="trend-table-wrap">
                  <table className="trend-table">
                    <thead><tr><th>Start</th><th>End</th><th>Gap</th></tr></thead>
                    <tbody>
                      {sensorGaps.map((gap, i) => (
                        <tr key={i}>
                          <td>{new Date(gap.start).toLocaleTimeString()}</td>
                          <td>{new Date(gap.end).toLocaleTimeString()}</td>
                          <td>{fmt(gap.duration_min, 0)} min</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </>
        ) : <p className="empty">No risk profile available — BG or activity data missing.</p>}
      </section>

      {/* Extended Recovery Monitor */}
      <section className="card">
        <h2>Recovery Monitor</h2>
        {/* N-06: Nocturnal risk window end time */}
        {hasInsulin && riskProfile?.recovery?.risk_window_end_iso && (
          <div style={{ marginBottom: '0.7rem', display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap' }}>
            <span style={{ fontSize: '0.8rem', fontWeight: 600, background: 'rgba(190,47,47,0.1)', color: '#be2f2f', borderRadius: '4px', padding: '0.2rem 0.5rem' }}>
              Elevated risk window closes {new Date(riskProfile.recovery.risk_window_end_iso).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
              {' '}({new Date(riskProfile.recovery.risk_window_end_iso).toLocaleDateString([], { weekday: 'short', month: 'short', day: 'numeric' })})
            </span>
            <span style={{ fontSize: '0.78rem', color: 'var(--text-muted)' }}>Scott et al. 2021 · Zaharieva et al. 2020</span>
          </div>
        )}
        <p style={{ fontSize: '0.82rem', color: 'var(--text-muted)', marginBottom: '0.8rem' }}>
          {hasInsulin
            ? "Nocturnal hypo risk persists 6–12 h after prolonged aerobic exercise (Scott et al. 2021, Lancet D&E 9:304–17)."
            : "Plots glucose recovery patterns to monitor glycemic stability after the walk."}
        </p>
        {recoveryPhases.length > 0 ? (
          <>
            {hasInsulin && riskProfile?.recovery?.overnight_risk && riskProfile.recovery.overnight_risk !== 'low' && (
              <div style={{ marginBottom: '0.7rem', padding: '0.5rem 0.7rem', background: riskProfile.recovery.overnight_risk === 'confirmed' ? 'rgba(190,47,47,0.08)' : 'rgba(209,106,0,0.08)', borderRadius: '4px', borderLeft: `3px solid ${riskProfile.recovery.overnight_risk === 'confirmed' ? '#be2f2f' : '#d16a00'}` }}>
                <strong style={{ color: riskProfile.recovery.overnight_risk === 'confirmed' ? '#be2f2f' : '#d16a00' }}>
                  {riskProfile.recovery.overnight_risk === 'confirmed'
                    ? 'Hypo confirmed in post-walk recovery window'
                    : 'Elevated overnight hypo risk detected'}
                </strong>
              </div>
            )}
            <div className="trend-table-wrap">
              <table className="trend-table">
                <thead>
                  <tr>
                    <th>Window</th>
                    <th>Readings</th>
                    <th>Min BG</th>
                    <th>Mean BG</th>
                    {hasInsulin && <th>Hypos</th>}
                    <th>Slope / h</th>
                  </tr>
                </thead>
                <tbody>
                  {recoveryPhases.map((phase) => (
                    <tr key={phase.key} style={{ color: hasInsulin && phase.hypos > 0 ? '#be2f2f' : undefined }}>
                      <td>{phase.label}</td>
                      <td>{phase.count || '—'}</td>
                      <td>{phase.min_bg != null ? `${fmt(phase.min_bg, 1)} mmol/L` : '—'}</td>
                      <td>{phase.mean_bg != null ? `${fmt(phase.mean_bg, 1)} mmol/L` : '—'}</td>
                      {hasInsulin && <td>{phase.hypos > 0 ? <strong style={{ color: '#be2f2f' }}>{phase.hypos}</strong> : (phase.count ? '0' : '—')}</td>}
                      <td>{phase.slope_per_hour != null ? `${fmt(phase.slope_per_hour, 2)} mmol/L/h` : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </>
        ) : <p className="empty">No post-walk BG data in Carelink export for this period.</p>}
      </section>

      {/* Next Walk Preparation */}
      {hasInsulin && riskProfile?.nextWalk && (
        <section className="card">
          <h2>Next Walk Preparation</h2>
          <p style={{ fontSize: '0.82rem', color: 'var(--text-muted)', marginBottom: '1rem' }}>
            Evidence-based guidance derived from this walk (Riddell et al. 2017, PMID 28126459; Scott et al. 2021; Zaharieva et al. 2020).
          </p>
          <div style={{ display: 'grid', gap: '1rem', gridTemplateColumns: 'repeat(auto-fit, minmax(260px, 1fr))' }}>
            <div style={{ padding: '0.9rem 1rem', background: 'rgba(31,138,91,0.06)', borderRadius: '6px', borderLeft: '3px solid #1f8a5b' }}>
              <h3 style={{ fontSize: '0.9rem', fontWeight: 700, marginBottom: '0.6rem', color: '#1f8a5b' }}>Before the walk</h3>
              <ul style={{ paddingLeft: '1.2rem', fontSize: '0.87rem', lineHeight: 1.65, margin: 0 }}>
                <li>Target BG <strong>{riskProfile.nextWalk.target_bg_range[0]}–{riskProfile.nextWalk.target_bg_range[1]} mmol/L</strong> at walk start</li>
                <li>Reduce basal by <strong>{riskProfile.nextWalk.basal_reduction_pct}%</strong> starting <strong>{riskProfile.nextWalk.basal_timing_min} min</strong> before</li>
                <li>If taking a pre-walk bolus, reduce by <strong>{riskProfile.nextWalk.bolus_reduction_pct}%</strong> (intensity: <em style={{ textTransform: 'capitalize' }}>{riskProfile.nextWalk.intensity}</em>)</li>
                <li>Prepare ~<strong>{riskProfile.nextWalk.carbs_per_hour_g} g carbs/h</strong>{riskProfile.nextWalk.total_carbs_g ? ` (~${riskProfile.nextWalk.total_carbs_g} g total based on duration)` : ''}</li>
              </ul>
              {/* E-02: Riddell intensity-reduction reference table */}
              <table style={{ marginTop: '0.8rem', width: '100%', fontSize: '0.8rem', borderCollapse: 'collapse' }}>
                <thead>
                  <tr style={{ borderBottom: '1px solid var(--border)' }}>
                    <th style={{ textAlign: 'left', paddingBottom: '0.25rem', fontWeight: 600 }}>Intensity</th>
                    <th style={{ textAlign: 'center', paddingBottom: '0.25rem', fontWeight: 600 }}>Bolus ↓</th>
                    <th style={{ textAlign: 'center', paddingBottom: '0.25rem', fontWeight: 600 }}>Basal ↓</th>
                  </tr>
                </thead>
                <tbody>
                  {[
                    { label: 'Light', bolus: 25, basal: 50 },
                    { label: 'Moderate', bolus: 50, basal: 65 },
                    { label: 'Vigorous', bolus: 75, basal: 80 },
                  ].map(({ label, bolus, basal }) => {
                    const active = riskProfile.nextWalk.intensity?.toLowerCase() === label.toLowerCase()
                    return (
                      <tr key={label} style={{
                        background: active ? 'rgba(31,138,91,0.12)' : 'transparent',
                        fontWeight: active ? 700 : 400,
                      }}>
                        <td style={{ padding: '0.2rem 0.3rem' }}>{label}{active ? ' ★' : ''}</td>
                        <td style={{ textAlign: 'center', padding: '0.2rem 0.3rem' }}>{bolus}%</td>
                        <td style={{ textAlign: 'center', padding: '0.2rem 0.3rem' }}>{basal}%</td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
              <p style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.4rem', marginBottom: 0 }}>
                Riddell et al. 2017, Table 3
              </p>
            </div>
            {riskProfile.nextWalk.cl_temp_target_mmol && (
              <div style={{ padding: '0.9rem 1rem', background: 'rgba(8,114,184,0.06)', borderRadius: '6px', borderLeft: '3px solid #0872b8' }}>
                <h3 style={{ fontSize: '0.9rem', fontWeight: 700, marginBottom: '0.6rem', color: '#0872b8' }}>
                  780G SmartGuard — temp target
                </h3>
                <ul style={{ paddingLeft: '1.2rem', fontSize: '0.87rem', lineHeight: 1.65, margin: 0 }}>
                  <li>
                    Raise CL target to{' '}
                    <strong>{riskProfile.nextWalk.cl_temp_target_mmol} mmol/L ({riskProfile.nextWalk.cl_temp_target_mgdl} mg/dL)</strong>{' '}
                    in SmartGuard settings
                  </li>
                  <li>
                    Set <strong>{riskProfile.nextWalk.cl_temp_start_min_before} min before</strong> starting activity
                  </li>
                  <li>
                    Cancel when BG returns to your normal range (<strong>≤ {riskProfile.nextWalk.target_bg_range[1]} mmol/L</strong>) after the walk
                  </li>
                </ul>
                <p style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.6rem', marginBottom: 0 }}>
                  Zaharieva et al. 2020 · Boom et al. 2021
                </p>
              </div>
            )}
            <div style={{ padding: '0.9rem 1rem', background: 'rgba(10,143,143,0.06)', borderRadius: '6px', borderLeft: '3px solid #0a8f8f' }}>
              <h3 style={{ fontSize: '0.9rem', fontWeight: 700, marginBottom: '0.6rem', color: '#0a8f8f' }}>After the walk</h3>
              <ul style={{ paddingLeft: '1.2rem', fontSize: '0.87rem', lineHeight: 1.65, margin: 0 }}>
                <li>Check BG immediately and every <strong>15 min</strong> for first hour</li>
                <li>
                  If BG &lt; 8.0 mmol/L: take <strong>15–20 g fast carbs</strong>{' '}
                  <span style={{ fontWeight: 400 }}>(mixed glucose + fructose — sports drink or fruit — restores liver glycogen faster than pure glucose for the same gram count; Scott et al. 2021)</span>
                </li>
                <li>Reduce bolus at first recovery meal by ~<strong>{riskProfile.nextWalk.recovery_bolus_reduction_pct}%</strong></li>
                <li>Reduce basal by <strong>20%</strong> for <strong>{riskProfile.nextWalk.recovery_basal_reduction_h} h</strong> post-walk</li>
                {/* E-02: bedtime snack guidance for light-to-moderate intensity (Riddell 2017 Table 3) */}
                {(riskProfile.nextWalk.intensity === 'light' || riskProfile.nextWalk.intensity === 'moderate') && (
                  <li>Bedtime snack of <strong>~0.4 g carbs/kg</strong> body weight to reduce nocturnal hypo risk (Riddell et al. 2017)</li>
                )}
                <li>Set CGM <strong>low alarm</strong> overnight</li>
              </ul>
            </div>
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


    </section>
  )
}
