import { useEffect, useState } from 'react'
import {
  ResponsiveContainer, LineChart, Line,
  CartesianGrid, XAxis, YAxis, Tooltip, Legend,
} from 'recharts'
import { MapContainer, TileLayer, Polyline } from 'react-leaflet'
import 'leaflet/dist/leaflet.css'

const COLORS = ['#2563eb', '#dc2626', '#16a34a', '#d97706', '#7c3aed', '#db2777', '#0891b2', '#65a30d', '#92400e', '#475569']

function haversineKm(lat1, lon1, lat2, lon2) {
  const R = 6371
  const dLat = (lat2 - lat1) * Math.PI / 180
  const dLon = (lon2 - lon1) * Math.PI / 180
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
}

function overlapBounds(trackArrays, threshold = 0.25) {
  const shared = []
  trackArrays.forEach((track, i) => {
    const others = trackArrays.filter((_, j) => j !== i)
    track.forEach(([lat, lon]) => {
      if (others.every(other => other.some(([olat, olon]) => haversineKm(lat, lon, olat, olon) <= threshold)))
        shared.push([lat, lon])
    })
  })
  if (shared.length < 2) return null
  const lats = shared.map(p => p[0])
  const lons = shared.map(p => p[1])
  return [[Math.min(...lats), Math.min(...lons)], [Math.max(...lats), Math.max(...lons)]]
}

function RouteMap({ walks }) {
  const tracks = walks.map((w, i) => ({ positions: w.track || [], color: COLORS[i % COLORS.length] }))
    .filter(t => t.positions.length > 1)
  if (!tracks.length) return null

  const trackArrays = tracks.map(t => t.positions)
  const allPts = trackArrays.flat()
  const lats = allPts.map(p => p[0])
  const lons = allPts.map(p => p[1])
  const fullBounds = [[Math.min(...lats), Math.min(...lons)], [Math.max(...lats), Math.max(...lons)]]
  const bounds = overlapBounds(trackArrays) ?? fullBounds

  return (
    <div className="card" style={{ padding: 0, overflow: 'hidden' }}>
      <MapContainer className="route-map" bounds={bounds} boundsOptions={{ padding: [24, 24] }} scrollWheelZoom style={{ height: 340 }}>
        <TileLayer
          attribution="&copy; OpenStreetMap contributors"
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        {tracks.map((t, i) => (
          <Polyline key={i} positions={t.positions} color={t.color} weight={3} opacity={0.85} />
        ))}
      </MapContainer>
    </div>
  )
}

function fmt(v, d = 1, s = '') {
  if (v == null || Number.isNaN(v)) return 'n/a'
  return `${Number(v).toFixed(d)}${s}`
}

function walkLabel(walk) {
  return walk.name ? `${walk.date} — ${walk.name}` : walk.date
}

// Build chart data on an absolute distance axis so sub-routes end at their
// own distance rather than being stretched to match longer walks.
function buildSeries(walks, key) {
  const maxDist = Math.max(...walks.map(w => w.metrics?.distance_km || 0))
  if (!maxDist) return []
  const n = 80
  return Array.from({ length: n }, (_, i) => {
    const dist = parseFloat(((i / (n - 1)) * maxDist).toFixed(2))
    const pt = { dist }
    walks.forEach(w => {
      const series = w.route_series?.[key]
      const walkDist = w.metrics?.distance_km || 0
      if (!series || walkDist <= 0 || dist > walkDist + 0.05) {
        pt[w.walk_id] = null
        return
      }
      // Interpolate into this walk's 50-point series at the requested distance.
      const progress = Math.min(1, dist / walkDist)
      const idx = progress * (series.length - 1)
      const lo = Math.floor(idx)
      const hi = Math.min(series.length - 1, Math.ceil(idx))
      const t = idx - lo
      const vLo = series[lo]
      const vHi = series[hi]
      pt[w.walk_id] = vLo != null && vHi != null
        ? parseFloat((vLo + t * (vHi - vLo)).toFixed(1))
        : (vLo ?? vHi ?? null)
    })
    return pt
  })
}

function hasData(walks, key) {
  return walks.some(w => w.route_series?.[key]?.some(v => v != null))
}

function MultiChart({ title, walks, seriesKey, unit, domain }) {
  if (!hasData(walks, seriesKey)) return null
  const data = buildSeries(walks, seriesKey)
  return (
    <div className="card chart-card">
      <div className="chart-card-header">
        <h2>{title}</h2>
      </div>
      <div className="chart-wrap">
        <ResponsiveContainer width="100%" height={220}>
          <LineChart data={data} margin={{ top: 4, right: 8, bottom: 4, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
            <XAxis
              dataKey="dist"
              type="number"
              domain={[0, 'dataMax']}
              tickFormatter={v => `${fmt(v, 1)} km`}
              tick={{ fontSize: 11 }}
            />
            <YAxis
              domain={domain || ['auto', 'auto']}
              tickFormatter={v => unit ? `${v}${unit}` : v}
              tick={{ fontSize: 11 }}
              width={45}
            />
            <Tooltip
              formatter={(v, id) => [v != null ? `${fmt(v, 1)}${unit || ''}` : 'n/a', walkLabel(walks.find(w => w.walk_id === id) || {})]}
              labelFormatter={l => `${fmt(l, 2)} km`}
            />
            <Legend
              formatter={id => walkLabel(walks.find(w => w.walk_id === id) || { walk_id: id, date: id })}
              wrapperStyle={{ fontSize: 11 }}
            />
            {walks.map((w, i) => (
              <Line
                key={w.walk_id}
                dataKey={w.walk_id}
                stroke={COLORS[i % COLORS.length]}
                dot={false}
                strokeWidth={2}
                connectNulls={false}
                isAnimationActive={false}
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  )
}

function RouteGroupList({ groups, computing, onSelect, onCompute }) {
  return (
    <>
      <div className="card" style={{ padding: '1rem 1.25rem', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '1rem', flexWrap: 'wrap' }}>
        <p style={{ fontSize: '0.85rem', color: 'var(--text-muted)', margin: 0 }}>
          Routes are detected automatically when walk analytics are computed. Trigger a full refresh if walks are missing.
        </p>
        <button
          className="btn-primary"
          style={{ whiteSpace: 'nowrap', alignSelf: 'flex-start' }}
          disabled={computing}
          onClick={onCompute}
        >
          {computing ? 'Computing…' : 'Compute all analytics'}
        </button>
      </div>

      {!groups.length ? (
        <div className="card">
          <p className="empty" style={{ padding: '1.5rem' }}>
            No repeated routes detected yet. Analytics need to be computed for at least two walks sharing a route corridor.
          </p>
        </div>
      ) : (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
          {groups.map(g => (
            <div key={g.group_id} className="walk-card" style={{ cursor: 'pointer' }} onClick={() => onSelect(g.group_id)}>
              <div className="walk-header">
                <div className="walk-title">
                  <span className="walk-date-main">{fmt(g.avg_distance_km, 1)} km route</span>
                  <span className="walk-date-sub">{g.walk_count} walks</span>
                </div>
                <button className="btn-secondary" onClick={e => { e.stopPropagation(); onSelect(g.group_id) }}>Compare</button>
              </div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.4rem', marginTop: '0.4rem' }}>
                {g.walks.map((w, i) => (
                  <span key={w.walk_id} style={{
                    display: 'inline-flex', alignItems: 'center', gap: '0.3rem',
                    background: '#f3f4f6', borderRadius: '999px',
                    padding: '0.15rem 0.6rem', fontSize: '0.78rem',
                  }}>
                    <span style={{ width: 8, height: 8, borderRadius: '50%', background: COLORS[i % COLORS.length], display: 'inline-block' }} />
                    {walkLabel(w)}
                    {w.distance_km != null && (
                      <span style={{ color: 'var(--text-muted)' }}>{fmt(w.distance_km, 1)} km</span>
                    )}
                  </span>
                ))}
              </div>
            </div>
          ))}
        </div>
      )}
    </>
  )
}

export default function RouteCompare({ onSelectWalk, apiFetch }) {
  const [groups, setGroups] = useState(null)
  const [selectedGroupId, setSelectedGroupId] = useState(null)
  const [compareData, setCompareData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [computing, setComputing] = useState(false)
  const [error, setError] = useState(null)

  function loadGroups() {
    return apiFetch('/api/analytics/routes')
      .then(r => r.json())
      .then(setGroups)
      .catch(() => setError('Could not load route groups'))
  }

  useEffect(() => { loadGroups() }, [apiFetch])

  function selectGroup(groupId) {
    setSelectedGroupId(groupId)
    setCompareData(null)
    setLoading(true)
    apiFetch(`/api/analytics/routes/${groupId}/compare`)
      .then(r => r.json())
      .then(d => { setCompareData(d); setLoading(false) })
      .catch(() => { setError('Could not load comparison data'); setLoading(false) })
  }

  function handleCompute() {
    setComputing(true)
    apiFetch('/api/analytics/backfill', { method: 'POST' })
      .then(() => loadGroups())
      .finally(() => setComputing(false))
  }

  const walks = compareData?.walks || []

  return (
    <div className="analysis-view">
      <div className="card analysis-header-card">
        <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: selectedGroupId && walks.length > 0 ? '0.5rem' : 0 }}>
          {selectedGroupId && (
            <button className="btn-back" onClick={() => { setSelectedGroupId(null); setCompareData(null) }}>
              ← All routes
            </button>
          )}
          <h2 style={{ textTransform: 'uppercase', letterSpacing: '0.05em', color: 'var(--text-muted)', fontSize: '1rem', fontWeight: 600 }}>
            Route Comparison
          </h2>
        </div>
        {selectedGroupId && walks.length > 0 && (
          <p className="analysis-subtitle">
            {walks.length} walks · distances {walks.map(w => fmt(w.metrics?.distance_km, 1)).join(' / ')} km
          </p>
        )}
      </div>

      {error && <div className="error-banner">{error}</div>}

      {!selectedGroupId && (
        groups == null
          ? <div className="card"><p className="empty" style={{ padding: '1.5rem' }}>Loading routes…</p></div>
          : <RouteGroupList groups={groups} computing={computing} onSelect={selectGroup} onCompute={handleCompute} />
      )}

      {selectedGroupId && loading && (
        <div className="card"><p className="empty" style={{ padding: '1.5rem' }}>Loading comparison…</p></div>
      )}

      {selectedGroupId && !loading && walks.length > 0 && (
        <>
          <div className="card" style={{ padding: '1rem 1.25rem' }}>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem' }}>
              {walks.map((w, i) => (
                <span key={w.walk_id} style={{
                  display: 'inline-flex', alignItems: 'center', gap: '0.4rem',
                  background: '#f3f4f6', borderRadius: '999px',
                  padding: '0.2rem 0.75rem', fontSize: '0.82rem',
                }}>
                  <span style={{ width: 10, height: 10, borderRadius: '50%', background: COLORS[i % COLORS.length], flexShrink: 0 }} />
                  {walkLabel(w)}
                  <span style={{ color: 'var(--text-muted)' }}>{fmt(w.metrics?.distance_km, 1)} km</span>
                  {onSelectWalk && (
                    <button
                      onClick={() => onSelectWalk(w.walk_id)}
                      style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer', color: 'var(--primary)', fontSize: '0.78rem', fontWeight: 600 }}
                    >
                      View ↗
                    </button>
                  )}
                </span>
              ))}
            </div>
          </div>

          <RouteMap walks={walks} />

          <MultiChart title="Heart Rate" walks={walks} seriesKey="hr" unit=" bpm" domain={[60, 160]} />
          <MultiChart title="Blood Glucose" walks={walks} seriesKey="bg" unit=" mmol/L" domain={[3, 12]} />
          <MultiChart title="Temperature" walks={walks} seriesKey="temp_c" unit="°C" />
          <MultiChart title="Wind Speed" walks={walks} seriesKey="wind_kph" unit=" km/h" domain={[0, 'auto']} />

          <div className="card">
            <h2>Walk Summary</h2>
            <div className="trend-table-wrap">
              <table className="trend-table">
                <thead>
                  <tr>
                    <th>Walk</th>
                    <th>Dist</th>
                    <th>Avg HR</th>
                    <th>BG Δ</th>
                    <th>TiR</th>
                    <th>Avg Temp</th>
                    <th>Avg Wind</th>
                    <th>Weather</th>
                    {onSelectWalk && <th></th>}
                  </tr>
                </thead>
                <tbody>
                  {walks.map((w, i) => (
                    <tr key={w.walk_id}>
                      <td>
                        <span style={{ display: 'inline-flex', alignItems: 'center', gap: '0.4rem' }}>
                          <span style={{ width: 8, height: 8, borderRadius: '50%', background: COLORS[i % COLORS.length], flexShrink: 0 }} />
                          {walkLabel(w)}
                        </span>
                      </td>
                      <td>{fmt(w.metrics?.distance_km, 1)} km</td>
                      <td>{fmt(w.metrics?.avg_hr, 0)} bpm</td>
                      <td>{w.metrics?.bg_delta != null ? `${w.metrics.bg_delta > 0 ? '+' : ''}${fmt(w.metrics.bg_delta, 1)}` : 'n/a'}</td>
                      <td>{fmt(w.metrics?.tir_pct, 0)}%</td>
                      <td>{fmt(w.metrics?.temp_avg_c, 1)}°C</td>
                      <td>{fmt(w.metrics?.wind_avg_kph, 1)} km/h</td>
                      <td>{w.metrics?.weather_stress_band || 'n/a'}</td>
                      {onSelectWalk && (
                        <td>
                          <button
                            onClick={() => onSelectWalk(w.walk_id)}
                            style={{ background: 'none', border: 'none', padding: 0, cursor: 'pointer', color: 'var(--primary)', fontSize: '0.82rem', fontWeight: 600, whiteSpace: 'nowrap' }}
                          >
                            View ↗
                          </button>
                        </td>
                      )}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
