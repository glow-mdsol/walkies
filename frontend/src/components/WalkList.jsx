function fmt(v, d = 1, s = '') {
  if (v == null || Number.isNaN(v)) return null
  return `${Number(v).toFixed(d)}${s}`
}

function fmtDuration(h) {
  if (h == null) return null
  const hours = Math.floor(h)
  const mins = Math.round((h - hours) * 60)
  return hours > 0 ? `${hours}h ${mins}m` : `${mins}m`
}

function fmtBgDelta(v) {
  if (v == null) return null
  return `${v > 0 ? '+' : ''}${Number(v).toFixed(1)} mmol/L`
}

function MetricPill({ label, value }) {
  if (value == null) return null
  return (
    <span className="metric-pill">
      <span className="metric-pill-label">{label}</span>
      <span className="metric-pill-value">{value}</span>
    </span>
  )
}

function WalkCard({ walk, onDeleted, onView, apiFetch }) {
  const m = walk.metrics

  const handleDelete = async () => {
    if (!confirm(`Delete walk ${walk.name || walk.date} and all its files?`)) return
    const res = await apiFetch(`/api/walks/${encodeURIComponent(walk.id)}`, { method: 'DELETE' })
    if (res.ok) onDeleted()
  }

  return (
    <article className="walk-card">
      <div className="walk-header">
        <div className="walk-title">
          {walk.name && <h3>{walk.name}</h3>}
          <span className={walk.name ? 'walk-date-sub' : 'walk-date-main'}>{walk.date}</span>
        </div>
        <div className="walk-actions">
          <button className="btn-secondary" onClick={() => onView(walk.id)}>View analysis</button>
          <button className="btn-danger" onClick={handleDelete}>Delete</button>
        </div>
      </div>
      {m ? (
        <div className="metric-pills">
          <MetricPill label="Distance" value={fmt(m.distance_km, 1, ' km')} />
          <MetricPill label="Duration" value={fmtDuration(m.duration_h)} />
          <MetricPill label="Avg HR" value={fmt(m.avg_hr, 0, ' bpm')} />
          <MetricPill label="BG Δ" value={fmtBgDelta(m.bg_delta)} />
          <MetricPill label="TiR" value={fmt(m.tir_pct, 0, '%')} />
        </div>
      ) : (
        <p className="metric-pills-empty">Analytics computing…</p>
      )}
    </article>
  )
}

export default function WalkList({ walks, onDeleted, onView, apiFetch }) {
  if (!walks.length) {
    return <p className="empty">No walks yet — upload some files above.</p>
  }

  return (
    <section className="walk-list">
      {walks.map(w => (
        <WalkCard key={w.id} walk={w} onDeleted={onDeleted} onView={onView} apiFetch={apiFetch} />
      ))}
    </section>
  )
}
