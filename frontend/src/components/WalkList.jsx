const FILE_ICONS = {
  fit: '🏃',
  csv: '📊',
  gpx: '🗺️',
}

function fileIcon(name) {
  const ext = name.split('.').pop().toLowerCase()
  return FILE_ICONS[ext] ?? '📄'
}

function WalkCard({ walk, onDeleted, onView }) {
  const handleDelete = async () => {
    if (!confirm(`Delete walk ${walk.name || walk.date} and all its files?`)) return
    const res = await fetch(`/api/walks/${encodeURIComponent(walk.id)}`, { method: 'DELETE' })
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
      <ul className="file-list">
        {walk.files.map(f => (
          <li key={f}>
            <span className="file-icon">{fileIcon(f)}</span>
            {f}
          </li>
        ))}
      </ul>
    </article>
  )
}

export default function WalkList({ walks, onDeleted, onView }) {
  if (!walks.length) {
    return <p className="empty">No walks yet — upload some files above.</p>
  }

  return (
    <section className="walk-list">
      <h2>Walks</h2>
      {walks.map(w => (
        <WalkCard key={w.id} walk={w} onDeleted={onDeleted} onView={onView} />
      ))}
    </section>
  )
}
