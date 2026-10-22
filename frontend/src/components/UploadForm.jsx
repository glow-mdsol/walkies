import { useState, useEffect, useCallback } from 'react'

const today = () => new Date().toISOString().slice(0, 10)

export default function UploadForm({ onUploaded, apiFetch, garminEmail }) {
  // Mode: 'garmin' | 'manual' | 'telemetry'
  const [mode, setMode] = useState(() => (garminEmail ? 'garmin' : 'manual'))

  // Form fields
  const [name, setName] = useState('')
  const [date, setDate] = useState(today)
  const [dateSource, setDateSource] = useState(null) // 'fit' | 'garmin' | null
  const [fitFile, setFitFile] = useState(null)
  const [carelinkFile, setCarelinkFile] = useState(null)
  const [gpxFile, setGpxFile] = useState(null)
  const [telemetryFile, setTelemetryFile] = useState(null)
  const [telemetryProvider, setTelemetryProvider] = useState('auto')
  const [uploading, setUploading] = useState(false)
  const [message, setMessage] = useState(null)

  // Garmin activity selection state
  const [activities, setActivities] = useState([])
  const [loadingActivities, setLoadingActivities] = useState(false)
  const [activitiesError, setActivitiesError] = useState(null)
  const [selectedActivityId, setSelectedActivityId] = useState('')

  const fetchGarminActivities = useCallback(async () => {
    if (!garminEmail) return
    setLoadingActivities(true)
    setActivitiesError(null)
    try {
      const res = await apiFetch('/api/garmin/activities?limit=25')
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.detail || 'Failed to fetch activities from Garmin')
      }
      const data = await res.json()
      const acts = data.activities || []
      setActivities(acts)

      // Pre-select first unimported activity if available
      const firstUnimported = acts.find(a => !a.already_imported && (a.activity_type === 'walking' || a.activity_type === 'hiking')) || acts[0]
      if (firstUnimported && !selectedActivityId) {
        setSelectedActivityId(String(firstUnimported.activity_id))
        setName(firstUnimported.name || 'Walk')
        if (firstUnimported.date) {
          setDate(firstUnimported.date)
          setDateSource('garmin')
        }
      }
    } catch (err) {
      setActivitiesError(err.message)
    } finally {
      setLoadingActivities(false)
    }
  }, [apiFetch, garminEmail, selectedActivityId])

  useEffect(() => {
    if (mode === 'garmin' && garminEmail && activities.length === 0) {
      fetchGarminActivities()
    }
  }, [mode, garminEmail, activities.length, fetchGarminActivities])

  const handleActivitySelect = (activityId) => {
    setSelectedActivityId(activityId)
    const act = activities.find(a => String(a.activity_id) === String(activityId))
    if (act) {
      setName(act.name || 'Walk')
      if (act.date) {
        setDate(act.date)
        setDateSource('garmin')
      }
    }
  }

  const handleFitChange = async (e) => {
    const file = e.target.files[0] ?? null
    setFitFile(file)
    setDateSource(null)
    if (!file) return

    const form = new FormData()
    form.append('file', file)
    try {
      const res = await apiFetch('/api/walks/parse-fit-date', { method: 'POST', body: form })
      if (res.ok) {
        const { date: detected } = await res.json()
        setDate(detected)
        setDateSource('fit')
      }
    } catch {
      // silently ignore
    }
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    setUploading(true)
    setMessage(null)

    try {
      if (mode === 'garmin') {
        if (!selectedActivityId) {
          throw new Error('Please select a Garmin activity to import.')
        }

        const form = new FormData()
        form.append('activity_id', selectedActivityId)
        if (name) form.append('name', name)
        if (date) form.append('date', date)
        if (carelinkFile) form.append('carelink_file', carelinkFile)
        if (gpxFile) form.append('gpx_file', gpxFile)

        const res = await apiFetch('/api/garmin/activities/import', { method: 'POST', body: form })
        if (!res.ok) {
          const errData = await res.json().catch(() => ({}))
          throw new Error(errData.detail || 'Failed to import Garmin activity')
        }
        const data = await res.json()
        setMessage(`Successfully imported "${data.walk?.name || name}" for ${data.walk?.date || date}!`)
        setName('')
        setDate(today())
        setDateSource(null)
        setCarelinkFile(null)
        setGpxFile(null)
        setSelectedActivityId('')
        onUploaded()
      } else if (mode === 'telemetry') {
        if (!telemetryFile) {
          throw new Error('Please choose a CareLink, Dexcom Clarity, or Tandem t:connect CSV file.')
        }
        const form = new FormData()
        form.append('file', telemetryFile)
        form.append('provider', telemetryProvider)
        form.append('tz_offset_mins', String(-new Date().getTimezoneOffset()))

        const res = await apiFetch('/api/telemetry/import', { method: 'POST', body: form })
        const data = await res.json().catch(() => ({}))
        if (!res.ok) {
          throw new Error(data.detail || 'Telemetry CSV import failed')
        }
        setMessage(
          `Imported ${data.events_ingested} records (${data.detected_provider?.toUpperCase()}, source unit: ${data.source_unit}) across ${data.days_updated} day(s) and updated ${data.walks_updated} walk(s)!`
        )
        setTelemetryFile(null)
        e.target.reset()
        onUploaded()
      } else {
        // Manual file upload
        const form = new FormData()
        form.append('date', date)
        form.append('name', name)
        form.append('files', fitFile)
        if (carelinkFile) form.append('files', carelinkFile)
        if (gpxFile) form.append('files', gpxFile)

        const res = await apiFetch('/api/walks/upload', { method: 'POST', body: form })
        if (!res.ok) {
          const errData = await res.json().catch(() => ({}))
          throw new Error(errData.detail || 'Upload failed')
        }
        const data = await res.json()
        const uploadedCount = data.uploaded?.length ?? 0
        const reusedCount = data.reused?.length ?? 0
        const reusedText = reusedCount ? ` Reused ${reusedCount} Carelink file.` : ''
        setMessage(`Uploaded ${uploadedCount} file(s) for ${data.date}.${reusedText}`)
        setName('')
        setDate(today())
        setDateSource(null)
        setFitFile(null)
        setCarelinkFile(null)
        setGpxFile(null)
        e.target.reset()
        onUploaded()
      }
    } catch (err) {
      setMessage(`Error: ${err.message}`)
    } finally {
      setUploading(false)
    }
  }

  const selectedActivity = activities.find(a => String(a.activity_id) === String(selectedActivityId))

  return (
    <section className="card upload-card" style={{ maxWidth: '640px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1rem', flexWrap: 'wrap', gap: '0.5rem' }}>
        <h2 style={{ margin: 0 }}>{mode === 'telemetry' ? 'Import CGM / Pump CSV' : 'Add Walk'}</h2>

        <div style={{ display: 'flex', gap: '0.35rem', background: 'var(--surface-tint)', padding: '0.2rem', borderRadius: 'var(--radius)', border: '1px solid var(--border)', flexWrap: 'wrap' }}>
          {garminEmail && (
            <button
              type="button"
              className="btn-secondary"
              onClick={() => setMode('garmin')}
              style={{
                padding: '0.3rem 0.75rem',
                fontSize: '0.82rem',
                borderRadius: '6px',
                border: 'none',
                background: mode === 'garmin' ? 'var(--primary)' : 'transparent',
                color: mode === 'garmin' ? '#fff' : 'var(--text)',
                fontWeight: mode === 'garmin' ? 600 : 400,
              }}
            >
              ⌚ Select from Garmin
            </button>
          )}
          <button
            type="button"
            className="btn-secondary"
            onClick={() => setMode('manual')}
            style={{
              padding: '0.3rem 0.75rem',
              fontSize: '0.82rem',
              borderRadius: '6px',
              border: 'none',
              background: mode === 'manual' ? 'var(--primary)' : 'transparent',
              color: mode === 'manual' ? '#fff' : 'var(--text)',
              fontWeight: mode === 'manual' ? 600 : 400,
            }}
          >
            📁 Upload FIT Walk
          </button>
          <button
            type="button"
            className="btn-secondary"
            onClick={() => setMode('telemetry')}
            style={{
              padding: '0.3rem 0.75rem',
              fontSize: '0.82rem',
              borderRadius: '6px',
              border: 'none',
              background: mode === 'telemetry' ? 'var(--primary)' : 'transparent',
              color: mode === 'telemetry' ? '#fff' : 'var(--text)',
              fontWeight: mode === 'telemetry' ? 600 : 400,
            }}
          >
            📊 Import CGM / Pump CSV
          </button>
        </div>
      </div>

      {!garminEmail && (
        <div style={{ padding: '0.65rem 0.85rem', background: '#f8fafc', border: '1px solid var(--border)', borderRadius: 'var(--radius)', fontSize: '0.82rem', color: 'var(--text-muted)', marginBottom: '1.25rem' }}>
          💡 <strong>Pro Tip:</strong> Connect your Garmin account in <strong>⚙ Settings</strong> to automatically browse and import your walks with a single click.
        </div>
      )}

      <form onSubmit={handleSubmit} className="upload-form">
        {mode === 'telemetry' ? (
          <>
            <div className="field">
              <label htmlFor="telemetry-provider">Telemetry Source Format</label>
              <select
                id="telemetry-provider"
                className="setup-text-input"
                value={telemetryProvider}
                onChange={(e) => setTelemetryProvider(e.target.value)}
                style={{ width: '100%', padding: '0.5rem', fontSize: '0.9rem' }}
              >
                <option value="auto">Auto-Detect (Medtronic CareLink, Dexcom Clarity, or Tandem t:connect)</option>
                <option value="carelink">Medtronic CareLink CSV</option>
                <option value="dexcom">Dexcom Clarity CSV (mg/dL or mmol/L)</option>
                <option value="tandem">Tandem t:connect CSV</option>
              </select>
            </div>
            <div className="field">
              <label htmlFor="telemetry-csv">CSV Telemetry File</label>
              <input
                id="telemetry-csv"
                type="file"
                accept=".csv"
                onChange={(e) => setTelemetryFile(e.target.files[0] ?? null)}
                required
              />
              <span className="hint">
                Imports 24-hour CGM readings, boluses, basal rates, carbs, and pump automation states into Daily Trends and overlapping walks.
              </span>
            </div>
            <button
              type="submit"
              disabled={uploading || !telemetryFile}
              className="btn-primary"
              style={{ padding: '0.6rem 1.4rem', fontSize: '0.95rem' }}
            >
              {uploading ? 'Importing Telemetry…' : 'Import Telemetry CSV'}
            </button>
          </>
        ) : (
        <>
        {mode === 'garmin' && garminEmail ? (
          <div className="field" style={{ background: '#f8fafc', padding: '0.85rem', borderRadius: 'var(--radius)', border: '1px solid var(--border)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
              <label htmlFor="garmin-activity-select" style={{ fontWeight: 600, color: 'var(--text)' }}>
                Select Garmin Activity
              </label>
              <button
                type="button"
                className="btn-secondary"
                onClick={fetchGarminActivities}
                disabled={loadingActivities}
                style={{ padding: '0.2rem 0.55rem', fontSize: '0.75rem' }}
              >
                {loadingActivities ? 'Loading...' : '🔄 Refresh List'}
              </button>
            </div>

            {loadingActivities && activities.length === 0 ? (
              <div style={{ padding: '1rem', textAlign: 'center', color: 'var(--text-muted)', fontSize: '0.85rem' }}>
                Fetching your recent Garmin activities...
              </div>
            ) : activitiesError ? (
              <div style={{ padding: '0.75rem', background: '#fee2e2', color: '#991b1b', borderRadius: '6px', fontSize: '0.82rem' }}>
                {activitiesError}
              </div>
            ) : activities.length === 0 ? (
              <div style={{ padding: '0.75rem', color: 'var(--text-muted)', fontSize: '0.85rem' }}>
                No recent activities found on your Garmin account.
              </div>
            ) : (
              <>
                <select
                  id="garmin-activity-select"
                  className="setup-text-input"
                  value={selectedActivityId}
                  onChange={e => handleActivitySelect(e.target.value)}
                  required
                  style={{ width: '100%', padding: '0.5rem', fontSize: '0.9rem' }}
                >
                  <option value="" disabled>-- Choose an activity from Garmin --</option>
                  {activities.map(act => (
                    <option key={act.activity_id} value={act.activity_id}>
                      {act.already_imported ? '✓ [Imported] ' : ''}
                      {act.name} — {act.date} ({act.distance_km ? `${act.distance_km} km` : ''}{act.duration_min ? `, ${act.duration_min} min` : ''}) [{act.activity_type}]
                    </option>
                  ))}
                </select>

                {selectedActivity && (
                  <div style={{ marginTop: '0.75rem', padding: '0.65rem 0.85rem', background: '#fff', border: '1px solid var(--border)', borderRadius: '6px', fontSize: '0.82rem', display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))', gap: '0.5rem' }}>
                    <div>
                      <span style={{ color: 'var(--text-muted)', display: 'block', fontSize: '0.72rem' }}>Distance</span>
                      <strong>{selectedActivity.distance_km ? `${selectedActivity.distance_km} km` : '—'}</strong>
                    </div>
                    <div>
                      <span style={{ color: 'var(--text-muted)', display: 'block', fontSize: '0.72rem' }}>Duration</span>
                      <strong>{selectedActivity.duration_min ? `${selectedActivity.duration_min} mins` : '—'}</strong>
                    </div>
                    <div>
                      <span style={{ color: 'var(--text-muted)', display: 'block', fontSize: '0.72rem' }}>Avg Heart Rate</span>
                      <strong>{selectedActivity.avg_hr ? `${selectedActivity.avg_hr} bpm` : '—'}</strong>
                    </div>
                    <div>
                      <span style={{ color: 'var(--text-muted)', display: 'block', fontSize: '0.72rem' }}>Status</span>
                      <strong style={{ color: selectedActivity.already_imported ? '#16a34a' : 'var(--primary)' }}>
                        {selectedActivity.already_imported ? 'Already in walks' : 'Ready to import'}
                      </strong>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        ) : (
          <div className="field">
            <label htmlFor="walk-fit">Garmin FIT File</label>
            <input
              id="walk-fit"
              type="file"
              accept=".fit"
              onChange={handleFitChange}
              required
            />
          </div>
        )}

        <div className="field">
          <label htmlFor="walk-name">Walk Name</label>
          <input
            id="walk-name"
            type="text"
            value={name}
            onChange={e => setName(e.target.value)}
            placeholder="e.g. Tongariro Alpine Crossing"
            required={mode === 'garmin'}
          />
        </div>

        <div className="field">
          <label htmlFor="walk-date">Date</label>
          <input
            id="walk-date"
            type="date"
            value={date}
            onChange={e => { setDate(e.target.value); setDateSource(null) }}
            required
          />
          {dateSource === 'fit' && <span className="hint detected">Detected from FIT file</span>}
          {dateSource === 'garmin' && <span className="hint detected" style={{ color: 'var(--primary)' }}>Detected from Garmin activity</span>}
        </div>

        <div className="field">
          <label htmlFor="walk-carelink">CareLink CSV Export <span className="optional">(optional)</span></label>
          <input
            id="walk-carelink"
            type="file"
            accept=".csv"
            onChange={e => setCarelinkFile(e.target.files[0] ?? null)}
          />
          <span className="hint">Optional if CareLink automated sync is enabled or an existing file covers this walk</span>
        </div>

        <div className="field">
          <label htmlFor="walk-gpx">
            GPX Route <span className="optional">(optional)</span>
          </label>
          <input
            id="walk-gpx"
            type="file"
            accept=".gpx"
            onChange={e => setGpxFile(e.target.files[0] ?? null)}
          />
        </div>

        <button
          type="submit"
          disabled={uploading || (mode === 'garmin' && (!selectedActivityId || loadingActivities))}
          className="btn-primary"
          style={{ padding: '0.6rem 1.4rem', fontSize: '0.95rem' }}
        >
          {uploading
            ? (mode === 'garmin' ? 'Importing from Garmin…' : 'Uploading…')
            : (mode === 'garmin' ? 'Import Walk from Garmin' : 'Upload Walk')}
        </button>
        </>
        )}
      </form>
      {message && <p className={message.startsWith('Error') ? 'msg-error' : 'msg-ok'} style={{ marginTop: '1rem' }}>{message}</p>}
    </section>
  )
}
