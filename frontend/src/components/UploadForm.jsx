import { useState } from 'react'

const today = () => new Date().toISOString().slice(0, 10)

export default function UploadForm({ onUploaded }) {
  const [name, setName] = useState('')
  const [date, setDate] = useState(today)
  const [dateSource, setDateSource] = useState(null) // 'fit' | null
  const [fitFile, setFitFile] = useState(null)
  const [carelinkFile, setCarelinkFile] = useState(null)
  const [gpxFile, setGpxFile] = useState(null)
  const [uploading, setUploading] = useState(false)
  const [message, setMessage] = useState(null)

  const handleFitChange = async (e) => {
    const file = e.target.files[0] ?? null
    setFitFile(file)
    setDateSource(null)
    if (!file) return

    const form = new FormData()
    form.append('file', file)
    try {
      const res = await fetch('/api/walks/parse-fit-date', { method: 'POST', body: form })
      if (res.ok) {
        const { date: detected } = await res.json()
        setDate(detected)
        setDateSource('fit')
      }
    } catch {
      // silently ignore — user can enter date manually
    }
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    setUploading(true)
    setMessage(null)

    const form = new FormData()
    form.append('date', date)
    form.append('name', name)
    form.append('files', fitFile)
    if (carelinkFile) form.append('files', carelinkFile)
    if (gpxFile) form.append('files', gpxFile)

    try {
      const res = await fetch('/api/walks/upload', { method: 'POST', body: form })
      if (!res.ok) throw new Error((await res.json()).detail)
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
    } catch (err) {
      setMessage(`Error: ${err.message}`)
    } finally {
      setUploading(false)
    }
  }

  return (
    <section className="card upload-card">
      <h2>Add walk</h2>
      <form onSubmit={handleSubmit} className="upload-form">
        <div className="field">
          <label htmlFor="walk-name">Name <span className="optional">(optional)</span></label>
          <input
            id="walk-name"
            type="text"
            value={name}
            onChange={e => setName(e.target.value)}
            placeholder="e.g. Tongariro Alpine Crossing"
          />
        </div>
        <div className="field">
          <label htmlFor="walk-fit">Garmin FIT file</label>
          <input
            id="walk-fit"
            type="file"
            accept=".fit"
            onChange={handleFitChange}
            required
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
        </div>
        <div className="field">
          <label htmlFor="walk-carelink">Carelink CSV export</label>
          <input
            id="walk-carelink"
            type="file"
            accept=".csv"
            onChange={e => setCarelinkFile(e.target.files[0] ?? null)}
          />
          <span className="hint">Optional if an existing loaded Carelink export already covers this walk</span>
        </div>
        <div className="field">
          <label htmlFor="walk-gpx">
            GPX route <span className="optional">(optional)</span>
          </label>
          <input
            id="walk-gpx"
            type="file"
            accept=".gpx"
            onChange={e => setGpxFile(e.target.files[0] ?? null)}
          />
        </div>
        <button type="submit" disabled={uploading} className="btn-primary">
          {uploading ? 'Uploading…' : 'Upload'}
        </button>
      </form>
      {message && <p className={message.startsWith('Error') ? 'msg-error' : 'msg-ok'}>{message}</p>}
    </section>
  )
}
