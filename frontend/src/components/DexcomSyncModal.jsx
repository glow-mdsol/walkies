import { useState } from 'react'

export default function DexcomSyncModal({
  apiFetch,
  currentUsername = '',
  currentRegion = 'ous',
  onConnected,
  onClose,
}) {
  const [username, setUsername] = useState(currentUsername || '')
  const [password, setPassword] = useState('')
  const [region, setRegion] = useState(currentRegion || 'ous')
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState(null)
  const [statusMsg, setStatusMsg] = useState(null)

  const handleConnect = async (e) => {
    e.preventDefault()
    if (!apiFetch) return
    setSubmitting(true)
    setError(null)
    setStatusMsg(null)
    try {
      const res = await apiFetch('/api/dexcom/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: username.trim(),
          password,
          region,
        }),
      })
      const payload = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(payload.detail || 'Dexcom Share login failed')
      setStatusMsg(`Connected to Dexcom Share (${region.toUpperCase()}) as ${payload.username}`)
      setPassword('')
      if (onConnected) onConnected(payload)
    } catch (err) {
      setError(err.message || 'Dexcom Share login failed')
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <div
      style={{
        padding: '1rem',
        border: '1px solid var(--border)',
        borderRadius: 'var(--radius)',
        background: 'var(--surface-tint)',
        marginTop: '0.75rem',
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
        <h4 style={{ margin: 0, fontSize: '0.95rem' }}>Connect Dexcom Share (Direct Cloud CGM Sync)</h4>
        {onClose && (
          <button type="button" className="btn-secondary" onClick={onClose} style={{ padding: '0.2rem 0.55rem', fontSize: '0.78rem' }}>
            ✕
          </button>
        )}
      </div>
      <form onSubmit={handleConnect} style={{ display: 'flex', flexDirection: 'column', gap: '0.65rem' }}>
        <div className="field" style={{ margin: 0 }}>
          <label style={{ fontSize: '0.82rem' }}>Dexcom Username / Email / Phone</label>
          <input
            type="text"
            className="setup-text-input"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="Dexcom account username"
            required
          />
        </div>
        <div className="field" style={{ margin: 0 }}>
          <label style={{ fontSize: '0.82rem' }}>Dexcom Password</label>
          <input
            type="password"
            className="setup-text-input"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="Dexcom account password"
            required
          />
        </div>
        <div className="field" style={{ margin: 0 }}>
          <label style={{ fontSize: '0.82rem' }}>Dexcom Share Server Region</label>
          <select
            className="setup-text-input"
            value={region}
            onChange={(e) => setRegion(e.target.value)}
          >
            <option value="ous">International / Outside US (shareous1.dexcom.com)</option>
            <option value="us">United States (share2.dexcom.com)</option>
          </select>
        </div>
        {error && <p className="msg-error" style={{ margin: 0, fontSize: '0.82rem' }}>{error}</p>}
        {statusMsg && <p className="msg-ok" style={{ margin: 0, fontSize: '0.82rem' }}>{statusMsg}</p>}
        <div style={{ display: 'flex', gap: '0.5rem' }}>
          <button
            type="submit"
            className="btn-primary"
            disabled={submitting}
            style={{ padding: '0.45rem 1rem', fontSize: '0.85rem' }}
          >
            {submitting ? 'Connecting...' : 'Connect Dexcom Share'}
          </button>
        </div>
      </form>
    </div>
  )
}
