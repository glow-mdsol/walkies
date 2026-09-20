import { useState, useEffect, useCallback } from 'react'

export default function SystemLogsSection({ apiFetch }) {
  const [logs, setLogs] = useState([])
  const [total, setTotal] = useState(0)
  const [summary, setSummary] = useState({
    total_logs: 0,
    auth_issues_count: 0,
    connection_issues_count: 0,
    error_count: 0,
    warning_count: 0,
  })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  // Filters
  const [filterMode, setFilterMode] = useState('all') // 'all' | 'auth_only' | 'connection_only' | 'exclude_conn' | 'errors'
  const [serviceFilter, setServiceFilter] = useState('all')
  const [levelFilter, setLevelFilter] = useState('all')
  const [searchQuery, setSearchQuery] = useState('')
  const [autoRefresh, setAutoRefresh] = useState(false)

  // Expanded details row
  const [expandedId, setExpandedId] = useState(null)
  const [copiedId, setCopiedId] = useState(null)

  // Simulate test event state
  const [showSimulate, setShowSimulate] = useState(false)
  const [simLevel, setSimLevel] = useState('ERROR')
  const [simType, setSimType] = useState('auth') // 'auth' | 'connection' | 'sync'
  const [simMessage, setSimMessage] = useState('401 Unauthorized: Garmin session token expired')

  const fetchLogs = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const params = new URLSearchParams()
      params.set('limit', '150')

      if (filterMode === 'auth_only') {
        params.set('auth_only', 'true')
      } else if (filterMode === 'connection_only') {
        params.set('is_connection', 'true')
      } else if (filterMode === 'exclude_conn') {
        params.set('exclude_connection', 'true')
      } else if (filterMode === 'errors') {
        params.set('level', 'ERROR')
      }

      if (serviceFilter !== 'all') {
        params.set('service', serviceFilter)
      }
      if (levelFilter !== 'all' && filterMode !== 'errors') {
        params.set('level', levelFilter)
      }
      if (searchQuery.trim()) {
        params.set('search', searchQuery.trim())
      }

      const res = await apiFetch(`/api/system/logs?${params.toString()}`)
      if (!res.ok) {
        throw new Error(`Failed to fetch logs (${res.status})`)
      }
      const data = await res.json()
      setLogs(data.logs || [])
      setTotal(data.total || 0)
      if (data.summary) {
        setSummary(data.summary)
      }
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }, [apiFetch, filterMode, serviceFilter, levelFilter, searchQuery])

  useEffect(() => {
    fetchLogs()
  }, [fetchLogs])

  useEffect(() => {
    if (!autoRefresh) return
    const interval = setInterval(() => {
      fetchLogs()
    }, 8000)
    return () => clearInterval(interval)
  }, [autoRefresh, fetchLogs])

  const handleClearLogs = async () => {
    if (!window.confirm('Are you sure you want to clear all system logs?')) return
    try {
      const res = await apiFetch('/api/system/logs/clear', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({}),
      })
      if (res.ok) {
        fetchLogs()
      }
    } catch (err) {
      alert(`Error clearing logs: ${err.message}`)
    }
  }

  const handleSimulateSubmit = async (e) => {
    e.preventDefault()
    try {
      let isAuth = false
      let isConn = false
      let category = 'general'

      if (simType === 'auth') {
        isAuth = true
        category = 'auth'
      } else if (simType === 'connection') {
        isConn = true
        category = 'connection'
      } else if (simType === 'sync') {
        category = 'sync'
      }

      const res = await apiFetch('/api/system/logs/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          level: simLevel,
          category,
          service: simType === 'auth' ? 'garmin' : simType === 'connection' ? 'garmin' : 'carelink',
          message: simMessage,
          is_auth_issue: isAuth,
          is_connection_issue: isConn,
          details: {
            simulated_at: new Date().toISOString(),
            sample_payload: 'Diagnostic simulation payload',
          },
        }),
      })
      if (res.ok) {
        setShowSimulate(false)
        fetchLogs()
      }
    } catch (err) {
      alert(`Simulation error: ${err.message}`)
    }
  }

  const copyLog = (log) => {
    const text = `[${log.timestamp}] [${log.level}] [${log.service || 'server'}] ${log.message}\nDetails: ${
      typeof log.details === 'object' ? JSON.stringify(log.details, null, 2) : log.details || 'None'
    }`
    navigator.clipboard.writeText(text)
    setCopiedId(log.id)
    setTimeout(() => setCopiedId(null), 2000)
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: '1.25rem' }}>
      {/* Overview & Quick Metrics */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(170px, 1fr))', gap: '0.85rem' }}>
        <div
          onClick={() => setFilterMode('all')}
          style={{
            background: filterMode === 'all' ? 'var(--primary-subtle)' : '#fff',
            border: `1px solid ${filterMode === 'all' ? 'var(--primary)' : 'var(--border)'}`,
            borderRadius: 'var(--radius)',
            padding: '0.85rem 1rem',
            cursor: 'pointer',
            transition: 'all 0.15s ease',
          }}
        >
          <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase' }}>
            Total Logs
          </div>
          <div style={{ fontSize: '1.45rem', fontWeight: 700, color: 'var(--text)', marginTop: '0.2rem' }}>
            {summary.total_logs}
          </div>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.15rem' }}>All recorded events</div>
        </div>

        <div
          onClick={() => setFilterMode('auth_only')}
          style={{
            background: filterMode === 'auth_only' ? '#fee2e2' : summary.auth_issues_count > 0 ? '#fff5f5' : '#fff',
            border: `1px solid ${filterMode === 'auth_only' ? '#ef4444' : summary.auth_issues_count > 0 ? '#fca5a5' : 'var(--border)'}`,
            borderRadius: 'var(--radius)',
            padding: '0.85rem 1rem',
            cursor: 'pointer',
            transition: 'all 0.15s ease',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <span style={{ fontSize: '0.78rem', color: summary.auth_issues_count > 0 ? '#991b1b' : 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase' }}>
              🔐 Auth Issues
            </span>
            {summary.auth_issues_count > 0 && (
              <span style={{ fontSize: '0.65rem', background: '#dc2626', color: '#fff', padding: '0.1rem 0.45rem', borderRadius: '10px', fontWeight: 700 }}>
                Action Needed
              </span>
            )}
          </div>
          <div style={{ fontSize: '1.45rem', fontWeight: 700, color: summary.auth_issues_count > 0 ? '#dc2626' : 'var(--text)', marginTop: '0.2rem' }}>
            {summary.auth_issues_count}
          </div>
          <div style={{ fontSize: '0.75rem', color: summary.auth_issues_count > 0 ? '#b91c1c' : 'var(--text-muted)', marginTop: '0.15rem' }}>
            Credentials & tokens
          </div>
        </div>

        <div
          onClick={() => setFilterMode('connection_only')}
          style={{
            background: filterMode === 'connection_only' ? '#fef3c7' : '#fff',
            border: `1px solid ${filterMode === 'connection_only' ? '#f59e0b' : 'var(--border)'}`,
            borderRadius: 'var(--radius)',
            padding: '0.85rem 1rem',
            cursor: 'pointer',
            transition: 'all 0.15s ease',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <span style={{ fontSize: '0.78rem', color: 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase' }}>
              📡 Connection (OK)
            </span>
            <span style={{ fontSize: '0.65rem', background: '#e0e7ff', color: '#3730a3', padding: '0.1rem 0.45rem', borderRadius: '10px', fontWeight: 600 }}>
              Transient
            </span>
          </div>
          <div style={{ fontSize: '1.45rem', fontWeight: 700, color: 'var(--text)', marginTop: '0.2rem' }}>
            {summary.connection_issues_count}
          </div>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.15rem' }}>
            Timeouts & gateways
          </div>
        </div>

        <div
          onClick={() => setFilterMode('errors')}
          style={{
            background: filterMode === 'errors' ? '#fef2f2' : '#fff',
            border: `1px solid ${filterMode === 'errors' ? '#dc2626' : 'var(--border)'}`,
            borderRadius: 'var(--radius)',
            padding: '0.85rem 1rem',
            cursor: 'pointer',
            transition: 'all 0.15s ease',
          }}
        >
          <div style={{ fontSize: '0.78rem', color: 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase' }}>
            ⚠️ Errors & Warnings
          </div>
          <div style={{ fontSize: '1.45rem', fontWeight: 700, color: summary.error_count > 0 ? '#dc2626' : 'var(--text)', marginTop: '0.2rem' }}>
            {summary.error_count + summary.warning_count}
          </div>
          <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: '0.15rem' }}>
            {summary.error_count} errors, {summary.warning_count} warnings
          </div>
        </div>
      </div>

      {/* Filter Toolbar */}
      <div style={{ background: '#fff', border: '1px solid var(--border)', borderRadius: 'var(--radius)', padding: '1rem', display: 'flex', flexDirection: 'column', gap: '0.85rem' }}>
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.5rem', alignItems: 'center' }}>
          <span style={{ fontSize: '0.82rem', fontWeight: 600, color: 'var(--text-muted)', marginRight: '0.25rem' }}>Quick Filters:</span>
          
          <button
            type="button"
            className="btn-secondary"
            onClick={() => setFilterMode('all')}
            style={{
              padding: '0.3rem 0.75rem',
              fontSize: '0.8rem',
              borderRadius: '20px',
              backgroundColor: filterMode === 'all' ? 'var(--primary)' : 'transparent',
              color: filterMode === 'all' ? '#fff' : 'var(--text)',
              borderColor: filterMode === 'all' ? 'var(--primary)' : 'var(--border)',
            }}
          >
            📋 All Logs
          </button>

          <button
            type="button"
            className="btn-secondary"
            onClick={() => setFilterMode('auth_only')}
            style={{
              padding: '0.3rem 0.75rem',
              fontSize: '0.8rem',
              borderRadius: '20px',
              backgroundColor: filterMode === 'auth_only' ? '#dc2626' : 'transparent',
              color: filterMode === 'auth_only' ? '#fff' : '#b91c1c',
              borderColor: filterMode === 'auth_only' ? '#dc2626' : '#fca5a5',
              fontWeight: 600,
            }}
          >
            🔐 Auth Issues Only
          </button>

          <button
            type="button"
            className="btn-secondary"
            onClick={() => setFilterMode('connection_only')}
            style={{
              padding: '0.3rem 0.75rem',
              fontSize: '0.8rem',
              borderRadius: '20px',
              backgroundColor: filterMode === 'connection_only' ? '#d97706' : 'transparent',
              color: filterMode === 'connection_only' ? '#fff' : '#b45309',
              borderColor: filterMode === 'connection_only' ? '#d97706' : '#fcd34d',
            }}
          >
            📡 Connection Issues (OK)
          </button>

          <button
            type="button"
            className="btn-secondary"
            onClick={() => setFilterMode('exclude_conn')}
            style={{
              padding: '0.3rem 0.75rem',
              fontSize: '0.8rem',
              borderRadius: '20px',
              backgroundColor: filterMode === 'exclude_conn' ? '#4b5563' : 'transparent',
              color: filterMode === 'exclude_conn' ? '#fff' : 'var(--text)',
              borderColor: filterMode === 'exclude_conn' ? '#4b5563' : 'var(--border)',
            }}
          >
            ⚡ Exclude Transient
          </button>

          <button
            type="button"
            className="btn-secondary"
            onClick={() => setFilterMode('errors')}
            style={{
              padding: '0.3rem 0.75rem',
              fontSize: '0.8rem',
              borderRadius: '20px',
              backgroundColor: filterMode === 'errors' ? '#991b1b' : 'transparent',
              color: filterMode === 'errors' ? '#fff' : '#991b1b',
              borderColor: filterMode === 'errors' ? '#991b1b' : '#f87171',
            }}
          >
            ⚠️ Errors Only
          </button>
        </div>

        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.75rem', alignItems: 'center', justifyContent: 'space-between' }}>
          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.6rem', alignItems: 'center', flex: '1 1 auto' }}>
            <input
              type="text"
              className="setup-text-input"
              placeholder="Search log messages or details..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              style={{ minWidth: '220px', maxWidth: '340px', padding: '0.35rem 0.65rem', fontSize: '0.85rem' }}
            />

            <select
              className="setup-text-input"
              value={serviceFilter}
              onChange={(e) => setServiceFilter(e.target.value)}
              style={{ width: '130px', padding: '0.35rem 0.5rem', fontSize: '0.85rem' }}
            >
              <option value="all">All Services</option>
              <option value="garmin">Garmin</option>
              <option value="carelink">CareLink</option>
              <option value="google_auth">Google Auth</option>
              <option value="server">Server</option>
            </select>

            <select
              className="setup-text-input"
              value={levelFilter}
              onChange={(e) => setLevelFilter(e.target.value)}
              disabled={filterMode === 'errors'}
              style={{ width: '120px', padding: '0.35rem 0.5rem', fontSize: '0.85rem' }}
            >
              <option value="all">All Levels</option>
              <option value="ERROR">ERROR</option>
              <option value="WARNING">WARNING</option>
              <option value="INFO">INFO</option>
              <option value="DEBUG">DEBUG</option>
            </select>
          </div>

          <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
            <label style={{ display: 'flex', alignItems: 'center', gap: '0.35rem', fontSize: '0.82rem', color: 'var(--text-muted)', cursor: 'pointer' }}>
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
              />
              Auto-refresh (8s)
            </label>

            <button
              type="button"
              className="btn-secondary"
              onClick={fetchLogs}
              disabled={loading}
              style={{ padding: '0.35rem 0.75rem', fontSize: '0.82rem' }}
            >
              {loading ? 'Refreshing...' : '🔄 Refresh'}
            </button>

            <button
              type="button"
              className="btn-secondary"
              onClick={() => setShowSimulate(!showSimulate)}
              style={{ padding: '0.35rem 0.75rem', fontSize: '0.82rem', borderColor: 'var(--primary)', color: 'var(--primary)' }}
            >
              🧪 Simulate Event
            </button>

            <button
              type="button"
              className="btn-secondary"
              onClick={handleClearLogs}
              style={{ padding: '0.35rem 0.75rem', fontSize: '0.82rem', color: '#be2f2f', borderColor: 'rgba(190, 47, 47, 0.4)' }}
            >
              Clear Logs
            </button>
          </div>
        </div>

        {/* Simulation Panel */}
        {showSimulate && (
          <form onSubmit={handleSimulateSubmit} style={{ marginTop: '0.5rem', padding: '0.85rem', background: '#f8fafc', border: '1px solid #e2e8f0', borderRadius: 'var(--radius)', display: 'flex', flexWrap: 'wrap', gap: '0.75rem', alignItems: 'flex-end' }}>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
              <label style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-muted)' }}>Issue Category</label>
              <select
                className="setup-text-input"
                value={simType}
                onChange={(e) => {
                  const t = e.target.value
                  setSimType(t)
                  if (t === 'auth') {
                    setSimLevel('ERROR')
                    setSimMessage('401 Unauthorized: Garmin session token expired')
                  } else if (t === 'connection') {
                    setSimLevel('WARNING')
                    setSimMessage('504 Gateway Timeout connecting to Garmin API')
                  } else {
                    setSimLevel('INFO')
                    setSimMessage('Synchronized 32 glucose records from CareLink')
                  }
                }}
                style={{ fontSize: '0.85rem', padding: '0.35rem' }}
              >
                <option value="auth">🔐 Authentication Issue (Action Required)</option>
                <option value="connection">📡 Connection Issue (Transient / OK)</option>
                <option value="sync">ℹ️ Standard Sync Event</option>
              </select>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem' }}>
              <label style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-muted)' }}>Level</label>
              <select
                className="setup-text-input"
                value={simLevel}
                onChange={(e) => setSimLevel(e.target.value)}
                style={{ fontSize: '0.85rem', padding: '0.35rem' }}
              >
                <option value="ERROR">ERROR</option>
                <option value="WARNING">WARNING</option>
                <option value="INFO">INFO</option>
              </select>
            </div>

            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.25rem', flex: '1 1 240px' }}>
              <label style={{ fontSize: '0.75rem', fontWeight: 600, color: 'var(--text-muted)' }}>Simulated Message</label>
              <input
                type="text"
                className="setup-text-input"
                value={simMessage}
                onChange={(e) => setSimMessage(e.target.value)}
                style={{ fontSize: '0.85rem', padding: '0.35rem 0.5rem' }}
                required
              />
            </div>

            <div style={{ display: 'flex', gap: '0.4rem' }}>
              <button type="submit" className="btn-primary" style={{ padding: '0.4rem 0.9rem', fontSize: '0.82rem' }}>
                Emit Test Log
              </button>
              <button type="button" className="btn-secondary" onClick={() => setShowSimulate(false)} style={{ padding: '0.4rem 0.75rem', fontSize: '0.82rem' }}>
                Cancel
              </button>
            </div>
          </form>
        )}
      </div>

      {error && (
        <div style={{ padding: '0.75rem 1rem', background: '#fee2e2', color: '#991b1b', borderRadius: 'var(--radius)', fontSize: '0.85rem' }}>
          {error}
        </div>
      )}

      {/* Log Feed */}
      <div style={{ background: '#fff', border: '1px solid var(--border)', borderRadius: 'var(--radius)', overflow: 'hidden' }}>
        <div style={{ padding: '0.75rem 1rem', background: 'var(--surface-tint)', borderBottom: '1px solid var(--border)', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ fontSize: '0.85rem', fontWeight: 600, color: 'var(--text)' }}>
            Showing {logs.length} of {total} events
          </span>
          {filterMode !== 'all' && (
            <span style={{ fontSize: '0.78rem', color: 'var(--text-muted)' }}>
              Filtered by: <strong>{filterMode.replace('_', ' ')}</strong>
            </span>
          )}
        </div>

        {logs.length === 0 ? (
          <div style={{ padding: '3rem 1rem', textAlign: 'center', color: 'var(--text-muted)' }}>
            <div style={{ fontSize: '2rem', marginBottom: '0.5rem' }}>📋</div>
            <p style={{ fontWeight: 500 }}>No log entries match the selected filters.</p>
            <p style={{ fontSize: '0.82rem', marginTop: '0.25rem' }}>
              {filterMode === 'auth_only'
                ? 'No authentication issues recorded. All credentials and sessions are healthy!'
                : 'Try resetting the filters or emitting a test event.'}
            </p>
          </div>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', divideY: '1px solid var(--border-muted)' }}>
            {logs.map((item) => {
              const isExpanded = expandedId === item.id
              const isError = item.level === 'ERROR' || item.level === 'CRITICAL'
              const isWarn = item.level === 'WARNING'
              const isAuth = item.is_auth_issue || item.category === 'auth'
              const isConn = item.is_connection_issue

              const levelColor = isError ? '#dc2626' : isWarn ? '#d97706' : '#2563eb'
              const levelBg = isError ? '#fee2e2' : isWarn ? '#fef3c7' : '#dbeafe'

              return (
                <div
                  key={item.id}
                  style={{
                    padding: '0.85rem 1rem',
                    borderBottom: '1px solid var(--border-muted)',
                    background: isAuth ? 'rgba(254, 242, 242, 0.45)' : isConn ? 'rgba(254, 252, 232, 0.3)' : '#fff',
                    transition: 'background 0.15s ease',
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', gap: '0.75rem' }}>
                    <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: '0.45rem', minWidth: 0, flex: 1 }}>
                      {/* Level Badge */}
                      <span
                        style={{
                          fontSize: '0.7rem',
                          fontWeight: 700,
                          color: levelColor,
                          background: levelBg,
                          padding: '0.15rem 0.45rem',
                          borderRadius: '4px',
                          textTransform: 'uppercase',
                        }}
                      >
                        {item.level}
                      </span>

                      {/* Service Tag */}
                      {item.service && (
                        <span
                          style={{
                            fontSize: '0.72rem',
                            fontWeight: 600,
                            color: '#475569',
                            background: '#f1f5f9',
                            padding: '0.15rem 0.45rem',
                            borderRadius: '4px',
                          }}
                        >
                          {item.service}
                        </span>
                      )}

                      {/* Classification Badge */}
                      {isAuth && (
                        <span
                          title="Authentication or credential failure"
                          style={{
                            fontSize: '0.72rem',
                            fontWeight: 700,
                            color: '#991b1b',
                            background: '#fee2e2',
                            border: '1px solid #fca5a5',
                            padding: '0.1rem 0.45rem',
                            borderRadius: '4px',
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '0.2rem',
                          }}
                        >
                          🔐 Auth Issue
                        </span>
                      )}

                      {isConn && !isAuth && (
                        <span
                          title="Transient network or gateway timeout (normal/recoverable)"
                          style={{
                            fontSize: '0.72rem',
                            fontWeight: 600,
                            color: '#065f46',
                            background: '#d1fae5',
                            border: '1px solid #a7f3d0',
                            padding: '0.1rem 0.45rem',
                            borderRadius: '4px',
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: '0.2rem',
                          }}
                        >
                          📡 Connection (OK)
                        </span>
                      )}

                      {/* Timestamp */}
                      <span style={{ fontSize: '0.75rem', color: 'var(--text-faint)', marginLeft: 'auto' }}>
                        {new Date(item.timestamp).toLocaleString()}
                      </span>
                    </div>

                    <div style={{ display: 'flex', gap: '0.35rem', flexShrink: 0 }}>
                      <button
                        type="button"
                        onClick={() => copyLog(item)}
                        style={{
                          background: 'none',
                          border: 'none',
                          cursor: 'pointer',
                          fontSize: '0.75rem',
                          color: copiedId === item.id ? 'var(--primary)' : 'var(--text-muted)',
                          padding: '0.2rem 0.4rem',
                        }}
                        title="Copy log text"
                      >
                        {copiedId === item.id ? '✓ Copied' : '📋 Copy'}
                      </button>

                      {item.details && (
                        <button
                          type="button"
                          onClick={() => setExpandedId(isExpanded ? null : item.id)}
                          style={{
                            background: 'none',
                            border: '1px solid var(--border)',
                            borderRadius: '4px',
                            cursor: 'pointer',
                            fontSize: '0.75rem',
                            color: 'var(--text-muted)',
                            padding: '0.15rem 0.45rem',
                          }}
                        >
                          {isExpanded ? 'Hide Trace ▲' : 'View Trace ▼'}
                        </button>
                      )}
                    </div>
                  </div>

                  {/* Log Message */}
                  <div style={{ marginTop: '0.35rem', fontSize: '0.88rem', color: 'var(--text)', wordBreak: 'break-word', fontFamily: 'system-ui, sans-serif' }}>
                    {item.message}
                  </div>

                  {/* Expandable Details / Stack Trace */}
                  {isExpanded && item.details && (
                    <div style={{ marginTop: '0.65rem', background: '#0f172a', color: '#e2e8f0', padding: '0.75rem', borderRadius: '6px', fontSize: '0.78rem', fontFamily: 'monospace', overflowX: 'auto', maxHeight: '300px' }}>
                      <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>
                        {typeof item.details === 'object' ? JSON.stringify(item.details, null, 2) : item.details}
                      </pre>
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        )}
      </div>
    </div>
  )
}
