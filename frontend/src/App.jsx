import { useState, useEffect, useCallback } from 'react'
import { GoogleLogin } from '@react-oauth/google'
import UploadForm from './components/UploadForm'
import WalkList from './components/WalkList'
import WalkAnalysisView from './components/WalkAnalysisView'
import DailyAggregateView from './components/DailyAggregateView'
import UserSettings from './components/UserSettings'
import RouteCompare from './components/RouteCompare'
import PersonasSection from './components/PersonasSection'

const AUTH_TOKEN_KEY = 'walkies.google.idToken'
const GOOGLE_CLIENT_ID_CONFIGURED = Boolean(import.meta.env.VITE_GOOGLE_CLIENT_ID)

function parseJwtPayload(token) {
  try {
    const payload = token.split('.')[1]
    const normalized = payload.replace(/-/g, '+').replace(/_/g, '/')
    const decoded = decodeURIComponent(atob(normalized).split('').map((c) => `%${(`00${c.charCodeAt(0).toString(16)}`).slice(-2)}`).join(''))
    return JSON.parse(decoded)
  } catch {
    return null
  }
}

export default function App() {
  const [walks, setWalks] = useState([])
  const [error, setError] = useState(null)
  const [idToken, setIdToken] = useState(() => window.localStorage.getItem(AUTH_TOKEN_KEY) || null)
  const [userProfile, setUserProfile] = useState(() => parseJwtPayload(window.localStorage.getItem(AUTH_TOKEN_KEY) || ''))
  const [selectedWalkId, setSelectedWalkId] = useState(null)
  const [showSettings, setShowSettings] = useState(false)
  const [showRouteCompare, setShowRouteCompare] = useState(false)
  const [userSettings, setUserSettings] = useState(null)
  const [homeTab, setHomeTab] = useState('walks')
  const [syncingGarmin, setSyncingGarmin] = useState(false)
  const [syncingCarelink, setSyncingCarelink] = useState(false)
  const [syncingDexcom, setSyncingDexcom] = useState(false)
  const [syncStartDate, setSyncStartDate] = useState('')
  const [syncEndDate, setSyncEndDate] = useState('')
  const [sharedDailyPayload, setSharedDailyPayload] = useState(null)
  const [shareToken, setShareToken] = useState(() => {
    const params = new URLSearchParams(window.location.search)
    return params.get('share') || null
  })

  useEffect(() => {
    if (!shareToken) {
      setSharedDailyPayload(null)
      return
    }
    fetch(`/api/shared/${encodeURIComponent(shareToken)}/analysis`)
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => {
        if (data && (data.share_type === 'daily' || data.share_type === 'range')) {
          setSharedDailyPayload(data)
        } else {
          setSharedDailyPayload(null)
        }
      })
      .catch(() => setSharedDailyPayload(null))
  }, [shareToken])

  const apiFetch = useCallback((url, options = {}) => {
    if (!idToken) throw new Error('Not authenticated')
    const headers = {
      ...(options.headers || {}),
      Authorization: `Bearer ${idToken}`,
    }
    return fetch(url, { ...options, headers })
  }, [idToken])

  const buildSyncRangeBody = () => {
    const body = {}
    if (syncStartDate) body.start_date = syncStartDate
    if (syncEndDate) body.end_date = syncEndDate
    return body
  }

  const handleGarminSync = async () => {
    setSyncingGarmin(true)
    setError(null)
    try {
      const res = await apiFetch('/api/garmin/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildSyncRangeBody()),
      })
      const payload = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(payload.detail || 'Sync failed')

      const count = payload.imported?.length ?? 0
      const epochsCount = payload.epochs_upserted ?? 0
      if (count > 0 || epochsCount > 0) {
        alert(`Garmin sync complete: ${count} new walk(s) and ${epochsCount} 15-min epochs synced!`)
        fetchWalks()
      } else {
        alert('All Garmin activities and epochs are already up-to-date.')
      }
    } catch (err) {
      setError(err.message || 'Sync failed')
    } finally {
      setSyncingGarmin(false)
    }
  }

  const handleCarelinkSync = async () => {
    setSyncingCarelink(true)
    setError(null)
    try {
      const res = await apiFetch('/api/carelink/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildSyncRangeBody()),
      })
      const payload = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(payload.detail || 'Sync failed')

      const count = payload.synced_walks?.length ?? 0
      const eventsCount = payload.events_upserted ?? 0
      if (count > 0 || eventsCount > 0) {
        alert(`CareLink sync complete: ${eventsCount} telemetry events and ${count} walk(s) updated!`)
        fetchWalks()
      } else {
        alert('All Garmin walks and daily views are already up-to-date with CareLink data.')
      }
    } catch (err) {
      setError(err.message || 'Sync failed')
    } finally {
      setSyncingCarelink(false)
    }
  }

  const handleDexcomSync = async () => {
    setSyncingDexcom(true)
    setError(null)
    try {
      const res = await apiFetch('/api/dexcom/sync', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildSyncRangeBody()),
      })
      const payload = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(payload.detail || 'Dexcom sync failed')
      alert(`Dexcom sync complete: ${payload.events_upserted ?? 0} CGM readings synced!`)
      fetchWalks()
    } catch (err) {
      setError(err.message || 'Dexcom sync failed')
    } finally {
      setSyncingDexcom(false)
    }
  }

  const fetchSettings = useCallback(() => {
    if (!idToken) {
      setUserSettings(null)
      return
    }
    apiFetch('/api/user/settings')
      .then(async (r) => {
        if (!r.ok) {
          throw new Error('Could not load user settings')
        }
        return r.json()
      })
      .then(setUserSettings)
      .catch((err) => setError(err.message || 'Could not load user settings'))
  }, [apiFetch, idToken])

  const fetchWalks = useCallback(() => {
    if (!idToken) {
      setWalks([])
      return
    }
    apiFetch('/api/walks')
      .then(async (r) => {
        if (r.status === 401) {
          setIdToken(null)
          setUserProfile(null)
          setWalks([])
          setSelectedWalkId(null)
          setShowSettings(false)
          setShowRouteCompare(false)
          setUserSettings(null)
          throw new Error('Session expired. Please sign in again.')
        }
        if (!r.ok) {
          throw new Error('Could not connect to backend')
        }
        return r.json()
      })
      .then(setWalks)
      .catch((err) => setError(err.message || 'Could not connect to backend'))
  }, [apiFetch, idToken])

  useEffect(() => {
    fetchWalks()
    fetchSettings()
  }, [fetchWalks, fetchSettings, idToken])

  useEffect(() => {
    if (!idToken) {
      window.localStorage.removeItem(AUTH_TOKEN_KEY)
      return
    }
    window.localStorage.setItem(AUTH_TOKEN_KEY, idToken)
  }, [idToken])

  const handleLoginSuccess = (credentialResponse) => {
    const token = credentialResponse?.credential
    if (!token) return
    setIdToken(token)
    setUserProfile(parseJwtPayload(token))
    setError(null)
  }

  const handleLogout = () => {
    setIdToken(null)
    setUserProfile(null)
    setWalks([])
    setSelectedWalkId(null)
    setShowSettings(false)
    setShowRouteCompare(false)
    setUserSettings(null)
  }

  const handleSaveSettings = async (updatedSettings) => {
    const res = await apiFetch('/api/user/settings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(updatedSettings),
    })
    if (!res.ok) {
      const payload = await res.json().catch(() => ({}))
      throw new Error(payload.detail || 'Failed to save settings')
    }
    fetchSettings()
    setShowSettings(false)
  }

  const handleDeleteAccount = async () => {
    const res = await apiFetch('/api/user/account', {
      method: 'DELETE',
    })
    if (!res.ok) {
      const payload = await res.json().catch(() => ({}))
      throw new Error(payload.detail || 'Failed to delete account')
    }
    handleLogout()
  }

  const isSettingsLoading = idToken && !userSettings
  const isRegistrationRequired = idToken && userSettings && userSettings.updated_at === null

  const isHome = idToken && !selectedWalkId && !showSettings && !showRouteCompare && !isRegistrationRequired && !shareToken
  const currentWalk = walks.find(w => w.id === selectedWalkId)

  const headerLeft = (() => {
    if (shareToken) return {
      back: () => {
        setShareToken(null)
        const url = new URL(window.location)
        url.searchParams.delete('share')
        window.history.replaceState({}, '', url.pathname + url.search)
      },
      label: 'Shared Walk',
    }
    if (selectedWalkId) return {
      back: () => setSelectedWalkId(null),
      label: currentWalk?.name || currentWalk?.date || '',
    }
    if (showRouteCompare) return { back: () => setShowRouteCompare(false), label: 'Routes' }
    if (showSettings) return { back: () => { setShowSettings(false); fetchSettings() }, label: 'Settings' }
    return null
  })()

  return (
    <div className="app">
      <header className="app-header">
        {headerLeft ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', minWidth: 0 }}>
            <button className="btn-header-action" onClick={headerLeft.back}>
              {shareToken ? '← Home' : '← Walks'}
            </button>
            {headerLeft.label && (
              <span style={{ color: 'white', fontWeight: 600, fontSize: '1rem', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {headerLeft.label}
              </span>
            )}
          </div>
        ) : (
          <h1>Trek GM</h1>
        )}
        {isHome && (
          <div style={{ display: 'flex', gap: '0.5rem' }}>
            <button className="btn-header-action" onClick={() => setShowRouteCompare(true)}>
              Routes
            </button>
            <button className="btn-header-action" onClick={() => setShowSettings(true)}>
              ⚙ Settings
            </button>
            <button className="btn-header-action" onClick={handleLogout}>
              Sign out
            </button>
          </div>
        )}
        {isRegistrationRequired && (
          <div style={{ display: 'flex', gap: '0.5rem' }}>
            <button className="btn-header-action" onClick={handleLogout}>
              Sign out
            </button>
          </div>
        )}
      </header>
      <main className="app-main">
        {error && <div className="error-banner">{error}</div>}
        {shareToken ? (
          sharedDailyPayload ? (
            <DailyAggregateView
              shareToken={shareToken}
              sharedInitialData={sharedDailyPayload}
              userSettings={sharedDailyPayload.user_settings}
              apiFetch={null}
            />
          ) : (
            <WalkAnalysisView
              shareToken={shareToken}
              insulinProfile={null}
              apiFetch={null}
            />
          )
        ) : !idToken ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
          <section className="card" style={{ textAlign: 'center', padding: '2.5rem 2rem', background: 'linear-gradient(180deg, var(--surface) 0%, var(--surface-tint) 100%)', maxWidth: '640px', margin: '0 auto', width: '100%' }}>
              <h2 style={{ fontSize: '1.8rem', fontWeight: 800, color: 'var(--primary)', textTransform: 'none', letterSpacing: 'normal', marginBottom: '0.75rem' }}>
                Trek GM
              </h2>
              <p className="analysis-subtitle" style={{ fontSize: '1.05rem', color: 'var(--text-muted)', marginBottom: '2rem', maxWidth: '480px', margin: '0 auto 2rem' }}>
                Analyze your walks, track heart rate effort, monitor weather conditions, and correlate diabetes therapy data in a sleek, integrated dashboard.
              </p>

              <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem', borderTop: '1px solid var(--border)', paddingTop: '2rem', marginTop: '1rem' }}>
                {/* Access Account Section */}
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '0.75rem' }}>
                  <h3 style={{ fontSize: '0.95rem', fontWeight: 600, color: 'var(--text)' }}>Access Your Account</h3>
                  <p className="hint" style={{ textAlign: 'center', maxWidth: '300px', marginBottom: '0.5rem' }}>
                    Sign in to securely upload raw walk files (.fit, .csv, .gpx) and view your history.
                  </p>
                  {GOOGLE_CLIENT_ID_CONFIGURED ? (
                    <div style={{ display: 'flex', justifyContent: 'center' }}>
                      <GoogleLogin onSuccess={handleLoginSuccess} onError={() => setError('Google login failed')} />
                    </div>
                  ) : (
                    <p className="msg-error" style={{ margin: '0 auto', maxWidth: 420 }}>
                      Missing VITE_GOOGLE_CLIENT_ID. Configure frontend env before enabling login.
                    </p>
                  )}
                </div>

                <div style={{ borderTop: '1px solid var(--border-muted)', margin: '0.5rem 0' }}></div>

                {/* Public Share Section */}
                <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '0.75rem' }}>
                  <h3 style={{ fontSize: '0.95rem', fontWeight: 600, color: 'var(--text)' }}>View Shared Route</h3>
                  <p className="hint" style={{ textAlign: 'center', maxWidth: '320px', marginBottom: '0.5rem' }}>
                    Have a route share code? Enter it below to inspect the interactive analysis.
                  </p>
                  <form 
                    onSubmit={(e) => {
                      e.preventDefault()
                      const val = e.currentTarget.elements.shareCode.value.trim()
                      if (val) {
                        setShareToken(val)
                      }
                    }}
                    style={{ display: 'flex', gap: '0.5rem', width: '100%', maxWidth: '360px' }}
                  >
                    <input
                      name="shareCode"
                      type="text"
                      placeholder="Enter share code or token"
                      required
                      style={{
                        flex: 1,
                        padding: '0.55rem 0.75rem',
                        border: '1px solid var(--border)',
                        borderRadius: 'var(--radius)',
                        fontSize: '0.9rem',
                        background: 'var(--surface)',
                        color: 'var(--text)',
                      }}
                    />
                    <button className="btn-primary" type="submit" style={{ alignSelf: 'stretch', padding: '0.55rem 1.2rem' }}>
                      View Walk
                    </button>
                  </form>
                </div>
              </div>
            </section>
            <PersonasSection />
          </div>
        ) : isSettingsLoading ? (
          <div style={{ textAlign: 'center', margin: '3rem auto', color: 'var(--text-muted)' }}>
            Loading user profile...
          </div>
        ) : isRegistrationRequired ? (
          <UserSettings
            userSettings={userSettings}
            onSave={handleSaveSettings}
            isRegistration={true}
            apiFetch={apiFetch}
          />
        ) : showSettings ? (
          <UserSettings
            userSettings={userSettings}
            onSave={handleSaveSettings}
            isRegistration={false}
            onBack={() => setShowSettings(false)}
            onDeleteAccount={handleDeleteAccount}
            apiFetch={apiFetch}
          />
        ) : showRouteCompare ? (
          <RouteCompare
            apiFetch={apiFetch}
            onSelectWalk={walkId => { setShowRouteCompare(false); setSelectedWalkId(walkId) }}
          />
        ) : selectedWalkId ? (
          <WalkAnalysisView
            walkId={selectedWalkId}
            insulinProfile={userSettings?.insulin_profile}
            userSettings={userSettings}
            apiFetch={apiFetch}
            onRenamed={fetchWalks}
          />
        ) : (
          <>
            <section className="card" style={{ padding: '0.75rem 1rem' }}>
              <p style={{ margin: 0, color: 'var(--text-muted)', fontSize: '0.85rem' }}>
                Signed in as <strong>{userProfile?.name || userProfile?.email || 'Google user'}</strong>
              </p>
            </section>
            <div className="home-tabs">
              <button
                className={`home-tab${homeTab === 'walks' ? ' home-tab--active' : ''}`}
                onClick={() => setHomeTab('walks')}
              >
                Walks
              </button>
              <button
                className={`home-tab${homeTab === 'daily' ? ' home-tab--active' : ''}`}
                onClick={() => setHomeTab('daily')}
              >
                Daily & Trends
              </button>
              <button
                className={`home-tab${homeTab === 'new-walk' ? ' home-tab--active' : ''}`}
                onClick={() => setHomeTab('new-walk')}
              >
                New Walk / Import
              </button>
            </div>
            {(homeTab === 'walks' || homeTab === 'daily') && (userSettings?.garmin_email || userSettings?.carelink_email || userSettings?.dexcom_username) && (
              <section className="card" style={{ padding: '0.75rem 1rem', marginTop: '1rem', display: 'flex', flexDirection: 'column', gap: '0.6rem' }}>
                <div style={{ display: 'flex', flexWrap: 'wrap', justifyContent: 'space-between', alignItems: 'center', gap: '0.5rem' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', flexWrap: 'wrap', fontSize: '0.83rem', color: 'var(--text-muted)' }}>
                    <span>Sync Window (default 7d, max 30d):</span>
                    <input
                      type="date"
                      value={syncStartDate}
                      onChange={(e) => setSyncStartDate(e.target.value)}
                      style={{ padding: '0.25rem 0.45rem', borderRadius: 'var(--radius)', border: '1px solid var(--border)', fontSize: '0.8rem' }}
                    />
                    <span>to</span>
                    <input
                      type="date"
                      value={syncEndDate}
                      onChange={(e) => setSyncEndDate(e.target.value)}
                      style={{ padding: '0.25rem 0.45rem', borderRadius: 'var(--radius)', border: '1px solid var(--border)', fontSize: '0.8rem' }}
                    />
                    {(syncStartDate || syncEndDate) && (
                      <button
                        className="btn-secondary"
                        onClick={() => { setSyncStartDate(''); setSyncEndDate('') }}
                        style={{ padding: '0.2rem 0.5rem', fontSize: '0.78rem' }}
                      >
                        Reset (7d)
                      </button>
                    )}
                  </div>
                  <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
                    {userSettings?.garmin_email && (
                      <button
                        className="btn-primary"
                        onClick={handleGarminSync}
                        disabled={syncingGarmin}
                        style={{ padding: '0.45rem 0.9rem', fontSize: '0.84rem' }}
                      >
                        {syncingGarmin ? 'Syncing Garmin...' : '🔄 Sync Garmin (Walks + 15m Epochs)'}
                      </button>
                    )}
                    {userSettings?.carelink_email && (
                      <button
                        className="btn-primary"
                        onClick={handleCarelinkSync}
                        disabled={syncingCarelink}
                        style={{ padding: '0.45rem 0.9rem', fontSize: '0.84rem' }}
                      >
                        {syncingCarelink ? 'Syncing CareLink...' : '🔄 Sync CareLink'}
                      </button>
                    )}
                    {userSettings?.dexcom_username && (
                      <button
                        className="btn-primary"
                        onClick={handleDexcomSync}
                        disabled={syncingDexcom}
                        style={{ padding: '0.45rem 0.9rem', fontSize: '0.84rem' }}
                      >
                        {syncingDexcom ? 'Syncing Dexcom...' : '🔄 Sync Dexcom CGM'}
                      </button>
                    )}
                  </div>
                </div>
              </section>
            )}
            {homeTab === 'walks' ? (
              <WalkList
                walks={walks}
                onDeleted={fetchWalks}
                onView={setSelectedWalkId}
                apiFetch={apiFetch}
                glucoseUnit={userSettings?.glucose_unit || 'mmol/L'}
              />
            ) : homeTab === 'daily' ? (
              <DailyAggregateView
                apiFetch={apiFetch}
                userSettings={userSettings}
                onSelectWalk={setSelectedWalkId}
              />
            ) : (
              <UploadForm
                onUploaded={() => { fetchWalks(); setHomeTab('walks') }}
                apiFetch={apiFetch}
                garminEmail={userSettings?.garmin_email}
              />
            )}
          </>
        )}
      </main>
    </div>
  )
}
