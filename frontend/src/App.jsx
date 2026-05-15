import { useState, useEffect, useCallback } from 'react'
import { GoogleLogin } from '@react-oauth/google'
import UploadForm from './components/UploadForm'
import WalkList from './components/WalkList'
import WalkAnalysisView from './components/WalkAnalysisView'
import InsulinSetup, { loadInsulinProfile } from './components/InsulinSetup'
import RouteCompare from './components/RouteCompare'

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
  const [showInsulinSetup, setShowInsulinSetup] = useState(false)
  const [showRouteCompare, setShowRouteCompare] = useState(false)
  const [insulinProfile, setInsulinProfile] = useState(() => loadInsulinProfile())
  const [homeTab, setHomeTab] = useState('walks')

  const apiFetch = useCallback((url, options = {}) => {
    if (!idToken) throw new Error('Not authenticated')
    const headers = {
      ...(options.headers || {}),
      Authorization: `Bearer ${idToken}`,
    }
    return fetch(url, { ...options, headers })
  }, [idToken])

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
          setShowInsulinSetup(false)
          setShowRouteCompare(false)
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

  useEffect(() => { fetchWalks() }, [fetchWalks])

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
    setShowInsulinSetup(false)
    setShowRouteCompare(false)
  }

  const handleInsulinBack = () => {
    setInsulinProfile(loadInsulinProfile())
    setShowInsulinSetup(false)
  }

  const onHome = () => {
    setSelectedWalkId(null)
    setShowInsulinSetup(false)
    setShowRouteCompare(false)
  }

  const isHome = !selectedWalkId && !showInsulinSetup && !showRouteCompare
  const currentWalk = walks.find(w => w.id === selectedWalkId)

  const headerLeft = (() => {
    if (selectedWalkId) return {
      back: () => setSelectedWalkId(null),
      label: currentWalk?.name || currentWalk?.date || '',
    }
    if (showRouteCompare) return { back: () => setShowRouteCompare(false), label: 'Routes' }
    if (showInsulinSetup) return { back: handleInsulinBack, label: 'Insulin Profile' }
    return null
  })()

  return (
    <div className="app">
      <header className="app-header">
        {headerLeft ? (
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', minWidth: 0 }}>
            <button className="btn-header-action" onClick={headerLeft.back}>← Walks</button>
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
            <button className="btn-header-action" onClick={() => setShowInsulinSetup(true)}>
              ⚙ Insulin profile
            </button>
            <button className="btn-header-action" onClick={handleLogout}>
              Sign out
            </button>
          </div>
        )}
      </header>
      <main className="app-main">
        {error && <div className="error-banner">{error}</div>}
        {!idToken ? (
          <section className="card" style={{ maxWidth: '620px', margin: '1rem auto', textAlign: 'center' }}>
            <h2 style={{ fontSize: '1rem' }}>Welcome to Trek GM</h2>
            <p className="analysis-subtitle" style={{ marginBottom: '1rem' }}>
              Sign in with Google to upload walks and keep your analysis data private.
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
          </section>
        ) : showInsulinSetup ? (
          <InsulinSetup />
        ) : showRouteCompare ? (
          <RouteCompare
            apiFetch={apiFetch}
            onSelectWalk={walkId => { setShowRouteCompare(false); setSelectedWalkId(walkId) }}
          />
        ) : selectedWalkId ? (
          <WalkAnalysisView walkId={selectedWalkId} insulinProfile={insulinProfile} apiFetch={apiFetch} />
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
                className={`home-tab${homeTab === 'new-walk' ? ' home-tab--active' : ''}`}
                onClick={() => setHomeTab('new-walk')}
              >
                New Walk
              </button>
            </div>
            {homeTab === 'walks'
              ? <WalkList walks={walks} onDeleted={fetchWalks} onView={setSelectedWalkId} apiFetch={apiFetch} />
              : <UploadForm onUploaded={() => { fetchWalks(); setHomeTab('walks') }} apiFetch={apiFetch} />
            }
          </>
        )}
      </main>
    </div>
  )
}
