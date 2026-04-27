import { useState, useEffect, useCallback } from 'react'
import UploadForm from './components/UploadForm'
import WalkList from './components/WalkList'
import WalkAnalysisView from './components/WalkAnalysisView'
import InsulinSetup, { loadInsulinProfile } from './components/InsulinSetup'
import RouteCompare from './components/RouteCompare'

export default function App() {
  const [walks, setWalks] = useState([])
  const [error, setError] = useState(null)
  const [selectedWalkId, setSelectedWalkId] = useState(null)
  const [showInsulinSetup, setShowInsulinSetup] = useState(false)
  const [showRouteCompare, setShowRouteCompare] = useState(false)
  const [insulinProfile, setInsulinProfile] = useState(() => loadInsulinProfile())
  const [homeTab, setHomeTab] = useState('walks')

  const fetchWalks = useCallback(() => {
    fetch('/api/walks')
      .then(r => r.json())
      .then(setWalks)
      .catch(() => setError('Could not connect to backend'))
  }, [])

  useEffect(() => { fetchWalks() }, [fetchWalks])

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
          </div>
        )}
      </header>
      <main className="app-main">
        {error && <div className="error-banner">{error}</div>}
        {showInsulinSetup ? (
          <InsulinSetup />
        ) : showRouteCompare ? (
          <RouteCompare
            onSelectWalk={walkId => { setShowRouteCompare(false); setSelectedWalkId(walkId) }}
          />
        ) : selectedWalkId ? (
          <WalkAnalysisView walkId={selectedWalkId} insulinProfile={insulinProfile} />
        ) : (
          <>
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
              ? <WalkList walks={walks} onDeleted={fetchWalks} onView={setSelectedWalkId} />
              : <UploadForm onUploaded={() => { fetchWalks(); setHomeTab('walks') }} />
            }
          </>
        )}
      </main>
    </div>
  )
}
