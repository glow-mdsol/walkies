import { useState, useEffect, useCallback } from 'react'
import UploadForm from './components/UploadForm'
import WalkList from './components/WalkList'
import WalkAnalysisView from './components/WalkAnalysisView'
import InsulinSetup, { loadInsulinProfile } from './components/InsulinSetup'

export default function App() {
  const [walks, setWalks] = useState([])
  const [error, setError] = useState(null)
  const [selectedWalkId, setSelectedWalkId] = useState(null)
  const [showInsulinSetup, setShowInsulinSetup] = useState(false)
  const [insulinProfile, setInsulinProfile] = useState(() => loadInsulinProfile())

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

  return (
    <div className="app">
      <header className="app-header">
        <h1>Walkies</h1>
        {!selectedWalkId && !showInsulinSetup && (
          <button className="btn-header-action" onClick={() => setShowInsulinSetup(true)}>
            ⚙ Insulin profile
          </button>
        )}
      </header>
      <main className="app-main">
        {error && <div className="error-banner">{error}</div>}
        {showInsulinSetup ? (
          <InsulinSetup onBack={handleInsulinBack} />
        ) : selectedWalkId ? (
          <WalkAnalysisView walkId={selectedWalkId} insulinProfile={insulinProfile} onBack={() => setSelectedWalkId(null)} />
        ) : (
          <>
            <UploadForm onUploaded={fetchWalks} />
            <WalkList walks={walks} onDeleted={fetchWalks} onView={setSelectedWalkId} />
          </>
        )}
      </main>
    </div>
  )
}
