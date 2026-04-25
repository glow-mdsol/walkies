import { useState, useEffect, useCallback } from 'react'
import UploadForm from './components/UploadForm'
import WalkList from './components/WalkList'
import WalkAnalysisView from './components/WalkAnalysisView'

export default function App() {
  const [walks, setWalks] = useState([])
  const [error, setError] = useState(null)
  const [selectedWalkId, setSelectedWalkId] = useState(null)

  const fetchWalks = useCallback(() => {
    fetch('/api/walks')
      .then(r => r.json())
      .then(setWalks)
      .catch(() => setError('Could not connect to backend'))
  }, [])

  useEffect(() => { fetchWalks() }, [fetchWalks])

  return (
    <div className="app">
      <header className="app-header">
        <h1>Walkies</h1>
      </header>
      <main className="app-main">
        {error && <div className="error-banner">{error}</div>}
        {selectedWalkId ? (
          <WalkAnalysisView walkId={selectedWalkId} onBack={() => setSelectedWalkId(null)} />
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
