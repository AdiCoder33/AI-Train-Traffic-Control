import { useEffect, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'

export default function CrewPage() {
  const api = useApi()
  const { scope, date, trainId } = usePrefs()
  const [rows, setRows] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)
  useEffect(() => {
    let live = true
    api.crewFeed(scope, date, trainId || undefined).then(d => { if (!live) return; setRows(d.instructions || []) }).catch(e => setErr(String(e)))
    return () => { live = false }
  }, [api, scope, date, trainId])
  return (
    <div>
      <h2>Crew Feed</h2>
      <ScopeBar />
      {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>Error: {err}</div>}
      <div className="row">
        {rows.map((x, i) => (
          <div key={i} className="card" style={{ minWidth: 280 }}>
            <div className="muted">Train {x.train_id}</div>
            <div style={{ fontSize: 18 }}>{x.summary}</div>
          </div>
        ))}
      </div>
    </div>
  )
}

