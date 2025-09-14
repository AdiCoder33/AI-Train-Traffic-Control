import { useEffect, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { DataTable } from '../components/DataTable'

export default function OpsPage() {
  const api = useApi()
  const { scope, date, trainId } = usePrefs()
  const [feed, setFeed] = useState<any[]>([])
  useEffect(() => {
    let live = true
    api.crewFeed(scope, date, trainId || undefined).then(d => { if (!live) return; setFeed(d.instructions || []) })
    return () => { live = false }
  }, [api, scope, date, trainId])
  return (
    <div>
      <h2>Rolling Stock & Crew</h2>
      <ScopeBar />
      <div className="card"><div className="muted">Crew Feed</div><DataTable columns={[{ key: 'train_id', label: 'Train' }, { key: 'summary', label: 'Instruction' }]} rows={feed} /></div>
      <div className="card" style={{ marginTop: 12 }}>
        <div className="muted">Rake Diagram (placeholder)</div>
        <div className="muted">Crew Duty Clock (placeholder)</div>
      </div>
    </div>
  )
}

