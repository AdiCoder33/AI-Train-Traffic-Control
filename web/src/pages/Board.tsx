import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { DataTable } from '../components/DataTable'

export default function BoardPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [state, setState] = useState<any | null>(null)
  const [err, setErr] = useState<string | null>(null)
  useEffect(() => {
    let live = true
    api.getState(scope, date).then(d => { if (!live) return; setState(d) }).catch(e => setErr(String(e)))
    return () => { live = false }
  }, [api, scope, date])

  const waiting = useMemo(() => (state?.waiting_ledger || []).slice(0, 100), [state])

  return (
    <div>
      <h2>Live Section Board</h2>
      <ScopeBar />
      {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>Error: {err}</div>}
      <div className="card" style={{ marginTop: 12 }}>
        <div className="hstack"><strong>Waiting Ledger</strong><span className="spacer" /><span className="muted">top 100</span></div>
        <DataTable columns={[
          { key: 'train_id', label: 'Train' },
          { key: 'resource', label: 'Resource' },
          { key: 'id', label: 'ID' },
          { key: 'eta', label: 'ETA' },
          { key: 'reason', label: 'Reason' }
        ]} rows={waiting} />
      </div>
    </div>
  )
}

