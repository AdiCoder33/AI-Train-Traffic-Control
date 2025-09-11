import { useEffect, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { DataTable } from '../components/DataTable'

export default function AuditPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [rows, setRows] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)
  useEffect(() => {
    let live = true
    api.getAuditTrail(scope, date).then(d => { if (!live) return; setRows(d.audit_trail || []) }).catch(e => setErr(String(e)))
    return () => { live = false }
  }, [api, scope, date])

  return (
    <div>
      <h2>Audit</h2>
      <ScopeBar />
      {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>Error: {err}</div>}
      <div className="card" style={{ marginTop: 12 }}>
        <DataTable columns={[
          { key: 'ts', label: 'Time' },
          { key: 'who', label: 'User' },
          { key: 'role', label: 'Role' },
          { key: 'decision', label: 'Decision' },
          { key: 'reason', label: 'Reason' }
        ]} rows={rows.slice(0, 100)} />
      </div>
    </div>
  )
}

