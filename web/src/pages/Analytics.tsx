import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { DataTable } from '../components/DataTable'

export default function AnalyticsPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [audit, setAudit] = useState<any[]>([])
  const [recs, setRecs] = useState<any[]>([])
  useEffect(() => {
    let live = true
    api.getAuditTrail(scope, date).then(d => { if (!live) return; setAudit(d.audit_trail || []) }).catch(() => {})
    api.getRecommendations(scope, date).then(d => { if (!live) return; setRecs(d.rec_plan || []) }).catch(() => {})
    return () => { live = false }
  }, [api, scope, date])
  const overrideMining = useMemo(() => {
    const byAction: Record<string, { rec?: any; count: number }> = {}
    for (const a of audit) {
      const key = String(a.action_id || '')
      if (!byAction[key]) byAction[key] = { count: 0 }
      byAction[key].count += 1
    }
    return Object.entries(byAction).map(([k, v]) => ({ action_id: k, decisions: v.count }))
  }, [audit])

  return (
    <div>
      <h2>Analytics & Planning</h2>
      <ScopeBar />
      <div className="row">
        <div className="card" style={{ flex: 1, minWidth: 360 }}>
          <div className="muted">Override Mining</div>
          <DataTable columns={[{ key: 'action_id', label: 'Action' }, { key: 'decisions', label: 'Decisions' }]} rows={overrideMining.slice(0, 50)} />
        </div>
        <div className="card" style={{ flex: 1, minWidth: 360 }}>
          <div className="muted">Recommendations (sample)</div>
          <DataTable columns={[{ key: 'train_id', label: 'Train' }, { key: 'type', label: 'Type' }, { key: 'reason', label: 'Reason' }, { key: 'minutes', label: 'Min' }]} rows={recs.slice(0, 50)} />
        </div>
      </div>
    </div>
  )
}

