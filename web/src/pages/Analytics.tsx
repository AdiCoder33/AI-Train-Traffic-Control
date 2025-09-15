import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { DataTable } from '../components/DataTable'
import { Bar } from '../components/charts/Bar'
import { Timeseries } from '../components/charts/Timeseries'

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
  const decisionsByType = useMemo(() => {
    const by: Record<string, number> = {}
    audit.forEach(a => { const k = String(a.decision || ''); by[k] = (by[k] || 0) + 1 })
    const labels = Object.keys(by)
    const vals = labels.map(k => by[k])
    return { labels, vals }
  }, [audit])
  const decisionsPerHour = useMemo(() => {
    const by: Record<string, number> = {}
    audit.forEach(a => {
      const t = a.ts ? new Date(a.ts) : null
      if (!t) return
      t.setMinutes(0, 0, 0)
      const k = t.toISOString()
      by[k] = (by[k] || 0) + 1
    })
    const keys = Object.keys(by).sort()
    return [{ name: 'Decisions', x: keys, y: keys.map(k => by[k]) }]
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
      <div className="row" style={{ marginTop: 12 }}>
        <div className="card" style={{ flex: 1, minWidth: 360 }}>
          <div className="muted">Decisions by Type</div>
          <Bar x={decisionsByType.labels} y={decisionsByType.vals} />
        </div>
        <div className="card" style={{ flex: 2, minWidth: 480 }}>
          <div className="muted">Decisions per Hour</div>
          <Timeseries series={decisionsPerHour as any} />
        </div>
      </div>
    </div>
  )
}
