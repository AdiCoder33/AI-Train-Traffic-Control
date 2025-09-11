import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { DataTable } from '../components/DataTable'
import { KpiCard } from '../components/KpiCard'

export default function RadarPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [rows, setRows] = useState<any[]>([])
  const [kpis, setKpis] = useState<Record<string, any>>({})
  const [err, setErr] = useState<string | null>(null)
  useEffect(() => {
    let live = true
    api.getRadar(scope, date).then(d => { if (!live) return; setRows(d.radar || []); setKpis(d.risk_kpis || {}) }).catch(e => setErr(String(e)))
    return () => { live = false }
  }, [api, scope, date])

  const severity = useMemo(() => {
    const m: Record<string, number> = {}
    for (const r of rows) m[r.severity || 'Unknown'] = (m[r.severity || 'Unknown'] || 0) + 1
    return m
  }, [rows])

  return (
    <div>
      <h2>Radar</h2>
      <ScopeBar />
      <div className="row">
        {Object.entries(severity).map(([k, v]) => (
          <KpiCard key={k} label={String(k)} value={v} />
        ))}
        {kpis.total_risks != null && <KpiCard label="Total Risks" value={kpis.total_risks} />}
        {kpis.avg_lead_min != null && <KpiCard label="Avg Lead (min)" value={kpis.avg_lead_min} />}
      </div>
      {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>Error: {err}</div>}
      <div className="card" style={{ marginTop: 12 }}>
        <DataTable columns={[
          { key: 'type', label: 'Type' },
          { key: 'severity', label: 'Severity' },
          { key: 'lead_min', label: 'Lead (min)' },
          { key: 'train_id', label: 'Train' },
          { key: 'station_id', label: 'At/Block' }
        ]} rows={rows.slice(0, 50)} />
      </div>
    </div>
  )
}

