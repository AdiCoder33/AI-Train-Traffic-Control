import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { DataTable } from '../components/DataTable'
import { KpiCard } from '../components/KpiCard'
import { Heatmap } from '../components/charts/Heatmap'

export default function RadarPage() {
  const api = useApi()
  const { scope, date, stationId, trainId } = usePrefs()
  const [rows, setRows] = useState<any[]>([])
  const [kpis, setKpis] = useState<Record<string, any>>({})
  const [err, setErr] = useState<string | null>(null)
  useEffect(() => {
    let live = true
    api.getRadar(scope, date, { station_id: stationId || undefined, train_id: trainId || undefined }).then(d => { if (!live) return; setRows(d.radar || []); setKpis(d.risk_kpis || {}) }).catch(e => setErr(String(e)))
    return () => { live = false }
  }, [api, scope, date, stationId, trainId])

  const severity = useMemo(() => {
    const m: Record<string, number> = {}
    for (const r of rows) m[r.severity || 'Unknown'] = (m[r.severity || 'Unknown'] || 0) + 1
    return m
  }, [rows])
  const heat = useMemo(() => {
    // Bucket lead_min into ranges and pivot by severity
    const buckets = [0, 5, 10, 20, 30, 45, 60, 90]
    const bucketLabels = buckets.map((b, i) => i === buckets.length - 1 ? `${b}+` : `${b}-${buckets[i + 1]}`)
    const sevCats = ['Critical', 'High', 'Medium', 'Low', 'Unknown']
    const z = sevCats.map(() => Array(bucketLabels.length).fill(0))
    function idxForLead(lead?: number) {
      const v = Number(lead ?? 0)
      for (let i = 0; i < buckets.length - 1; i++) if (v >= buckets[i] && v < buckets[i + 1]) return i
      return bucketLabels.length - 1
    }
    rows.forEach(r => {
      const sidx = Math.max(0, sevCats.indexOf(r.severity || 'Unknown'))
      const bidx = idxForLead(Number(r.lead_min))
      z[sidx][bidx] += 1
    })
    return { z, x: bucketLabels, y: sevCats }
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
        <Heatmap z={heat.z} x={heat.x} y={heat.y} title="Lead (min) vs Severity" />
      </div>
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
