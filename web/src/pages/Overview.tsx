import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { KpiCard } from '../components/KpiCard'
import { DataTable } from '../components/DataTable'
import { Donut } from '../components/charts/Donut'
import { Timeline, TimelineItem } from '../components/charts/Timeline'
import { colorForKey } from '../lib/colors'

export default function OverviewPage() {
  const api = useApi()
  const { scope, date, stationId, trainId } = usePrefs()
  const [state, setState] = useState<any | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [radar, setRadar] = useState<any[]>([])
  useEffect(() => {
    let live = true
    api.getState(scope, date, { station_id: stationId || undefined, train_id: trainId || undefined }).then(d => { if (!live) return; setState(d) }).catch(e => setErr(String(e)))
    api.getRadar(scope, date, { station_id: stationId || undefined, train_id: trainId || undefined }).then(r => { if (!live) return; setRadar(r.radar || []) }).catch(() => {})
    return () => { live = false }
  }, [api, scope, date, stationId, trainId])

  const kpis = state?.sim_kpis || {}
  const sev = useMemo(() => {
    const m: Record<string, number> = {}
    for (const r of radar) m[r.severity || 'Unknown'] = (m[r.severity || 'Unknown'] || 0) + 1
    return m
  }, [radar])
  const timelineItems: TimelineItem[] = useMemo(() => {
    const rows = (state?.platform_occupancy || []).slice(0, 40)
    return rows.map((r: any) => ({
      y: String(r.station_id ?? ''),
      start: r.arr_platform,
      end: r.dep_platform,
      label: `${r.train_id}`,
      color: colorForKey(String(r.train_id ?? '')),
    }))
  }, [state])
  return (
    <div>
      <h2>Overview</h2>
      <ScopeBar />
      <div className="row">
        <KpiCard label="OTP (%)" value={kpis.otp_pct ?? '—'} />
        <KpiCard label="Avg Delay (min)" value={kpis.avg_delay ?? '—'} />
        <KpiCard label="Risks" value={kpis.total_risks ?? '—'} />
        <KpiCard label="Planned Actions" value={kpis.actions ?? '—'} />
      </div>
      {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>Error: {err}</div>}
      <div className="row" style={{ marginTop: 12 }}>
        <div className="card" style={{ flex: 1, minWidth: 320 }}>
          <div className="hstack"><strong>Risk Severity Mix</strong></div>
          <Donut data={sev} />
        </div>
        <div className="card" style={{ flex: 2, minWidth: 420 }}>
          <div className="hstack"><strong>Platform Occupancy (sample)</strong><span className="spacer" /><span className="muted">top 40</span></div>
          <Timeline items={timelineItems} />
        </div>
      </div>
      <div className="card" style={{ marginTop: 12 }}>
        <div className="hstack"><strong>Platform Occupancy (table)</strong><span className="spacer" /><span className="muted">top 20</span></div>
        <DataTable columns={[
          { key: 'train_id', label: 'Train' },
          { key: 'station_id', label: 'Station' },
          { key: 'arr_platform', label: 'Arr' },
          { key: 'dep_platform', label: 'Dep' }
        ]} rows={(state?.platform_occupancy || []).slice(0, 20)} />
      </div>
    </div>
  )
}
