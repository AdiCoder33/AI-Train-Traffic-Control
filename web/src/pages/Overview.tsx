import { useEffect, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { KpiCard } from '../components/KpiCard'
import { DataTable } from '../components/DataTable'

export default function OverviewPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [state, setState] = useState<any | null>(null)
  const [err, setErr] = useState<string | null>(null)
  useEffect(() => {
    let live = true
    api.getState(scope, date).then(d => { if (!live) return; setState(d) }).catch(e => setErr(String(e)))
    return () => { live = false }
  }, [api, scope, date])

  const kpis = state?.sim_kpis || {}
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
      <div className="card" style={{ marginTop: 12 }}>
        <div className="hstack"><strong>Platform Occupancy</strong><span className="spacer" /><span className="muted">top 20</span></div>
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

