import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { KpiCard } from '../components/KpiCard'

export default function ExecPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [state, setState] = useState<any | null>(null)
  useEffect(() => {
    let live = true
    api.getState(scope, date).then(d => { if (!live) return; setState(d) }).catch(() => {})
    return () => { live = false }
  }, [api, scope, date])
  const k = useMemo(() => state?.sim_kpis || {}, [state])
  return (
    <div>
      <h2>Executive Scorecard</h2>
      <ScopeBar />
      <div className="row">
        <KpiCard label="On-time %" value={(k.otp_pct ?? 0).toFixed(1)} />
        <KpiCard label="Avg Delay" value={(k.avg_delay ?? 0).toFixed(1)} />
        <KpiCard label="Trains Served" value={(k.trains_served ?? 0)} />
        <KpiCard label="Total Wait (min)" value={(k.total_wait_min ?? 0)} />
      </div>
    </div>
  )
}

