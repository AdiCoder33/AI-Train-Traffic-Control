import { useStore } from '../../state/store'

export function TopStatus() {
  const plan = useStore(s => s.plan)
  return (
    <div className="hstack" style={{ gap: 8 }}>
      <span className="badge">ΣDelay: {plan?.kpi?.sumDelay ?? 0}</span>
      <span className="badge">p90: {plan?.kpi?.p90Delay ?? 0}</span>
      <span className="badge">TPH: {plan?.kpi?.throughput ?? 0}</span>
    </div>
  )
}

