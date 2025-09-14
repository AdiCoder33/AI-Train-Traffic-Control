import { useStore } from '../../state/store'

export function RightPanel() {
  const recs = useStore(s => s.recs)
  return (
    <div className="card" style={{ width: 320, minHeight: 300 }}>
      <div className="hstack"><strong>Recommendations</strong><span className="spacer" /><span className="muted">top 3</span></div>
      {recs.slice(0, 3).map(r => (
        <div key={r.id} className="card" style={{ marginTop: 8 }}>
          <div>{r.label}</div>
          <div className="muted">ΔΣDelay {r.delta.sumDelay}</div>
          <div className="hstack" style={{ gap: 6, flexWrap: 'wrap' }}>{r.why.map((w, i) => <span key={i} className="badge">{w}</span>)}</div>
          <div className="hstack" style={{ gap: 6 }}>
            <button className="primary">Apply</button>
            <button>Undo</button>
          </div>
        </div>
      ))}
    </div>
  )
}

