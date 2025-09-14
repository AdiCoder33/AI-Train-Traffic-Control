import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { DataTable } from '../components/DataTable'
import { KpiCard } from '../components/KpiCard'

export default function CorridorPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [edges, setEdges] = useState<any[]>([])
  const [blocks, setBlocks] = useState<any[]>([])
  const [heat, setHeat] = useState<Record<string, number>>({})
  const [rec, setRec] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    let live = true
    setErr(null)
    api.getEdges(scope, date).then(d => { if (!live) return; setEdges(d.edges || []) }).catch(e => setErr(String(e)))
    api.getBlockOccupancy(scope, date).then(d => { if (!live) return; setBlocks(d.blocks || []) }).catch(() => {})
    api.getRiskHeat(scope, date).then(d => { if (!live) return; setHeat(d.heat || {}) }).catch(() => {})
    return () => { live = false }
  }, [api, scope, date])

  const congestion = useMemo(() => {
    const counts: Record<string, number> = {}
    for (const b of (blocks || [])) counts[String(b.block_id)] = (counts[String(b.block_id)] || 0) + 1
    return counts
  }, [blocks])

  const kpi = useMemo(() => {
    const z = Object.values(congestion)
    const avg = z.length ? z.reduce((a, b) => a + b, 0) / z.length : 0
    return { blocks: Object.keys(congestion).length, avgOccupancy: avg.toFixed(1) }
  }, [congestion])

  return (
    <div>
      <h2>Corridor Coordination</h2>
      <ScopeBar />
      {err && <div className="card" style={{ borderColor: '#ff6b6b' }}>Error: {err}</div>}
      <div className="row">
        <KpiCard label="Blocks" value={kpi.blocks} />
        <KpiCard label="Avg Occ (count)" value={kpi.avgOccupancy} />
      </div>
      <div className="card" style={{ marginTop: 12 }}>
        <div className="hstack"><strong>Congestion Heat</strong><span className="spacer" /><span className="muted">higher=more</span></div>
        <DataTable columns={[
          { key: 'block_id', label: 'Block' },
          { key: 'u', label: 'From' },
          { key: 'v', label: 'To' },
          { key: 'occ', label: 'Occ' },
          { key: 'risk', label: 'Risk' },
        ]} rows={(edges || []).slice(0, 200).map((e: any) => ({ block_id: e.block_id, u: e.u, v: e.v, occ: (congestion[String(e.block_id)] || 0), risk: (heat[String(e.block_id)] || 0).toFixed(2) }))} />
      </div>
      <div className="card" style={{ marginTop: 12 }}>
        <div className="hstack"><strong>Interchange Handshake (Demo)</strong><span className="spacer" /><span className="muted">align boundary slots</span></div>
        <HandshakeForm onPlan={setRec} />
        {rec.length > 0 && <div style={{ marginTop: 8 }}><DataTable columns={[
          { key: 'train_id', label: 'Train' },
          { key: 'at_station', label: 'At' },
          { key: 'minutes', label: 'Min' },
          { key: 'reason', label: 'Reason' },
        ]} rows={rec} /></div>}
      </div>
    </div>
  )
}

function HandshakeForm({ onPlan }: { onPlan: (rows: any[]) => void }) {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [scopeB, setScopeB] = useState('demo_section')
  const [dateB, setDateB] = useState(date)
  const [boundary, setBoundary] = useState('STN-B')
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  async function onRun() {
    setBusy(true); setErr(null); onPlan([])
    try {
      const res = await api.handshake({ scopeA: scope, dateA: date, scopeB, dateB, boundary_station: boundary })
      onPlan(res.actions || [])
    } catch (e: any) { setErr(e?.message || 'Handshake failed') } finally { setBusy(false) }
  }
  return (
    <div className="controls">
      <div className="hstack" style={{ gap: 8 }}>
        <label>Section B</label>
        <input value={scopeB} onChange={e => setScopeB(e.target.value)} />
        <label>Date</label>
        <input value={dateB} onChange={e => setDateB(e.target.value)} />
        <label>Boundary</label>
        <input value={boundary} onChange={e => setBoundary(e.target.value)} />
        <button onClick={onRun} disabled={busy}>{busy ? 'Running…' : 'Run'}</button>
      </div>
      {err && <div className="muted">{err}</div>}
    </div>
  )
}

