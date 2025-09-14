import { useState } from 'react'
import { ScopeBar } from '../components/ScopeBar'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'

export default function LabPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [note] = useState('Scenario runner and predictive demos')
  const [out, setOut] = useState<any>(null)
  const [err, setErr] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)

  async function onRunScenario() {
    setBusy(true); setErr(null); setOut(null)
    try {
      const res = await api.runScenario(scope, date, { kind: 'late_start', params: { train_id: 'T00001', station_id: 'STN-A', delay_min: 5 } })
      setOut(res)
    } catch (e: any) { setErr(e?.message || 'Scenario failed') } finally { setBusy(false) }
  }
  async function onBatch() {
    setBusy(true); setErr(null); setOut(null)
    try {
      const res = await api.runScenarioBatch(scope, date, [
        { kind: 'late_start', name: 'LS-5', params: { train_id: 'T00001', station_id: 'STN-A', delay_min: 5 } },
        { kind: 'platform_outage', name: 'PO-1', params: { station_id: 'STN-B', platforms: 1 } },
        { kind: 'speed_restriction', name: 'SR-1.2', params: { u: 'STN-B', v: 'STN-C', speed_factor: 1.2 } }
      ], 60)
      setOut(res)
    } catch (e: any) { setErr(e?.message || 'Batch failed') } finally { setBusy(false) }
  }
  async function onTrainEta() {
    setBusy(true); setErr(null); setOut(null)
    try {
      const r = await api.trainEta(scope, date)
      setOut(r)
    } catch (e: any) { setErr(e?.message || 'Train ETA failed') } finally { setBusy(false) }
  }
  async function onBuildRisk() {
    setBusy(true); setErr(null); setOut(null)
    try {
      const r = await api.buildIncidentRisk(scope, date)
      setOut(r)
    } catch (e: any) { setErr(e?.message || 'Build risk failed') } finally { setBusy(false) }
  }
  return (
    <div>
      <h2>Analyst Lab</h2>
      <ScopeBar />
      <div className="card"><div className="muted">Note</div><div>{note}</div></div>
      <div className="hstack" style={{ gap: 8, marginTop: 8 }}>
        <button onClick={onRunScenario} disabled={busy}>Run Scenario</button>
        <button onClick={onBatch} disabled={busy}>Run Batch</button>
        <button onClick={onTrainEta} disabled={busy}>Train ETA</button>
        <button onClick={onBuildRisk} disabled={busy}>Build Risk Heat</button>
      </div>
      {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>Error: {err}</div>}
      {out && <div className="card" style={{ marginTop: 8 }}><pre style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(out, null, 2)}</pre></div>}
    </div>
  )
}
