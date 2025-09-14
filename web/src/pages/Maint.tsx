import { useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'

export default function MaintPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [out, setOut] = useState<any>(null)
  const [err, setErr] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  async function addSpeedRestriction() {
    setBusy(true); setErr(null); setOut(null)
    try {
      const res = await api.runScenario(scope, date, { kind: 'speed_restriction', params: { u: 'STN-B', v: 'STN-C', speed_factor: 1.3 }, name: 'SR-1.3' })
      setOut(res)
    } catch (e: any) { setErr(e?.message || 'Failed') } finally { setBusy(false) }
  }
  return (
    <div>
      <h2>Maintenance / Engineering</h2>
      <ScopeBar />
      {err && <div className="card" style={{ borderColor: '#ff6b6b' }}>Error: {err}</div>}
      <div className="card">
        <div className="hstack"><strong>What-if: Speed Restriction</strong><span className="spacer" /><button onClick={addSpeedRestriction} disabled={busy}>{busy ? 'Running…' : 'Run'}</button></div>
        {out && <pre style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(out, null, 2)}</pre>}
      </div>
    </div>
  )
}

