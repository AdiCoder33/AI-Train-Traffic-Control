import { useEffect, useState } from 'react'
import { useApi } from '../lib/session'
import { ScopeBar } from '../components/ScopeBar'

export default function ITPage() {
  const api = useApi()
  const [health, setHealth] = useState<any>(null)
  const [ready, setReady] = useState<any>(null)
  const [metrics, setMetrics] = useState<string>('')
  const [etaImp, setEtaImp] = useState<any>(null)
  const [err, setErr] = useState<string | null>(null)

  useEffect(() => {
    let live = true
    api.health().then(d => { if (!live) return; setHealth(d) }).catch(() => {})
    api.readiness().then(d => { if (!live) return; setReady(d) }).catch(() => {})
    api.metrics().then(t => { if (!live) return; setMetrics(t) }).catch(() => {})
    return () => { live = false }
  }, [api])

  return (
    <div>
      <h2>Integration & Model Health</h2>
      <ScopeBar />
      {err && <div className="card" style={{ borderColor: '#ff6b6b' }}>Error: {err}</div>}
      <div className="card"><strong>Service</strong><pre>{JSON.stringify({ health, ready }, null, 2)}</pre></div>
      <div className="card" style={{ marginTop: 12 }}><strong>Metrics</strong><pre style={{ whiteSpace: 'pre-wrap' }}>{metrics}</pre></div>
    </div>
  )
}

