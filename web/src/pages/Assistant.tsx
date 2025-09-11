import { useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { Field } from '../components/Field'

export default function AssistantPage() {
  const api = useApi()
  const { scope, date, stationId, trainId } = usePrefs()
  const [q, setQ] = useState('What are top risks?')
  const [res, setRes] = useState<any>(null)
  const [err, setErr] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function onAsk() {
    setLoading(true)
    setErr(null)
    setRes(null)
    try {
      const r = await api.ask({ scope, date, query: q, station_id: stationId || null, train_id: trainId || null })
      setRes(r)
    } catch (e: any) {
      setErr(e?.message || 'Ask failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div>
      <h2>Assistant</h2>
      <ScopeBar />
      <div className="controls">
        <Field label="Question"><input value={q} onChange={e => setQ(e.target.value)} style={{ minWidth: 400 }} /></Field>
        <button className="primary" onClick={onAsk} disabled={loading}>{loading ? 'Asking…' : 'Ask'}</button>
      </div>
      {err && <div className="card" style={{ borderColor: '#ff6b6b' }}>Error: {err}</div>}
      {res && <div className="card"><pre style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(res, null, 2)}</pre></div>}
    </div>
  )
}

