import { useEffect, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'

export default function PolicyPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [data, setData] = useState<any>({})
  const [err, setErr] = useState<string | null>(null)
  useEffect(() => {
    let live = true
    api.getPolicy(scope, date).then(d => { if (!live) return; setData(d || {}) }).catch(e => setErr(String(e)))
    return () => { live = false }
  }, [api, scope, date])
  return (
    <div>
      <h2>Policy</h2>
      <ScopeBar />
      {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>Error: {err}</div>}
      <div className="card" style={{ marginTop: 12 }}>
        <pre style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(data, null, 2)}</pre>
      </div>
    </div>
  )
}

