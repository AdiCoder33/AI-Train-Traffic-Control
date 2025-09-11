import { useEffect, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { DataTable } from '../components/DataTable'

export default function RecommendationsPage() {
  const api = useApi()
  const { scope, date, stationId } = usePrefs()
  const [rows, setRows] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    let live = true
    api.getRecommendations(scope, date).then(d => { if (!live) return; setRows(d.rec_plan || []) }).catch(e => setErr(String(e)))
    return () => { live = false }
  }, [api, scope, date])

  async function onSuggest() {
    setLoading(true)
    setErr(null)
    try {
      const res = await api.suggest({ scope, date, train_id: null, max_hold_min: 3, station_id: stationId || '' })
      setRows(res?.result?.suggestions || [])
    } catch (e: any) {
      setErr(e?.message || 'Suggest failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div>
      <div className="hstack" style={{ alignItems: 'center' }}>
        <h2 style={{ margin: 0 }}>Recommendations</h2>
        <span className="spacer" />
        <button className="primary" onClick={onSuggest} disabled={loading}>{loading ? 'Suggesting…' : 'Suggest'}</button>
      </div>
      <ScopeBar />
      {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>Error: {err}</div>}
      <div className="card" style={{ marginTop: 12 }}>
        <DataTable columns={[
          { key: 'train_id', label: 'Train' },
          { key: 'type', label: 'Type' },
          { key: 'at_station', label: 'At' },
          { key: 'station_id', label: 'Station' },
          { key: 'block_id', label: 'Block' },
          { key: 'minutes', label: 'Minutes' },
          { key: 'reason', label: 'Reason' }
        ]} rows={rows.slice(0, 50)} />
      </div>
    </div>
  )
}

