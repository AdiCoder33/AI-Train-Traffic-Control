import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { DataTable } from '../components/DataTable'
import { Bar } from '../components/charts/Bar'
import { Histogram } from '../components/charts/Histogram'

export default function RecommendationsPage() {
  const api = useApi()
  const { scope, date, stationId, trainId } = usePrefs()
  const [rows, setRows] = useState<any[]>([])
  const [applyReport, setApplyReport] = useState<any | null>(null)
  const [err, setErr] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [optimizing, setOptimizing] = useState(false)
  const [applying, setApplying] = useState<string | null>(null)
  const [selected, setSelected] = useState<string | null>(null)
  const reductionChart = useMemo(() => {
    if (!applyReport) return null
    const toNumber = (v: any) => {
      const n = Number(v)
      return Number.isFinite(n) ? n : 0
    }
    const delayBefore = toNumber(applyReport.wait_minutes_before)
    const delayAfter = toNumber(applyReport.wait_minutes_after)
    const riskBefore = toNumber(applyReport.baseline_risks)
    const riskAfter = toNumber(applyReport.applied_risks)
    const hasData = [delayBefore, delayAfter, riskBefore, riskAfter].some(v => v > 0)
    if (!hasData) return null
    const categories = ['Delay Minutes', 'Risk Events']
    return {
      series: [
        { name: 'Before', x: categories, y: [delayBefore, riskBefore], colorKey: 'before' },
        { name: 'After', x: categories, y: [delayAfter, riskAfter], colorKey: 'after' },
      ],
      delta: { delay: delayBefore - delayAfter, risk: riskBefore - riskAfter },
    }
  }, [applyReport])

  useEffect(() => {
    let live = true
    api.getRecommendations(scope, date, stationId || undefined).then(d => {
      if (!live) return
      setRows(d.rec_plan || [])
      setApplyReport(d.plan_apply_report ?? null)
    }).catch(e => {
      if (!live) return
      setErr(String(e))
      setApplyReport(null)
    })
    return () => { live = false }
  }, [api, scope, date, stationId])

  async function onSuggest() {
    setLoading(true)
    setErr(null)
    try {
      const res = await api.suggest({ scope, date, train_id: trainId || null, max_hold_min: 3, station_id: stationId || '' })
      setRows(res?.result?.suggestions || [])
      setApplyReport(null)
    } catch (e: any) {
      setErr(e?.message || 'Suggest failed')
    } finally {
      setLoading(false)
    }
  }

  async function onOptimize() {
    setOptimizing(true)
    setErr(null)
    try {
      await api.optimize({ scope, date, horizon_min: 60 })
      const d = await api.getRecommendations(scope, date, stationId || undefined)
      setRows(d.rec_plan || [])
      setApplyReport(d.plan_apply_report ?? null)
    } catch (e: any) {
      setErr(e?.message || 'Optimize failed')
    } finally {
      setOptimizing(false)
    }
  }

  async function onApply(action_id: string) {
    setApplying(action_id)
    setErr(null)
    try {
      await api.applyAction(scope, date, action_id)
    } catch (e: any) {
      setErr(e?.message || 'Apply failed')
    } finally {
      setApplying(null)
    }
  }

  return (
    <div>
      <div className="hstack" style={{ alignItems: 'center' }}>
        <h2 style={{ margin: 0 }}>Recommendations</h2>
        <span className="spacer" />
        <button onClick={onOptimize} disabled={optimizing} style={{ marginRight: 8 }}>{optimizing ? 'Optimizing…' : 'Optimize'}</button>
        <button className="primary" onClick={onSuggest} disabled={loading}>{loading ? 'Suggesting…' : 'Suggest'}</button>
      </div>
      <ScopeBar />
      {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>Error: {err}</div>}
      <div className="card" style={{ marginTop: 12 }}>
        <div className="muted">Tip: Click a row to select. Press 'a' to Apply, 'd' to Dismiss, 'r' to Revert plan.</div>
        <Keybinds selected={selected} rows={rows} onApply={async (id) => {
          const rec = rows.find(r => (r.action_id || '') === id)
          if (!rec) return
          await api.postFeedback(scope, date, rec, 'APPLY')
        }} onDismiss={async (id) => {
          const rec = rows.find(r => (r.action_id || '') === id)
          if (!rec) return
          await api.postFeedback(scope, date, rec, 'DISMISS')
        }} onRevert={async () => { await api.revertPlan(scope, date) }} />
        <DataTable columns={[
          { key: 'train_id', label: 'Train' },
          { key: 'type', label: 'Type' },
          { key: 'at_station', label: 'At' },
          { key: 'station_id', label: 'Station' },
          { key: 'block_id', label: 'Block' },
          { key: 'minutes', label: 'Minutes' },
          { key: 'reason', label: 'Reason' }
        ]} rows={rows.slice(0, 50)} />
        <div className="muted" style={{ marginTop: 4 }}>Selected: {selected || 'None'}</div>
      </div>
      {reductionChart && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="hstack"><strong>Impact: Delay & Risk Reduction</strong></div>
          <Bar series={reductionChart.series} />
          <div className="muted" style={{ marginTop: 8 }}>
            Delay change: {Math.round(reductionChart.delta.delay).toLocaleString()} min · Risk change: {Math.round(reductionChart.delta.risk).toLocaleString()}
          </div>
        </div>
      )}
      <div className="row" style={{ marginTop: 12 }}>
        <div className="card" style={{ flex: 1, minWidth: 320 }}>
          <div className="hstack"><strong>Recommendation Types</strong></div>
          <Bar x={[...new Set(rows.map((r: any) => String(r.type || 'UNKNOWN')))]}
               y={(function(){ const by: Record<string, number> = {}; rows.forEach((r:any)=>{ const k=String(r.type||'UNKNOWN'); by[k]=(by[k]||0)+1 }); return Object.keys(by).map(k=>by[k]) })()} />
        </div>
        <div className="card" style={{ flex: 1, minWidth: 320 }}>
          <div className="hstack"><strong>Hold Minutes Distribution</strong></div>
          <Histogram values={rows.map((r: any) => Number(r.minutes || 0)).filter((v: number) => !isNaN(v))} nbins={10} />
        </div>
      </div>
      <div className="card" style={{ marginTop: 12 }}>
        <div className="hstack"><strong>Quick Apply</strong><span className="spacer" /><span className="muted">first 20</span></div>
        <div className="row">
          {rows.slice(0, 20).map((r: any, i: number) => (
            <div key={i} className="card" style={{ minWidth: 260, borderColor: (selected === (r.action_id || '') ? '#8fb4ff' : undefined) }} onClick={() => setSelected(r.action_id || '')}>
              <div className="muted">{r.type} · Train {r.train_id}</div>
              <div style={{ fontSize: 14 }}>At {r.at_station || r.station_id} · {r.minutes} min</div>
              <button onClick={() => onApply(r.action_id || '')} disabled={!r.action_id || applying === (r.action_id || '')} style={{ marginTop: 8 }}>{applying === (r.action_id || '') ? 'Applying…' : 'Apply'}</button>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}

function Keybinds({ selected, rows, onApply, onDismiss, onRevert }: { selected: string | null, rows: any[], onApply: (id: string) => Promise<void>, onDismiss: (id: string) => Promise<void>, onRevert: () => Promise<void> }) {
  useEffect(() => {
    function handler(e: KeyboardEvent) {
      const id = selected || ''
      if (!id && (e.key === 'a' || e.key === 'd')) return
      if (e.key === 'a') { onApply(id) }
      if (e.key === 'd') { onDismiss(id) }
      if (e.key === 'r') { onRevert() }
    }
    window.addEventListener('keydown', handler)
    return () => window.removeEventListener('keydown', handler)
  }, [selected, rows, onApply, onDismiss, onRevert])
  return null
}
