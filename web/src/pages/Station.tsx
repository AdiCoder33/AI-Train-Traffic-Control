import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { KpiCard } from '../components/KpiCard'
import { DataTable } from '../components/DataTable'
import { Timeline, TimelineItem } from '../components/charts/Timeline'
import { Timeseries } from '../components/charts/Timeseries'
import { Bar } from '../components/charts/Bar'
import { Histogram } from '../components/charts/Histogram'
import { colorForKey } from '../lib/colors'
import { StationMap2D } from '../components/station/StationMap2D'
import { StationMapGL } from '../components/station/StationMapGL'
import { TrainGraph } from '../components/charts/TrainGraph'

export default function StationPage() {
  const api = useApi()
  const { scope, date, stationId } = usePrefs()
  const [state, setState] = useState<any | null>(null)
  const [radar, setRadar] = useState<any[]>([])
  const [blocks, setBlocks] = useState<any[]>([])
  const [recs, setRecs] = useState<any[]>([])
  const [nodes, setNodes] = useState<any[]>([])
  const [edges, setEdges] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)
  const [audit, setAudit] = useState<any[]>([])
  const [activeTab, setActiveTab] = useState<'graph'|'gantt'>('graph')

  useEffect(() => {
    if (!stationId) return
    let live = true
    setErr(null)
    api.getState(scope, date, { station_id: stationId }).then(d => { if (!live) return; setState(d) }).catch(e => setErr(String(e)))
    api.getRadar(scope, date, { station_id: stationId }).then(r => { if (!live) return; setRadar(r.radar || []) }).catch(() => {})
    api.getBlockOccupancy(scope, date, stationId).then(d => { if (!live) return; setBlocks(d.blocks || []) }).catch(() => {})
    api.getRecommendations(scope, date, stationId).then(d => { if (!live) return; setRecs(d.rec_plan || []) }).catch(() => {})
    api.getNodes(scope, date).then(d => { if (!live) return; setNodes(d.nodes || []) }).catch(() => {})
    api.getEdges(scope, date, stationId).then(d => { if (!live) return; setEdges(d.edges || []) }).catch(() => {})
    api.getAuditTrail(scope, date).then(d => { if (!live) return; setAudit(d.audit_trail || []) }).catch(() => {})
    return () => { live = false }
  }, [api, scope, date, stationId])

  const platformItems: TimelineItem[] = useMemo(() => {
    const rows = (state?.platform_occupancy || []).slice(0, 60)
    return rows.map((r: any) => ({
      y: String(r.station_id ?? ''),
      start: r.arr_platform,
      end: r.dep_platform,
      label: `${r.train_id}`,
      color: colorForKey(String(r.train_id ?? '')),
    }))
  }, [state])

  const kpiNow = useMemo(() => {
    const po = state?.platform_occupancy || []
    const wl = state?.waiting_ledger || []
    return {
      trainsAtPlatform: po.length,
      waiting: wl.length,
      risks: radar.length,
      actions: recs.length,
    }
  }, [state, radar, recs])

  const platCount = useMemo(() => {
    const node = (nodes || []).find((n: any) => String(n.station_id || '') === String(stationId))
    const p = Number(node?.platforms ?? 0)
    return isNaN(p) || p <= 0 ? 4 : p
  }, [nodes, stationId])

  const alerts = useMemo(() => {
    const soon = (radar || []).filter((r: any) => Number(r.lead_min ?? 999) <= 15 && ['Critical','High'].includes(String(r.severity || '')))
    return soon
  }, [radar])
  const riskTimeline = useMemo(() => {
    const buckets: Record<string, number> = {}
    (radar || []).forEach((r: any) => {
      const t = r.time_window?.[0]
      if (!t) return
      const d = new Date(t)
      d.setSeconds(0, 0)
      const step = 15
      d.setMinutes(Math.floor(d.getMinutes() / step) * step)
      const k = d.toISOString()
      buckets[k] = (buckets[k] || 0) + 1
    })
    const keys = Object.keys(buckets).sort()
    return [{ name: 'Risks', x: keys, y: keys.map(k => buckets[k]) }]
  }, [radar])
  const waitingByReasonHere = useMemo(() => {
    const wl = state?.waiting_ledger || []
    const sid = String(stationId || '')
    const by: Record<string, number> = {}
    wl.forEach((w: any) => {
      const isStation = String(w.resource || '') === 'platform' && String(w.id || '') === sid
      if (!isStation) return
      const r = String(w.reason || 'other')
      const m = Number(w.minutes || 0)
      by[r] = (by[r] || 0) + (isNaN(m) ? 0 : m)
    })
    const labels = Object.keys(by)
    const vals = labels.map(k => by[k])
    return { labels, vals }
  }, [state, stationId])
  const waitingMinutesHere = useMemo(() => {
    const wl = state?.waiting_ledger || []
    const sid = String(stationId || '')
    const vals: number[] = []
    wl.forEach((w: any) => {
      const isStation = String(w.resource || '') === 'platform' && String(w.id || '') === sid
      if (!isStation) return
      const m = Number(w.minutes || 0)
      if (!isNaN(m)) vals.push(m)
    })
    return vals
  }, [state, stationId])

  const center = useMemo(() => {
    const node = (nodes || []).find((n: any) => String(n.station_id || '') === String(stationId))
    const lat = Number(node?.lat ?? node?.latitude)
    const lon = Number(node?.lon ?? node?.longitude)
    return isNaN(lat) || isNaN(lon) ? null : { lat, lon }
  }, [nodes, stationId])

  async function onToggleLock() {
    if (!stationId) return
    try {
      // naive toggle: call lock=true (idempotent). Extend by reading /locks to flip.
      await api.lockStation(scope, date, stationId, true)
    } catch {}
  }

  if (!stationId) {
    return (
      <div>
        <h2>Station</h2>
        <ScopeBar />
        <div className="card" style={{ borderColor: '#ff6b6b' }}>No station assigned. Ask admin to set station for your account.</div>
      </div>
    )
  }

  return (
    <div>
      <div className="hstack" style={{ alignItems: 'baseline' }}>
        <h2 style={{ margin: 0 }}>Operations Console</h2>
        <span className="badge">{stationId}</span>
      </div>
      <ScopeBar />
      {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>Error: {err}</div>}
      {alerts.length > 0 && (
        <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>
          <strong>Alerts:</strong> {alerts.length} critical/high conflicts within 15 minutes
        </div>
      )}

      <div className="row" style={{ gap: 8, alignItems: 'flex-start' }}>
        {/* Left rail: filters + incidents */}
        <div className="card" style={{ width: 260, minHeight: 300 }}>
          <div className="muted">Filters</div>
          <div className="controls">
            <label><input type="checkbox" defaultChecked /> Suburban/EMU</label>
            <label><input type="checkbox" defaultChecked /> Express</label>
            <label><input type="checkbox" defaultChecked /> Freight</label>
          </div>
          <div className="muted" style={{ marginTop: 12 }}>Incidents</div>
          <div style={{ maxHeight: 240, overflow: 'auto' }}>
            {(radar || []).slice(0, 40).map((r: any, i: number) => (
              <div key={i} className="hstack" style={{ gap: 6, padding: '4px 0', borderBottom: '1px solid #1f2a52' }}>
                <span className="badge" style={{ background: r.severity==='Critical'?'#ff6b6b':r.severity==='High'?'#ffb86b':'#6bff81' }}>{r.severity}</span>
                <span>{r.type}</span>
                <span className="muted">{r.lead_min}m</span>
              </div>
            ))}
          </div>
        </div>

        {/* Center tabs */}
        <div style={{ flex: 1, minWidth: 480 }}>
          <div className="hstack" style={{ gap: 6 }}>
            <button className={activeTab==='graph'?'badge':''} onClick={() => setActiveTab('graph')}>Train Graph</button>
            <button className={activeTab==='gantt'?'badge':''} onClick={() => setActiveTab('gantt')}>Block/Platform Gantt</button>
            <span className="spacer" />
            <button onClick={onToggleLock}>Lock Station</button>
          </div>
          {activeTab==='graph' && (
            <div className="card" style={{ marginTop: 8 }}>
              <TrainGraph rows={(state?.platform_occupancy || []).slice(0, 400)} />
            </div>
          )}
          {activeTab==='gantt' && (
            <>
              <div className="card" style={{ marginTop: 8 }}>
                <div className="hstack"><strong>Platform Occupancy</strong><span className="spacer" /><span className="muted">{stationId}</span></div>
                <Timeline items={platformItems} />
              </div>
              <div className="card" style={{ marginTop: 8 }}>
                <div className="hstack"><strong>Block Occupancy near {stationId}</strong><span className="spacer" /><span className="muted">sample</span></div>
                <Timeline items={(blocks || []).slice(0, 100).map((r: any) => ({ y: String(r.block_id ?? ''), start: r.entry_time, end: r.exit_time, color: colorForKey(String(r.train_id ?? '')), label: `${r.train_id}` }))} />
              </div>
            </>
          )}
          <div className="card" style={{ marginTop: 8 }}>
            <StationMapGL stationId={stationId} center={center} edges={edges} blocks={blocks} />
          </div>
          <div className="card" style={{ marginTop: 8 }}>
            <StationMap2D stationId={stationId} platforms={platCount} blocks={blocks} platformsOcc={state?.platform_occupancy || []} />
          </div>
        </div>

        {/* Right panel: Recommendations + Explain */}
        <div className="card" style={{ width: 340, minHeight: 300 }}>
          <div className="hstack"><strong>Recommendations</strong><span className="spacer" /><span className="muted">top 20</span></div>
          <div style={{ maxHeight: 520, overflow: 'auto' }}>
            {(recs || []).slice(0, 20).map((r: any, i: number) => (
              <div key={i} className="card" style={{ marginBottom: 8 }}>
                <div className="muted">{r.type} · Train {r.train_id}</div>
                <div>{r.why || r.reason}</div>
                <div className="hstack" style={{ gap: 6, flexWrap: 'wrap' }}>
                  {(r.binding_constraints || []).map((c: string, j: number) => (<span key={j} className="badge">{c}</span>))}
                </div>
                <div className="hstack" style={{ gap: 6 }}>
                  <button className="primary">Apply</button>
                  <button>Partial apply</button>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* KPI chips and Event timeline */}
      <div className="row" style={{ marginTop: 8 }}>
        <KpiCard label="Throughput (tph)" value={kpiNow.trainsAtPlatform} />
        <KpiCard label="Sum Delay (min)" value={(state?.sim_kpis?.total_wait_min ?? 0)} />
        <KpiCard label="90p Delay (min)" value={(state?.sim_kpis?.p90_exit_delay_min ?? 0)} />
        <KpiCard label="Re-opt Count" value={(state?.sim_kpis?.reopt_count ?? 0)} />
      </div>
      <div className="row" style={{ marginTop: 8 }}>
        <div className="card" style={{ flex: 2, minWidth: 420 }}>
          <div className="hstack"><strong>Risk Timeline</strong><span className="spacer" /><span className="muted">15 min buckets</span></div>
          <Timeseries series={riskTimeline as any} />
        </div>
        <div className="card" style={{ flex: 1, minWidth: 320 }}>
          <div className="hstack"><strong>Waiting by Reason</strong><span className="spacer" /><span className="muted">{stationId}</span></div>
          <Bar x={waitingByReasonHere.labels} y={waitingByReasonHere.vals} />
        </div>
        <div className="card" style={{ flex: 1, minWidth: 320 }}>
          <div className="hstack"><strong>Hold Minutes Distribution</strong><span className="spacer" /><span className="muted">{stationId}</span></div>
          <Histogram values={waitingMinutesHere} nbins={10} />
        </div>
      </div>
      <div className="card" style={{ marginTop: 8 }}>
        <div className="hstack"><strong>Event Timeline</strong><span className="spacer" /><span className="muted">latest 20</span></div>
        <div style={{ maxHeight: 180, overflow: 'auto' }}>
          {(audit || []).slice(-20).reverse().map((e: any, i: number) => (
            <div key={i} className="hstack" style={{ gap: 8, padding: '2px 0', borderBottom: '1px solid #1f2a52' }}>
              <span className="muted">{e.ts}</span>
              <span className="badge">{e.decision || 'EVENT'}</span>
              <span className="muted">{e.reason || ''}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}
