import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { ScopeBar } from '../components/ScopeBar'
import { DataTable } from '../components/DataTable'
import { Timeline, TimelineItem } from '../components/charts/Timeline'
import { Heatmap } from '../components/charts/Heatmap'
import { colorForKey } from '../lib/colors'
import { MapScatter } from '../components/charts/MapScatter'

export default function BoardPage() {
  const api = useApi()
  const { scope, date, stationId, trainId } = usePrefs()
  const [state, setState] = useState<any | null>(null)
  const [blocks, setBlocks] = useState<any[]>([])
  const [nodes, setNodes] = useState<any[]>([])
  const [activeTab, setActiveTab] = useState<'plat'|'block'|'map'>('plat')
  const [err, setErr] = useState<string | null>(null)
  useEffect(() => {
    let live = true
    api.getState(scope, date, { station_id: stationId || undefined, train_id: trainId || undefined }).then(d => { if (!live) return; setState(d) }).catch(e => setErr(String(e)))
    api.getBlockOccupancy(scope, date, stationId || undefined).then(d => { if (!live) return; setBlocks(d.blocks || []) }).catch(() => {})
    api.getNodes(scope, date).then(d => { if (!live) return; setNodes(d.nodes || []) }).catch(() => {})
    return () => { live = false }
  }, [api, scope, date, stationId, trainId])

  const waiting = useMemo(() => (state?.waiting_ledger || []).slice(0, 100), [state])
  const timelineItems: TimelineItem[] = useMemo(() => {
    const rows = (state?.platform_occupancy || []).slice(0, 80)
    return rows.map((r: any) => ({ y: String(r.station_id ?? ''), start: r.arr_platform, end: r.dep_platform, label: `${r.train_id}` }))
  }, [state])
  const occupancyHeat = useMemo(() => {
    const rows = state?.platform_occupancy || []
    const stations = Array.from(new Set(rows.map((r: any) => String(r.station_id || '')))).sort()
    const buckets: string[] = []
    const index: Record<string, number> = {}
    const grid: number[][] = stations.map(() => [])
    function pushBucket(ts: any) {
      const d = ts ? new Date(ts) : null
      if (!d) return null
      d.setSeconds(0, 0)
      const step = 15
      d.setMinutes(Math.floor(d.getMinutes() / step) * step)
      const key = d.toISOString()
      if (!(key in index)) { index[key] = buckets.length; buckets.push(key); stations.forEach((_, i) => { grid[i][index[key]] = grid[i][index[key]] || 0 }) }
      return key
    }
    rows.forEach((r: any) => {
      const sid = String(r.station_id || '')
      const rowIdx = stations.indexOf(sid)
      const k = pushBucket(r.arr_platform)
      if (rowIdx >= 0 && k) {
        const col = index[k]
        grid[rowIdx][col] = (grid[rowIdx][col] || 0) + 1
      }
    })
    return { z: grid.map(row => row.map(v => v || 0)), x: buckets, y: stations }
  }, [state])

  return (
    <div>
      <h2>Live Section Board</h2>
      <ScopeBar />
      <div className="hstack" style={{ gap: 6, marginTop: 8 }}>
        <button onClick={() => setActiveTab('plat')} className={activeTab==='plat'?'badge':''}>Platforms</button>
        <button onClick={() => setActiveTab('block')} className={activeTab==='block'?'badge':''}>Blocks</button>
        <button onClick={() => setActiveTab('map')} className={activeTab==='map'?'badge':''}>Map</button>
      </div>
      {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 8 }}>Error: {err}</div>}
      {activeTab === 'plat' && (
        <>
          <div className="card" style={{ marginTop: 12 }}>
            <div className="hstack"><strong>Platform Occupancy Timeline</strong><span className="spacer" /><span className="muted">sample</span></div>
            <Timeline items={timelineItems} />
          </div>
          <div className="card" style={{ marginTop: 12 }}>
            <div className="hstack"><strong>Platform Occupancy Heatmap</strong><span className="spacer" /><span className="muted">15 min buckets</span></div>
            <Heatmap z={occupancyHeat.z} x={occupancyHeat.x} y={occupancyHeat.y} />
          </div>
          <div className="card" style={{ marginTop: 12 }}>
            <div className="hstack"><strong>Waiting Ledger</strong><span className="spacer" /><span className="muted">top 100</span></div>
            <DataTable columns={[
              { key: 'train_id', label: 'Train' },
              { key: 'resource', label: 'Resource' },
              { key: 'id', label: 'ID' },
              { key: 'eta', label: 'ETA' },
              { key: 'reason', label: 'Reason' }
            ]} rows={waiting} />
          </div>
        </>
      )}
      {activeTab === 'block' && (
        <div className="card" style={{ marginTop: 12 }}>
          <div className="hstack"><strong>Block Occupancy Timeline</strong><span className="spacer" /><span className="muted">sample</span></div>
          <Timeline items={(blocks || []).slice(0, 100).map((r: any) => ({
            y: String(r.block_id ?? ''),
            start: r.entry_time,
            end: r.exit_time,
            color: colorForKey(String(r.train_id ?? '')),
            label: `${r.train_id}`,
          }))} />
        </div>
      )}
      {activeTab === 'map' && (
        <div className="card" style={{ marginTop: 12 }}>
          {nodes && nodes.length > 0 ? (
            <MapScatter nodes={nodes} />
          ) : (
            <div className="muted">No lat/lon available in nodes.</div>
          )}
        </div>
      )}
    </div>
  )
}
