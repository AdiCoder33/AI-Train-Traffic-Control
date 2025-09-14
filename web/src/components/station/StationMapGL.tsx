import { useEffect, useMemo, useRef, useState } from 'react'
import maplibregl, { Map } from 'maplibre-gl'
import 'maplibre-gl/dist/maplibre-gl.css'

type Edge = { u: string; v: string; u_lat: number; u_lon: number; v_lat: number; v_lon: number; block_id?: string }
type Block = { u?: string; v?: string; entry_time?: string; exit_time?: string; train_id?: string }

function parseTs(x: any): number { const t = Date.parse(String(x)); return isNaN(t) ? NaN : t }

export function StationMapGL({
  stationId,
  center,
  edges = [],
  blocks = [],
}: {
  stationId: string
  center: { lat: number; lon: number } | null
  edges: Edge[]
  blocks: Block[]
}) {
  const mapRef = useRef<Map | null>(null)
  const elRef = useRef<HTMLDivElement | null>(null)
  const [playing, setPlaying] = useState(true)
  const [speed, setSpeed] = useState(60)
  const [t, setT] = useState<number>(0)
  const timerRef = useRef<number | undefined>(undefined)

  const timeBounds = useMemo(() => {
    const times: number[] = []
    for (const b of blocks) { const a = parseTs(b.entry_time); const z = parseTs(b.exit_time); if (!isNaN(a)) times.push(a); if (!isNaN(z)) times.push(z) }
    if (!times.length) return { t0: 0, t1: 0 }
    return { t0: Math.min(...times), t1: Math.max(...times) }
  }, [blocks])

  useEffect(() => { if (t === 0 && timeBounds.t0) setT(timeBounds.t0) }, [timeBounds, t])

  useEffect(() => {
    if (!playing) { if (timerRef.current) window.clearInterval(timerRef.current); timerRef.current = undefined; return }
    if (!timeBounds.t0 || !timeBounds.t1) return
    timerRef.current = window.setInterval(() => { setT(prev => { const dt = speed * 60 * 1000; const next = prev + dt; return next > timeBounds.t1 ? timeBounds.t0 : next }) }, 1000) as any
    return () => { if (timerRef.current) window.clearInterval(timerRef.current); timerRef.current = undefined }
  }, [playing, speed, timeBounds])

  // Initialize map
  useEffect(() => {
    if (!elRef.current || mapRef.current) return
    const styleFromEnv = (import.meta as any).env?.VITE_MAP_STYLE as string | undefined
    const map = new maplibregl.Map({
      container: elRef.current,
      style: styleFromEnv || {
        version: 8,
        sources: {
          osm: { type: 'raster', tiles: ['https://tile.openstreetmap.org/{z}/{x}/{y}.png'], tileSize: 256, attribution: '© OpenStreetMap' }
        },
        layers: [ { id: 'osm', type: 'raster', source: 'osm' } ]
      },
      center: [ center?.lon || 77.2, center?.lat || 28.6 ],
      zoom: 12,
      pitch: 45,
      bearing: 0,
    })
    map.addControl(new maplibregl.NavigationControl({ visualizePitch: true }), 'top-right')
    mapRef.current = map
    return () => { map.remove(); mapRef.current = null }
  }, [center])

  // Add/Update edges layer
  useEffect(() => {
    const map = mapRef.current; if (!map) return
    const fc = { type: 'FeatureCollection', features: (edges || []).map((e: Edge) => ({ type: 'Feature', geometry: { type: 'LineString', coordinates: [[e.u_lon, e.u_lat],[e.v_lon, e.v_lat]] }, properties: { u: e.u, v: e.v, block_id: e.block_id } })) } as any
    if (!map.getSource('edges')) {
      map.addSource('edges', { type: 'geojson', data: fc })
      map.addLayer({ id: 'edges', type: 'line', source: 'edges', paint: { 'line-color': '#44a', 'line-width': 3 } })
    } else {
      (map.getSource('edges') as any).setData(fc)
    }
  }, [edges])

  // Update train markers
  useEffect(() => {
    const map = mapRef.current; if (!map) return
    type T = { type: 'Feature'; geometry: any; properties: any }
    const feats: T[] = []
    const now = t
    for (const b of blocks) {
      const a = parseTs(b.entry_time); const z = parseTs(b.exit_time); if (isNaN(a) || isNaN(z) || now < a || now > z) continue
      const e = edges.find(e => (String(e.u) === String(b.u) && String(e.v) === String(b.v)) || (String(e.u) === String(b.v) && String(e.v) === String(b.u)))
      if (!e) continue
      const p = Math.max(0, Math.min(1, (now - a) / Math.max(1, z - a)))
      const lon = e.u_lon + (e.v_lon - e.u_lon) * p
      const lat = e.u_lat + (e.v_lat - e.u_lat) * p
      feats.push({ type: 'Feature', geometry: { type: 'Point', coordinates: [lon, lat] }, properties: { train_id: String(b.train_id || '') } })
    }
    const fc = { type: 'FeatureCollection', features: feats } as any
    if (!map.getSource('trains')) {
      map.addSource('trains', { type: 'geojson', data: fc })
      map.addLayer({ id: 'trains', type: 'circle', source: 'trains', paint: { 'circle-radius': 6, 'circle-color': '#f80', 'circle-stroke-width': 1.5, 'circle-stroke-color': '#000' } })
    } else {
      (map.getSource('trains') as any).setData(fc)
    }
  }, [blocks, edges, t])

  return (
    <div>
      <div className="hstack" style={{ alignItems: 'center', gap: 8, marginBottom: 6 }}>
        <strong>Map</strong>
        <span className="badge">{stationId}</span>
        <span className="spacer" />
        <button onClick={() => setPlaying(p => !p)}>{playing ? 'Pause' : 'Play'}</button>
        <span className="muted">Speed</span>
        <select value={speed} onChange={e => setSpeed(Number(e.target.value))}>
          {[30, 60, 120, 240].map(s => <option key={s} value={s}>{s}x</option>)}
        </select>
        <span className="muted">{t ? new Date(t).toLocaleString() : ''}</span>
      </div>
      <div ref={elRef} style={{ width: '100%', height: 420, borderRadius: 8, overflow: 'hidden' }} />
    </div>
  )
}

