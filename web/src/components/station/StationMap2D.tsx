import { useEffect, useMemo, useRef, useState } from 'react'
import { colorForKey } from '../../lib/colors'

type Block = { block_id?: string; u?: string; v?: string; entry_time?: string; exit_time?: string; train_id?: string }
type Plat = { train_id?: string; station_id?: string; arr_platform?: string; dep_platform?: string; platform_slot?: number }

function parseTs(x: any): number {
  if (!x) return NaN
  const t = Date.parse(String(x))
  return isNaN(t) ? NaN : t
}

export function StationMap2D({
  stationId,
  platforms,
  blocks = [],
  platformsOcc = [],
}: {
  stationId: string
  platforms: number
  blocks: Block[]
  platformsOcc: Plat[]
}) {
  const [playing, setPlaying] = useState(true)
  const [speed, setSpeed] = useState(60) // sim minutes per real second
  const [t, setT] = useState<number>(0)
  const timerRef = useRef<number | undefined>(undefined)

  const timeBounds = useMemo(() => {
    const times: number[] = []
    for (const b of blocks) {
      const a = parseTs(b.entry_time)
      const z = parseTs(b.exit_time)
      if (!isNaN(a)) times.push(a)
      if (!isNaN(z)) times.push(z)
    }
    for (const p of platformsOcc) {
      const a = parseTs(p.arr_platform)
      const z = parseTs(p.dep_platform)
      if (!isNaN(a)) times.push(a)
      if (!isNaN(z)) times.push(z)
    }
    if (!times.length) return { t0: 0, t1: 0 }
    return { t0: Math.min(...times), t1: Math.max(...times) }
  }, [blocks, platformsOcc])

  // Initialize time
  useEffect(() => {
    if (t === 0 && timeBounds.t0 && timeBounds.t1) setT(timeBounds.t0)
  }, [timeBounds, t])

  useEffect(() => {
    if (!playing) {
      if (timerRef.current) window.clearInterval(timerRef.current)
      timerRef.current = undefined
      return
    }
    if (!timeBounds.t0 || !timeBounds.t1) return
    timerRef.current = window.setInterval(() => {
      setT(prev => {
        const dt = speed * 60 * 1000 // speed minutes per second -> ms per tick
        const next = prev + dt
        if (next > timeBounds.t1) return timeBounds.t0
        return next
      })
    }, 1000) as unknown as number
    return () => {
      if (timerRef.current) window.clearInterval(timerRef.current)
      timerRef.current = undefined
    }
  }, [playing, speed, timeBounds])

  const width = 900
  const height = 420
  const padding = { l: 120, r: 80, t: 40, b: 60 }
  const innerW = width - padding.l - padding.r
  const innerH = height - padding.t - padding.b

  const platCount = Math.max(1, platforms || 1)
  const platGap = innerH / (platCount + 1)
  const platformYs = Array.from({ length: platCount }, (_, i) => padding.t + (i + 1) * platGap)

  // Nearby tracks: group blocks by direction relative to station
  const adjTracks = useMemo(() => {
    const inbound: Block[] = []
    const outbound: Block[] = []
    for (const b of blocks || []) {
      const u = String(b.u || '')
      const v = String(b.v || '')
      if (u === stationId) outbound.push(b)
      else if (v === stationId) inbound.push(b)
    }
    return { inbound, outbound }
  }, [blocks, stationId])

  function xForProgress(p: number, dir: 'in' | 'out') {
    const x0 = padding.l
    const x1 = width - padding.r
    const cx = (x0 + x1) / 2
    const len = innerW * 0.45
    if (dir === 'in') return cx - len * (1 - p)
    return cx + len * p
  }

  const trainsOnPlat = useMemo(() => {
    const out: { y: number; color: string; label: string }[] = []
    platformsOcc.forEach(p => {
      if (String(p.station_id || '') !== String(stationId)) return
      const a = parseTs(p.arr_platform)
      const d = parseTs(p.dep_platform)
      if (isNaN(a) || isNaN(d) || t < a || t > d) return
      const slot = Math.max(0, Math.min(platCount - 1, Number(p.platform_slot ?? 0)))
      const y = platformYs[slot]
      const label = String(p.train_id || '')
      out.push({ y, color: colorForKey(label), label })
    })
    return out
  }, [platformsOcc, t, stationId, platCount, platformYs])

  const trainsOnTracks = useMemo(() => {
    type T = { x: number; y: number; color: string; label: string }
    const out: T[] = []
    const trackYIn = padding.t + innerH * 0.2
    const trackYOut = padding.t + innerH * 0.8
    function push(b: Block, dir: 'in' | 'out') {
      const a = parseTs(b.entry_time)
      const z = parseTs(b.exit_time)
      if (isNaN(a) || isNaN(z) || t < a || t > z) return
      const p = (t - a) / Math.max(1, z - a)
      const x = xForProgress(Math.max(0, Math.min(1, p)), dir)
      const y = dir === 'in' ? trackYIn : trackYOut
      const label = String(b.train_id || '')
      out.push({ x, y, color: colorForKey(label), label })
    }
    let cap = 0
    for (const b of adjTracks.inbound) { push(b, 'in'); if (++cap >= 10) break }
    for (const b of adjTracks.outbound) { push(b, 'out'); if (++cap >= 20) break }
    return out
  }, [adjTracks, t, innerH])

  return (
    <div>
      <div className="hstack" style={{ alignItems: 'center', gap: 8, marginBottom: 6 }}>
        <strong>Live Station Map</strong>
        <span className="spacer" />
        <button onClick={() => setPlaying(p => !p)}>{playing ? 'Pause' : 'Play'}</button>
        <span className="muted">Speed</span>
        <select value={speed} onChange={e => setSpeed(Number(e.target.value))}>
          {[30, 60, 120, 240].map(s => <option key={s} value={s}>{s}x</option>)}
        </select>
      </div>
      <svg width={width} height={height} style={{ width: '100%', height }}>
        {/* Title & time */}
        <text x={padding.l} y={24} fontSize={14} fill="#888">{stationId} • {t ? new Date(t).toLocaleString() : ''}</text>

        {/* Tracks */}
        {/* Inbound track line */}
        <line x1={padding.l} y1={padding.t + innerH * 0.2} x2={width - padding.r} y2={padding.t + innerH * 0.2} stroke="#556" strokeWidth={3} strokeDasharray="6 4" />
        <text x={padding.l} y={padding.t + innerH * 0.2 - 8} fill="#889" fontSize={12}>Inbound</text>
        {/* Outbound track line */}
        <line x1={padding.l} y1={padding.t + innerH * 0.8} x2={width - padding.r} y2={padding.t + innerH * 0.8} stroke="#556" strokeWidth={3} strokeDasharray="6 4" />
        <text x={padding.l} y={padding.t + innerH * 0.8 - 8} fill="#889" fontSize={12}>Outbound</text>

        {/* Platforms as lanes */}
        {platformYs.map((y, i) => (
          <g key={i}>
            <line x1={padding.l + innerW * 0.25} x2={padding.l + innerW * 0.75} y1={y} y2={y} stroke="#667" strokeWidth={8} strokeLinecap="round" />
            <text x={padding.l + innerW * 0.24} y={y - 10} fill="#99a" fontSize={12}>Platform {i + 1}</text>
          </g>
        ))}

        {/* Trains on platforms */}
        {trainsOnPlat.map((p, i) => (
          <g key={i}>
            <rect x={padding.l + innerW * 0.3} y={p.y - 10} width={innerW * 0.4} height={20} fill={p.color} opacity={0.8} rx={4} />
            <text x={padding.l + innerW * 0.3 + 6} y={p.y + 4} fontSize={12} fill="#000" style={{ fontWeight: 700 }}>{p.label}</text>
          </g>
        ))}

        {/* Moving trains on tracks */}
        {trainsOnTracks.map((t, i) => (
          <g key={i}>
            <circle cx={t.x} cy={t.y} r={8} fill={t.color} />
            <text x={t.x + 10} y={t.y + 4} fontSize={12} fill="#99a">{t.label}</text>
          </g>
        ))}
      </svg>
    </div>
  )
}
