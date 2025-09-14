import { useEffect } from 'react'
import { useStore } from '../../state/store'

export function TimelineBar() {
  const simClock = useStore(s => s.simClock)
  const setSimClock = useStore(s => s.setSimClock)
  const playing = useStore(s => s.playing)
  const setPlaying = useStore(s => s.setPlaying)
  const timeline = useStore(s => s.timeline)
  const max = Math.max(1, timeline?.duration ?? 600000)
  function fmt(ms: number) {
    const s = Math.max(0, Math.floor(ms / 1000))
    const h = Math.floor(s / 3600)
    const m = Math.floor((s % 3600) / 60)
    const ss = s % 60
    return (h > 0 ? `${h}:${String(m).padStart(2,'0')}:${String(ss).padStart(2,'0')}` : `${m}:${String(ss).padStart(2,'0')}`)
  }
  return (
    <div className="card" style={{ marginTop: 8 }}>
      <div className="hstack" style={{ gap: 8 }}>
        <button onClick={() => setPlaying(!playing)}>{playing ? 'Pause' : 'Play'}</button>
        <input type="range" min={0} max={max} value={simClock} onChange={e => setSimClock(Number(e.target.value))} style={{ flex: 1 }} />
        <span className="muted">{fmt(simClock)} / {fmt(max)}</span>
      </div>
    </div>
  )
}
