import { useEffect, useState } from 'react'
import { Gauge, Wifi, ShieldCheck, Clock, Play, RotateCcw } from 'lucide-react'

export function TopBar({ right }: { right?: React.ReactNode }) {
  const [now, setNow] = useState<string>('')
  const [live, setLive] = useState<boolean>(() => (localStorage.getItem('mode_live') ?? '1') === '1')
  useEffect(() => {
    const id = setInterval(() => { setNow(new Date().toISOString().replace('T',' ').replace('Z',' UTC')) }, 1000)
    return () => clearInterval(id)
  }, [])
  function toggleMode() {
    const v = !live; setLive(v); localStorage.setItem('mode_live', v ? '1' : '0')
  }
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '10px 14px', borderBottom: '1px solid #2e3a68', background: '#0c1430', position: 'sticky', top: 0, zIndex: 10 }}>
      <Gauge size={18} />
      <div style={{ fontWeight: 700 }}>Train Control</div>
      <span style={{ flex: 1 }} />
      <div className="muted" style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
        <span className="hstack" style={{ gap: 6 }}><Clock size={16} /> {now}</span>
        <button onClick={toggleMode} className="badge" title="Live/Replay">
          {live ? <span className="hstack" style={{ gap: 6 }}><Play size={14}/> Live</span> : <span className="hstack" style={{ gap: 6 }}><RotateCcw size={14}/> Replay</span>}
        </button>
        <span className="hstack" style={{ gap: 6 }}><Wifi size={16} /> Live</span>
        <span className="hstack" style={{ gap: 6 }}><ShieldCheck size={16} /> Sandbox</span>
      </div>
      {right}
    </div>
  )
}
