import { Gauge, Wifi, ShieldCheck } from 'lucide-react'

export function TopBar({ right }: { right?: React.ReactNode }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 12, padding: '10px 14px', borderBottom: '1px solid #2e3a68', background: '#0c1430', position: 'sticky', top: 0, zIndex: 10 }}>
      <Gauge size={18} />
      <div style={{ fontWeight: 700 }}>Train Control</div>
      <span style={{ flex: 1 }} />
      <div className="muted" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
        <Wifi size={16} /> Live
        <ShieldCheck size={16} /> Sandbox
      </div>
      {right}
    </div>
  )
}

