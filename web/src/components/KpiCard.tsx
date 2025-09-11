export function KpiCard({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="card">
      <div className="muted">{label}</div>
      <div style={{ fontSize: 28, fontWeight: 700 }}>{value}</div>
    </div>
  )
}

