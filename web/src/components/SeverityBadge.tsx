export function SeverityBadge({ severity }: { severity?: string }) {
  const cls = severity === 'Critical' ? 'status-critical' : severity === 'High' ? 'status-high' : ''
  return <span className={`badge ${cls}`}>{severity || 'Unknown'}</span>
}

