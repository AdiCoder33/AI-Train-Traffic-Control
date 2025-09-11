import React from 'react'

export function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
      <span className="muted" style={{ minWidth: 80 }}>{label}</span>
      {children}
    </label>
  )
}

