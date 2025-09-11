import { useState } from 'react'
import { ScopeBar } from '../components/ScopeBar'

export default function LabPage() {
  const [note] = useState('Run apply-and-validate via scripts or add endpoints as needed.')
  return (
    <div>
      <h2>Analyst Lab</h2>
      <ScopeBar />
      <div className="card"><div className="muted">Note</div><div>{note}</div></div>
    </div>
  )
}

