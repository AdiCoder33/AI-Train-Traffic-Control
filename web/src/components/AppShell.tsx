import React from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { Sidebar } from './Sidebar'
import { TopBar } from './TopBar'

export function AppShell({ children }: { children: React.ReactNode }) {
  const api = useApi()
  const { scope, date } = usePrefs()
  async function onRevert() {
    try { await api.revertPlan(scope, date) } catch {}
  }
  return (
    <div className="shell">
      <Sidebar />
      <main className="content">
        <TopBar right={<button onClick={onRevert} className="badge">Revert Plan</button>} />
        {children}
      </main>
    </div>
  )}
