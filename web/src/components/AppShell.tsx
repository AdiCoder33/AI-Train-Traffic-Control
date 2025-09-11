import React from 'react'
import { Sidebar } from './Sidebar'
import { TopBar } from './TopBar'

export function AppShell({ children }: { children: React.ReactNode }) {
  return (
    <div className="shell">
      <Sidebar />
      <main className="content">
        <TopBar />
        {children}
      </main>
    </div>
  )}
