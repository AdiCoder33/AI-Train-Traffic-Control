import { useEffect, useState } from 'react'
import { SceneRoot } from '../components/three/SceneRoot'
import { RightPanel } from '../components/hud/RightPanel'
import { TimelineBar } from '../components/hud/TimelineBar'
import { ScopeBar } from '../components/ScopeBar'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { startBus } from '../services/bus'

export default function Ops3DPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [err, setErr] = useState<string | null>(null)
  useEffect(() => {
    let live = true
    startBus(scope, date, api).catch(e => { if (live) setErr(String(e)) })
    return () => { live = false }
  }, [api, scope, date])
  return (
    <div>
      <h2>3D Operations (R3F)</h2>
      <ScopeBar />
      {err && <div className="card" style={{ borderColor: '#ff6b6b' }}>Error: {err}</div>}
      <div className="row" style={{ alignItems: 'flex-start' }}>
        <div style={{ flex: 1, minWidth: 600 }}>
          <SceneRoot />
          <TimelineBar />
        </div>
        <RightPanel />
      </div>
    </div>
  )
}

