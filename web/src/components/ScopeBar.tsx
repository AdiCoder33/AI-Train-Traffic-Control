import { Field } from './Field'
import { usePrefs } from '../lib/prefs'
import { useEffect } from 'react'
import { useSession } from '../lib/session'

export function ScopeBar() {
  const { scope, setScope, date, setDate, stationId, setStationId, trainId, setTrainId } = usePrefs()
  const { principal } = useSession()
  // If logged in as Station Controller (SC), lock the station to assigned one
  useEffect(() => {
    if (principal?.role === 'SC' && principal.station_id && stationId !== principal.station_id) {
      setStationId(principal.station_id)
    }
  }, [principal, stationId, setStationId])
  return (
    <div className="controls" style={{ marginBottom: 12 }}>
      <Field label="Scope"><input value={scope} onChange={e => setScope(e.target.value)} /></Field>
      <Field label="Date"><input value={date} onChange={e => setDate(e.target.value)} /></Field>
      <Field label="Station"><input value={stationId} onChange={e => setStationId(e.target.value)} placeholder="optional" disabled={principal?.role === 'SC'} /></Field>
      <Field label="Train"><input value={trainId} onChange={e => setTrainId(e.target.value)} placeholder="optional" /></Field>
    </div>
  )
}
