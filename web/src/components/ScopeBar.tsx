import { Field } from './Field'
import { usePrefs } from '../lib/prefs'

export function ScopeBar() {
  const { scope, setScope, date, setDate, stationId, setStationId, trainId, setTrainId } = usePrefs()
  return (
    <div className="controls" style={{ marginBottom: 12 }}>
      <Field label="Scope"><input value={scope} onChange={e => setScope(e.target.value)} /></Field>
      <Field label="Date"><input value={date} onChange={e => setDate(e.target.value)} /></Field>
      <Field label="Station"><input value={stationId} onChange={e => setStationId(e.target.value)} placeholder="optional" /></Field>
      <Field label="Train"><input value={trainId} onChange={e => setTrainId(e.target.value)} placeholder="optional" /></Field>
    </div>
  )
}

