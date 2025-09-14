import { useEffect, useMemo, useState } from 'react'
import { useApi } from '../lib/session'
import { usePrefs } from '../lib/prefs'
import { Field } from '../components/Field'
import { StationMap2D } from '../components/station/StationMap2D'
import { StationMapGL } from '../components/station/StationMapGL'

const ROLES = ['SC','CREW','OM','DH','AN','ADM']

export default function AdminPage() {
  const api = useApi()
  const { scope, date } = usePrefs()
  const [users, setUsers] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [nu, setNu] = useState('')
  const [np, setNp] = useState('')
  const [nr, setNr] = useState('AN')
  const [ns, setNs] = useState('')
  const [nt, setNt] = useState('')
  const [nodes, setNodes] = useState<any[]>([])
  const [stationSel, setStationSel] = useState('')
  const [blocks, setBlocks] = useState<any[]>([])
  const [state, setState] = useState<any | null>(null)
  const [edges, setEdges] = useState<any[]>([])

  async function refresh() {
    setLoading(true)
    setErr(null)
    try {
      const res = await api.adminListUsers()
      setUsers(res.users || [])
    } catch (e: any) {
      setErr(e?.message || 'Failed to load users')
    } finally { setLoading(false) }
  }
  useEffect(() => { refresh() }, [])

  // Load stations from data (nodes)
  useEffect(() => {
    let live = true
    api.getNodes(scope, date).then(d => { if (!live) return; setNodes(d.nodes || []) }).catch(() => {})
    return () => { live = false }
  }, [api, scope, date])

  const stationOptions = useMemo(() => {
    const list = Array.from(new Set((nodes || []).map((n: any) => String(n.station_id || '')).filter(Boolean)))
    list.sort()
    return list
  }, [nodes])

  useEffect(() => {
    // Default selection for create-user and viewer
    if (!ns && stationOptions.length) setNs(stationOptions[0])
    if (!stationSel && stationOptions.length) setStationSel(stationOptions[0])
  }, [stationOptions])

  // Load map data for selected station
  useEffect(() => {
    if (!stationSel) return
    let live = true
    setErr(null)
    api.getBlockOccupancy(scope, date, stationSel).then(d => { if (!live) return; setBlocks(d.blocks || []) }).catch((e:any) => { if (!live) return; setErr(String(e)) })
    api.getState(scope, date, { station_id: stationSel }).then(d => { if (!live) return; setState(d) }).catch((e:any) => { if (!live) return; setErr(String(e)) })
    api.getEdges(scope, date, stationSel).then(d => { if (!live) return; setEdges(d.edges || []) }).catch(() => {})
    return () => { live = false }
  }, [api, scope, date, stationSel])

  async function createUser() {
    setLoading(true)
    setErr(null)
    try { await api.adminCreateUser(nu, np, nr, ns || undefined, nt || undefined); setNu(''); setNp(''); setNs(''); setNt(''); await refresh() } catch (e:any) { setErr(e?.message || 'Create failed') } finally { setLoading(false) }
  }

  async function changeRole(u: string, r: string) {
    setLoading(true)
    setErr(null)
    try { await api.adminChangeRole(u, r); await refresh() } catch (e:any) { setErr(e?.message || 'Update failed') } finally { setLoading(false) }
  }

  async function changeStation(u: string, s: string) {
    setLoading(true)
    setErr(null)
    try { await api.adminChangeStation(u, s || null); await refresh() } catch (e:any) { setErr(e?.message || 'Update failed') } finally { setLoading(false) }
  }

  async function changeTrain(u: string, t: string) {
    setLoading(true)
    setErr(null)
    try { await api.adminChangeTrain(u, t || null); await refresh() } catch (e:any) { setErr(e?.message || 'Update failed') } finally { setLoading(false) }
  }

  return (
    <div>
      <h2>Admin</h2>
      {err && <div className="card" style={{ borderColor: '#ff6b6b' }}>Error: {err}</div>}
      <div className="card" style={{ marginTop: 8 }}>
        <div style={{ fontWeight: 600, marginBottom: 8 }}>Create user</div>
        <div className="controls">
          <Field label="Username"><input value={nu} onChange={e => setNu(e.target.value)} /></Field>
          <Field label="Password"><input type="password" value={np} onChange={e => setNp(e.target.value)} /></Field>
          <Field label="Role">
            <select value={nr} onChange={e => setNr(e.target.value)}>{ROLES.map(r => <option key={r} value={r}>{r}</option>)}</select>
          </Field>
          <Field label="Station (for SC)">
            <select value={ns} onChange={e => setNs(e.target.value)}>
              <option value="">—</option>
              {stationOptions.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </Field>
          <Field label="Train (for CREW)"><input value={nt} onChange={e => setNt(e.target.value)} placeholder="e.g., 12951" /></Field>
          <button className="primary" onClick={createUser} disabled={loading || !nu || !np}>Create</button>
        </div>
      </div>
      <div className="card" style={{ marginTop: 12 }}>
        <div className="hstack" style={{ alignItems: 'center' }}>
          <strong>Users</strong>
          <span className="spacer" />
          <button onClick={refresh} disabled={loading}>{loading ? 'Refreshing…' : 'Refresh'}</button>
        </div>
        <table className="table">
          <thead><tr><th>User</th><th>Role</th><th>Station</th><th>Train</th><th>Change</th></tr></thead>
          <tbody>
            {users.map((u, i) => (
              <tr key={i}>
                <td>{u.username || u.user || u.name}</td>
                <td>{u.role}</td>
                <td>
                  <input defaultValue={u.station_id || ''} onBlur={e => changeStation(u.username || u.user || u.name, e.target.value)} placeholder="e.g., NDLS" style={{ width: 100 }} />
                </td>
                <td>
                  <input defaultValue={u.train_id || ''} onBlur={e => changeTrain(u.username || u.user || u.name, e.target.value)} placeholder="e.g., 12951" style={{ width: 100 }} />
                </td>
                <td>
                  <select defaultValue={u.role} onChange={e => changeRole(u.username || u.user || u.name, e.target.value)}>
                    {ROLES.map(r => <option key={r} value={r}>{r}</option>)}
                  </select>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="card" style={{ marginTop: 12 }}>
        <div className="hstack" style={{ alignItems: 'center', gap: 8 }}>
          <strong>Station Map (Admin Preview)</strong>
          <span className="spacer" />
          <Field label="Station">
            <select value={stationSel} onChange={e => setStationSel(e.target.value)}>
              {stationOptions.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </Field>
        </div>
        {stationSel ? (
          <>
            <StationMapGL
              stationId={stationSel}
              center={useMemo(() => {
                const node = (nodes || []).find((n: any) => String(n.station_id || '') === String(stationSel))
                const lat = Number(node?.lat ?? node?.latitude)
                const lon = Number(node?.lon ?? node?.longitude)
                return isNaN(lat) || isNaN(lon) ? null : { lat, lon }
              }, [nodes, stationSel])}
              edges={edges}
              blocks={blocks}
            />
            <div style={{ height: 12 }} />
            <StationMap2D
              stationId={stationSel}
              platforms={useMemo(() => {
                const node = (nodes || []).find((n: any) => String(n.station_id || '') === String(stationSel))
                const p = Number(node?.platforms ?? 0)
                return isNaN(p) || p <= 0 ? 4 : p
              }, [nodes, stationSel])}
              blocks={blocks}
              platformsOcc={state?.platform_occupancy || []}
            />
          </>
        ) : (
          <div className="muted">Select a station to preview its map.</div>
        )}
      </div>
    </div>
  )
}
