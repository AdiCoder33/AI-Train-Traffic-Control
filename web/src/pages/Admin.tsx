import { useEffect, useState } from 'react'
import { useApi } from '../lib/session'
import { Field } from '../components/Field'

const ROLES = ['SC','CREW','OM','DH','AN','ADM']

export default function AdminPage() {
  const api = useApi()
  const [users, setUsers] = useState<any[]>([])
  const [err, setErr] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const [nu, setNu] = useState('')
  const [np, setNp] = useState('')
  const [nr, setNr] = useState('AN')
  const [ns, setNs] = useState('')

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

  async function createUser() {
    setLoading(true)
    setErr(null)
    try { await api.adminCreateUser(nu, np, nr, ns || undefined); setNu(''); setNp(''); setNs(''); await refresh() } catch (e:any) { setErr(e?.message || 'Create failed') } finally { setLoading(false) }
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
          <Field label="Station (for SC)"><input value={ns} onChange={e => setNs(e.target.value)} placeholder="e.g., NDLS" /></Field>
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
          <thead><tr><th>User</th><th>Role</th><th>Station</th><th>Change</th></tr></thead>
          <tbody>
            {users.map((u, i) => (
              <tr key={i}>
                <td>{u.username || u.user || u.name}</td>
                <td>{u.role}</td>
                <td>
                  <input defaultValue={u.station_id || ''} onBlur={e => changeStation(u.username || u.user || u.name, e.target.value)} placeholder="e.g., NDLS" style={{ width: 100 }} />
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
    </div>
  )
}
