import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useSession } from '../lib/session'
import { Field } from '../components/Field'

export default function LoginPage() {
  const { apiBase, setApiBase, login, setUser, setRole } = useSession()
  const [username, setUsername] = useState('admin')
  const [password, setPassword] = useState('')
  const [err, setErr] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const nav = useNavigate()

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault()
    setLoading(true)
    setErr(null)
    try {
      await login(username, password)
      nav('/')
    } catch (e: any) {
      setErr(e?.message || 'Login failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="shell">
      <div className="content" style={{ maxWidth: 420, margin: '80px auto' }}>
        <h2>Sign in</h2>
        <form onSubmit={onSubmit} className="controls">
          <Field label="API Base"><input value={apiBase} onChange={e => setApiBase(e.target.value)} placeholder="http://127.0.0.1:8000" /></Field>
          <Field label="Username"><input value={username} onChange={e => setUsername(e.target.value)} autoFocus /></Field>
          <Field label="Password"><input type="password" value={password} onChange={e => setPassword(e.target.value)} /></Field>
          <button className="primary" type="submit" disabled={loading}>{loading ? 'Signing in…' : 'Sign in'}</button>
        </form>
        {err && <div className="card" style={{ borderColor: '#ff6b6b', marginTop: 12 }}>Error: {err}</div>}
        <div className="card" style={{ marginTop: 12 }}>
          <div className="muted" style={{ marginBottom: 6 }}>Dev: continue without login</div>
          <div className="controls">
            <Field label="User"><input value={username} onChange={e => setUsername(e.target.value)} /></Field>
            <Field label="Role">
              <select defaultValue={'AN'} onChange={e => setRole(e.target.value)}>
                {['SC','CREW','OM','DH','AN','ADM'].map(r => <option key={r} value={r}>{r}</option>)}
              </select>
            </Field>
            <button onClick={() => { setUser(username); nav('/') }}>Continue</button>
          </div>
        </div>
      </div>
    </div>
  )
}
