import { NavLink } from 'react-router-dom'
import { useSession } from '../lib/session'
import { Field } from './Field'

export function Sidebar() {
  const { apiBase, setApiBase, user, role, logout } = useSession()
  return (
    <aside className="sidebar">
      <div className="brand">Train Control</div>
      <div className="muted">Signed in</div>
      <div className="hstack" style={{ marginBottom: 8 }}>
        <span className="badge">{user}</span>
        <span className="badge">{role}</span>
      </div>
      <div className="nav" style={{ marginTop: 8 }}>
        <NavLink to="/" end>Overview</NavLink>
        <NavLink to="/board">Board</NavLink>
        <NavLink to="/radar">Radar</NavLink>
        <NavLink to="/reco">Recommendations</NavLink>
        <NavLink to="/audit">Audit</NavLink>
        <NavLink to="/policy">Policy</NavLink>
        <NavLink to="/crew">Crew</NavLink>
        <NavLink to="/assistant">Assistant</NavLink>
        <NavLink to="/lab">Lab</NavLink>
        <NavLink to="/admin">Admin</NavLink>
      </div>
      <div style={{ height: 16 }} />
      <div className="muted">API Base</div>
      <div className="controls"><Field label=""><input value={apiBase} onChange={e => setApiBase(e.target.value)} /></Field></div>
      <button onClick={logout} style={{ marginTop: 8 }}>Sign out</button>
    </aside>
  )
}

