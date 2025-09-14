import { NavLink } from 'react-router-dom'
import { useSession } from '../lib/session'
import { Field } from './Field'
import { LayoutDashboard, List, Radar, ListChecks, ScrollText, Shield, Users, Bot, FlaskConical, Settings, Network, Wrench, LineChart, Server, Briefcase } from 'lucide-react'

export function Sidebar() {
  const { apiBase, setApiBase, user, role, principal, logout } = useSession()
  return (
    <aside className="sidebar">
      <div className="brand">Train Control</div>
      <div className="muted">Signed in</div>
      <div className="hstack" style={{ marginBottom: 8 }}>
        <span className="badge">{user}</span>
        <span className="badge">{role}</span>
        {principal?.station_id && <span className="badge">{principal.station_id}</span>}
      </div>
      <div className="nav" style={{ marginTop: 8 }}>
        {role === 'SC' ? (
          <>
            <NavLink to="/station" end><span className="hstack" style={{ gap: 8 }}><LayoutDashboard size={16}/> Station</span></NavLink>
            <NavLink to="/radar"><span className="hstack" style={{ gap: 8 }}><Radar size={16}/> Risks</span></NavLink>
            <NavLink to="/ops3d"><span className="hstack" style={{ gap: 8 }}><LayoutDashboard size={16}/> 3D Ops</span></NavLink>
            <NavLink to="/reco"><span className="hstack" style={{ gap: 8 }}><ListChecks size={16}/> Recommendations</span></NavLink>
            <NavLink to="/board"><span className="hstack" style={{ gap: 8 }}><List size={16}/> Board</span></NavLink>
            <NavLink to="/assistant"><span className="hstack" style={{ gap: 8 }}><Bot size={16}/> Assistant</span></NavLink>
          </>
        ) : role === 'CREW' ? (
          <>
            <NavLink to="/crew"><span className="hstack" style={{ gap: 8 }}><Users size={16}/> Crew</span></NavLink>
            <NavLink to="/assistant"><span className="hstack" style={{ gap: 8 }}><Bot size={16}/> Assistant</span></NavLink>
          </>
        ) : role === 'OM' || role === 'DH' || role === 'ADM' ? (
          <>
            <NavLink to="/exec"><span className="hstack" style={{ gap: 8 }}><Briefcase size={16}/> Exec</span></NavLink>
            <NavLink to="/corridor"><span className="hstack" style={{ gap: 8 }}><Network size={16}/> Corridor</span></NavLink>
            <NavLink to="/ops"><span className="hstack" style={{ gap: 8 }}><Users size={16}/> Ops</span></NavLink>
            <NavLink to="/maint"><span className="hstack" style={{ gap: 8 }}><Wrench size={16}/> Maintenance</span></NavLink>
            <NavLink to="/it"><span className="hstack" style={{ gap: 8 }}><Server size={16}/> IT/Ops</span></NavLink>
            <NavLink to="/analytics"><span className="hstack" style={{ gap: 8 }}><LineChart size={16}/> Analytics</span></NavLink>
            <NavLink to="/"><span className="hstack" style={{ gap: 8 }}><LayoutDashboard size={16}/> Overview</span></NavLink>
            <NavLink to="/radar"><span className="hstack" style={{ gap: 8 }}><Radar size={16}/> Radar</span></NavLink>
            <NavLink to="/reco"><span className="hstack" style={{ gap: 8 }}><ListChecks size={16}/> Recommendations</span></NavLink>
            <NavLink to="/policy"><span className="hstack" style={{ gap: 8 }}><Shield size={16}/> Policy</span></NavLink>
            <NavLink to="/lab"><span className="hstack" style={{ gap: 8 }}><FlaskConical size={16}/> Lab</span></NavLink>
            <NavLink to="/admin"><span className="hstack" style={{ gap: 8 }}><Settings size={16}/> Admin</span></NavLink>
          </>
        ) : (
          <>
            <NavLink to="/" end><span className="hstack" style={{ gap: 8 }}><LayoutDashboard size={16}/> Overview</span></NavLink>
            <NavLink to="/board"><span className="hstack" style={{ gap: 8 }}><List size={16}/> Board</span></NavLink>
            <NavLink to="/radar"><span className="hstack" style={{ gap: 8 }}><Radar size={16}/> Radar</span></NavLink>
            <NavLink to="/reco"><span className="hstack" style={{ gap: 8 }}><ListChecks size={16}/> Recommendations</span></NavLink>
            <NavLink to="/audit"><span className="hstack" style={{ gap: 8 }}><ScrollText size={16}/> Audit</span></NavLink>
            <NavLink to="/policy"><span className="hstack" style={{ gap: 8 }}><Shield size={16}/> Policy</span></NavLink>
            <NavLink to="/crew"><span className="hstack" style={{ gap: 8 }}><Users size={16}/> Crew</span></NavLink>
            <NavLink to="/assistant"><span className="hstack" style={{ gap: 8 }}><Bot size={16}/> Assistant</span></NavLink>
            <NavLink to="/lab"><span className="hstack" style={{ gap: 8 }}><FlaskConical size={16}/> Lab</span></NavLink>
            <NavLink to="/admin"><span className="hstack" style={{ gap: 8 }}><Settings size={16}/> Admin</span></NavLink>
          </>
        )}
      </div>
      <div style={{ height: 16 }} />
      <div className="muted">API Base</div>
      <div className="controls"><Field label=""><input value={apiBase} onChange={e => setApiBase(e.target.value)} /></Field></div>
      <button onClick={logout} style={{ marginTop: 8 }}>Sign out</button>
    </aside>
  )
}
