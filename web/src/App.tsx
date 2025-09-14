import { Route, Routes, Navigate, useLocation } from 'react-router-dom'
import { SessionProvider, useSession } from './lib/session'
import { AppShell } from './components/AppShell'
import LoginPage from './pages/Login'
import OverviewPage from './pages/Overview'
import BoardPage from './pages/Board'
import RadarPage from './pages/Radar'
import RecommendationsPage from './pages/Recommendations'
import AuditPage from './pages/Audit'
import PolicyPage from './pages/Policy'
import CrewPage from './pages/Crew'
import AssistantPage from './pages/Assistant'
import LabPage from './pages/Lab'
import AdminPage from './pages/Admin'
import StationPage from './pages/Station'
import Ops3DPage from './pages/Ops3D'
import CorridorPage from './pages/Corridor'
import MaintPage from './pages/Maint'
import OpsPage from './pages/Ops'
import ITPage from './pages/IT'
import AnalyticsPage from './pages/Analytics'
import ExecPage from './pages/Exec'

function Protected({ children }: { children: JSX.Element }) {
  const { token, user, role } = useSession()
  const loc = useLocation()
  if (!token && !(user && role)) {
    return <Navigate to="/login" state={{ from: loc }} replace />
  }
  return children
}

function Home() {
  const { role } = useSession()
  if (role === 'SC') return <Navigate to="/station" replace />
  if (role === 'CREW') return <Navigate to="/crew" replace />
  return <Navigate to="/" replace />
}

function AppRoutes() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/" element={<Protected><AppShell><OverviewPage /></AppShell></Protected>} />
      <Route path="/station" element={<Protected><AppShell><StationPage /></AppShell></Protected>} />
      <Route path="/ops3d" element={<Protected><AppShell><Ops3DPage /></AppShell></Protected>} />
      <Route path="/board" element={<Protected><AppShell><BoardPage /></AppShell></Protected>} />
      <Route path="/radar" element={<Protected><AppShell><RadarPage /></AppShell></Protected>} />
      <Route path="/reco" element={<Protected><AppShell><RecommendationsPage /></AppShell></Protected>} />
      <Route path="/corridor" element={<Protected><AppShell><CorridorPage /></AppShell></Protected>} />
      <Route path="/maint" element={<Protected><AppShell><MaintPage /></AppShell></Protected>} />
      <Route path="/ops" element={<Protected><AppShell><OpsPage /></AppShell></Protected>} />
      <Route path="/it" element={<Protected><AppShell><ITPage /></AppShell></Protected>} />
      <Route path="/analytics" element={<Protected><AppShell><AnalyticsPage /></AppShell></Protected>} />
      <Route path="/exec" element={<Protected><AppShell><ExecPage /></AppShell></Protected>} />
      <Route path="/audit" element={<Protected><AppShell><AuditPage /></AppShell></Protected>} />
      <Route path="/policy" element={<Protected><AppShell><PolicyPage /></AppShell></Protected>} />
      <Route path="/crew" element={<Protected><AppShell><CrewPage /></AppShell></Protected>} />
      <Route path="/assistant" element={<Protected><AppShell><AssistantPage /></AppShell></Protected>} />
      <Route path="/lab" element={<Protected><AppShell><LabPage /></AppShell></Protected>} />
      <Route path="/admin" element={<Protected><AppShell><AdminPage /></AppShell></Protected>} />
      <Route path="*" element={<Home />} />
    </Routes>
  )
}

export default function App() {
  return (
    <SessionProvider>
      <AppRoutes />
    </SessionProvider>
  )
}
