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

function Protected({ children }: { children: JSX.Element }) {
  const { token, user, role } = useSession()
  const loc = useLocation()
  if (!token && !(user && role)) {
    return <Navigate to="/login" state={{ from: loc }} replace />
  }
  return children
}

function AppRoutes() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/" element={<Protected><AppShell><OverviewPage /></AppShell></Protected>} />
      <Route path="/board" element={<Protected><AppShell><BoardPage /></AppShell></Protected>} />
      <Route path="/radar" element={<Protected><AppShell><RadarPage /></AppShell></Protected>} />
      <Route path="/reco" element={<Protected><AppShell><RecommendationsPage /></AppShell></Protected>} />
      <Route path="/audit" element={<Protected><AppShell><AuditPage /></AppShell></Protected>} />
      <Route path="/policy" element={<Protected><AppShell><PolicyPage /></AppShell></Protected>} />
      <Route path="/crew" element={<Protected><AppShell><CrewPage /></AppShell></Protected>} />
      <Route path="/assistant" element={<Protected><AppShell><AssistantPage /></AppShell></Protected>} />
      <Route path="/lab" element={<Protected><AppShell><LabPage /></AppShell></Protected>} />
      <Route path="/admin" element={<Protected><AppShell><AdminPage /></AppShell></Protected>} />
      <Route path="*" element={<Navigate to="/" replace />} />
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
