export type Principal = { user: string; role: string; station_id?: string | null; train_id?: string | null }

export type StateResponse = {
  platform_occupancy: any[]
  waiting_ledger: any[]
  sim_kpis: Record<string, number>
  whoami: Principal
}
const API_BASE = (import.meta.env.VITE_API_BASE as string) || 'http://127.0.0.1:8000'

export type ClientConfig = {
  apiBase?: string
  token?: string
  user?: string
  role?: string
}

export class ApiClient {
  base: string
  token?: string
  user?: string
  role?: string
  constructor(cfg: ClientConfig = {}) {
    this.base = cfg.apiBase || API_BASE
    this.token = cfg.token
    this.user = cfg.user
    this.role = cfg.role
  }

  private headers() {
    const h: Record<string, string> = { 'Content-Type': 'application/json' }
    if (this.token) h['Authorization'] = `Bearer ${this.token}`
    if (!this.token && this.user) h['x-user'] = this.user
    if (!this.token && this.role) h['x-role'] = this.role
    return h
  }

  async login(username: string, password: string): Promise<{ token: string; role: string; username: string; station_id?: string | null; train_id?: string | null }> {
    const res = await fetch(this.base + '/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, password })
    })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async whoami(): Promise<Principal> {
    const res = await fetch(this.base + '/whoami', { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json() as any
  }

  async getState(scope: string, date: string, opts?: { train_id?: string; station_id?: string }): Promise<StateResponse> {
    const u = new URL(this.base + '/state')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    if (opts?.train_id) u.searchParams.set('train_id', String(opts.train_id))
    if (opts?.station_id) u.searchParams.set('station_id', String(opts.station_id))
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(`state failed: ${res.status}`)
    return res.json()
  }


  async getRadar(scope: string, date: string, opts?: { station_id?: string; train_id?: string }): Promise<{ radar: any[]; risk_kpis: Record<string, any> }> {
    const u = new URL(this.base + '/radar')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    if (opts?.station_id) u.searchParams.set('station_id', opts.station_id)
    if (opts?.train_id) u.searchParams.set('train_id', opts.train_id)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(`radar failed: ${res.status}`)
    return res.json()
  }

  async getRecommendations(scope: string, date: string, station_id?: string): Promise<{ rec_plan: any[]; alt_options: any[]; plan_metrics: Record<string, any>; plan_apply_report?: Record<string, any> | null; plan_version?: string; audit_log?: Record<string, any> }> {
    const u = new URL(this.base + '/recommendations')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    if (station_id) u.searchParams.set('station_id', station_id)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(`reco failed: ${res.status}`)
    return res.json()
  }

  async optimize(body: { scope: string; date: string; t0?: string; horizon_min?: number; use_ga?: boolean }): Promise<any> {
    const res = await fetch(this.base + '/optimize', { method: 'POST', headers: this.headers(), body: JSON.stringify(body) })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async disruption(body: { scope: string; date: string; train_id: string; station_id: string; delay_min: number; t0?: string; horizon_min?: number; use_ga?: boolean }): Promise<any> {
    const res = await fetch(this.base + '/disruption', { method: 'POST', headers: this.headers(), body: JSON.stringify(body) })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async applyAction(scope: string, date: string, action_id: string): Promise<any> {
    const res = await fetch(this.base + '/apply', { method: 'POST', headers: this.headers(), body: JSON.stringify({ scope, date, action_id }) })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async suggest(body: { scope: string; date: string; train_id?: string | null; max_hold_min?: number; station_id?: string }): Promise<any> {
    const res = await fetch(this.base + '/ai/suggest', {
      method: 'POST',
      headers: this.headers(),
      body: JSON.stringify(body),
    })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async ask(body: { scope: string; date: string; query: string; train_id?: string | null; station_id?: string | null }): Promise<any> {
    const res = await fetch(this.base + '/ai/ask', {
      method: 'POST',
      headers: this.headers(),
      body: JSON.stringify(body),
    })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async postFeedback(scope: string, date: string, action: any, decision: 'APPLY'|'DISMISS'|'MODIFY'|'ACK', reason?: string, modified?: any): Promise<any> {
    const res = await fetch(this.base + '/feedback', { method: 'POST', headers: this.headers(), body: JSON.stringify({ scope, date, action, decision, reason: reason || '', modified: modified || null }) })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async lockStation(scope: string, date: string, station_id: string, locked: boolean): Promise<any> {
    const res = await fetch(this.base + '/locks/resource', { method: 'POST', headers: this.headers(), body: JSON.stringify({ scope, date, type: 'platform', id: station_id, locked }) })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async getAuditTrail(scope: string, date: string): Promise<{ audit_trail: any[] }> {
    const u = new URL(this.base + '/audit/trail')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async getAudit(scope: string, date: string, start_ts?: string, end_ts?: string): Promise<{ audit: any[] }> {
    const u = new URL(this.base + '/audit')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    if (start_ts) u.searchParams.set('start_ts', start_ts)
    if (end_ts) u.searchParams.set('end_ts', end_ts)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async getPolicy(scope: string, date: string): Promise<any> {
    const u = new URL(this.base + '/policy')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async crewFeed(scope: string, date: string, train_id?: string): Promise<{ instructions: any[] }> {
    const u = new URL(this.base + '/crew/feed')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    if (train_id) u.searchParams.set('train_id', train_id)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  // Admin
  async adminListUsers(): Promise<{ users: any[] }> {
    const res = await fetch(this.base + '/admin/users', { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }
  async adminCreateUser(username: string, password: string, role: string, station_id?: string | null, train_id?: string | null): Promise<any> {
    const res = await fetch(this.base + '/admin/users', {
      method: 'POST', headers: this.headers(), body: JSON.stringify({ username, password, role, station_id, train_id })
    })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }
  async adminChangeRole(username: string, role: string): Promise<any> {
    const res = await fetch(this.base + `/admin/users/${encodeURIComponent(username)}/role`, {
      method: 'PUT', headers: this.headers(), body: JSON.stringify({ role })
    })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async adminChangeStation(username: string, station_id: string | null): Promise<any> {
    const res = await fetch(this.base + `/admin/users/${encodeURIComponent(username)}/station`, {
      method: 'PUT', headers: this.headers(), body: JSON.stringify({ station_id })
    })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async adminChangeTrain(username: string, train_id: string | null): Promise<any> {
    const res = await fetch(this.base + `/admin/users/${encodeURIComponent(username)}/train`, {
      method: 'PUT', headers: this.headers(), body: JSON.stringify({ train_id })
    })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async getNodes(scope: string, date: string): Promise<{ nodes: any[] }> {
    const u = new URL(this.base + '/nodes')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async getEdges(scope: string, date: string, station_id?: string): Promise<{ edges: any[] }> {
    const u = new URL(this.base + '/edges')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    if (station_id) u.searchParams.set('station_id', station_id)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async getBlockOccupancy(scope: string, date: string, station_id?: string): Promise<{ blocks: any[] }> {
    const u = new URL(this.base + '/blocks')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    if (station_id) u.searchParams.set('station_id', station_id)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  // Predictive + Scenarios
  async trainEta(scope: string, date: string): Promise<any> {
    const u = new URL(this.base + '/admin/train_eta')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    const res = await fetch(u.toString(), { method: 'POST', headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async predictEta(scope: string, date: string, train_id: string): Promise<any> {
    const u = new URL(this.base + '/predict/eta')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    u.searchParams.set('train_id', train_id)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async buildIncidentRisk(scope: string, date: string): Promise<any> {
    const u = new URL(this.base + '/admin/build_incident_risk')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    const res = await fetch(u.toString(), { method: 'POST', headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async getRiskHeat(scope: string, date: string): Promise<{ heat: Record<string, number> }> {
    const u = new URL(this.base + '/risk/heatmap')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async runScenario(scope: string, date: string, scenario: { kind: string; params?: any; name?: string }): Promise<any> {
    const u = new URL(this.base + '/scenario/run')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    const res = await fetch(u.toString(), { method: 'POST', headers: this.headers(), body: JSON.stringify(scenario) })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async runScenarioBatch(scope: string, date: string, scenarios: { kind: string; params?: any; name?: string }[], horizon_min = 60): Promise<any> {
    const res = await fetch(this.base + '/scenario/batch', { method: 'POST', headers: this.headers(), body: JSON.stringify({ scope, date, scenarios, horizon_min }) })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async handshake(body: { scopeA: string; dateA: string; scopeB: string; dateB: string; boundary_station: string }): Promise<any> {
    const res = await fetch(this.base + '/coord/handshake', { method: 'POST', headers: this.headers(), body: JSON.stringify(body) })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async revertPlan(scope: string, date: string): Promise<any> {
    const res = await fetch(this.base + '/plan/revert', { method: 'POST', headers: this.headers(), body: JSON.stringify({ scope, date }) })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async health(): Promise<any> {
    const res = await fetch(this.base + '/healthz', { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async readiness(): Promise<any> {
    const res = await fetch(this.base + '/readiness', { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  async metrics(): Promise<string> {
    const res = await fetch(this.base + '/metrics', { headers: this.headers() })
    if (!res.ok) throw new Error(await res.text())
    return res.text()
  }
}

export function makeClientFromStorage(): ApiClient {
  const user = localStorage.getItem('user') || ''
  const role = localStorage.getItem('role') || 'AN'
  const token = localStorage.getItem('token') || undefined
  const apiBase = localStorage.getItem('apiBase') || API_BASE
  return new ApiClient({ apiBase, token, user, role })
}
