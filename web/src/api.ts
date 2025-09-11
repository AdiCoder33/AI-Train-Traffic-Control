export type Principal = { user: string; role: string }

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

  async login(username: string, password: string): Promise<{ token: string; role: string; username: string }> {
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

  async getRadar(scope: string, date: string): Promise<{ radar: any[]; risk_kpis: Record<string, any> }> {
    const u = new URL(this.base + '/radar')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(`radar failed: ${res.status}`)
    return res.json()
  }

  async getRecommendations(scope: string, date: string): Promise<{ rec_plan: any[]; alt_options: any[]; plan_metrics: Record<string, any> }> {
    const u = new URL(this.base + '/recommendations')
    u.searchParams.set('scope', scope)
    u.searchParams.set('date', date)
    const res = await fetch(u.toString(), { headers: this.headers() })
    if (!res.ok) throw new Error(`reco failed: ${res.status}`)
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
  async adminCreateUser(username: string, password: string, role: string): Promise<any> {
    const res = await fetch(this.base + '/admin/users', {
      method: 'POST', headers: this.headers(), body: JSON.stringify({ username, password, role })
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
}

export function makeClientFromStorage(): ApiClient {
  const user = localStorage.getItem('user') || ''
  const role = localStorage.getItem('role') || 'AN'
  const token = localStorage.getItem('token') || undefined
  const apiBase = localStorage.getItem('apiBase') || API_BASE
  return new ApiClient({ apiBase, token, user, role })
}
