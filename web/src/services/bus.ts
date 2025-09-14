import { ApiClient } from '../api'
import { useStore } from '../state/store'
import { Topology, Track, Block, Platform, Signal, Rec } from '../types/domain'
import dayjs from 'dayjs'
import utc from 'dayjs/plugin/utc'
dayjs.extend(utc)

function projectSchematic(topologyId: string, edges: any[], blocksIn: any[]): Topology {
  // Build station list from edges; fallback to unique u/v from blocks if edges empty
  const stations: string[] = []
  const seen = new Set<string>()
  function add(s?: string | null) { if (!s) return; const k = String(s); if (!seen.has(k)) { seen.add(k); stations.push(k) } }
  if (edges && edges.length) {
    edges.forEach(e => { add(e.u); add(e.v) })
  } else if (blocksIn && blocksIn.length) {
    blocksIn.forEach(b => { add(b.u); add(b.v) })
  }
  if (stations.length === 0) stations.push('A', 'B')
  const points = stations.map((_, i) => [i * 40, 0, 0] as [number, number, number])
  const idx: Record<string, number> = {}; stations.forEach((s, i) => { idx[s] = i })
  const blocks: Block[] = (edges && edges.length ? edges : [])
    .map((e: any) => {
      const iu = idx[String(e.u)] ?? 0; const iv = idx[String(e.v)] ?? 0
      const s = Math.min(iu, iv) / Math.max(1, stations.length - 1)
      const ee = Math.max(iu, iv) / Math.max(1, stations.length - 1)
      return { id: String(e.block_id || `${e.u}-${e.v}`), trackId: 'T1', s, e: ee, lengthM: Math.abs(iv - iu) * 100 }
    })
  if (blocks.length === 0 && blocksIn && blocksIn.length) {
    const seenPairs = new Set<string>()
    for (const b of blocksIn) {
      const u = String(b.u); const v = String(b.v); const key = `${u}-${v}`
      if (seenPairs.has(key)) continue; seenPairs.add(key)
      const iu = idx[u] ?? 0; const iv = idx[v] ?? 0
      const s = Math.min(iu, iv) / Math.max(1, stations.length - 1)
      const ee = Math.max(iu, iv) / Math.max(1, stations.length - 1)
      blocks.push({ id: String(b.block_id || key), trackId: 'T1', s, e: ee, lengthM: Math.abs(iv - iu) * 100 })
    }
  }
  const platforms: Platform[] = stations.map((sid, i) => ({ id: sid, trackId: 'T1', s: i / Math.max(1, stations.length - 1) - 0.02, e: i / Math.max(1, stations.length - 1) + 0.02, lengthM: 50, name: `PF-${sid}` }))
  const signals: Signal[] = []
  const track: Track = { id: 'T1', points, blocks }
  return { sectionId: topologyId, tracks: [track], platforms, signals }
}

export async function startBus(scope: string, date: string, api: ApiClient) {
  const setTopology = useStore.getState().setTopology
  const setTimeline = useStore.getState().setTimeline
  const setRecs = useStore.getState().setRecs
  // Initial load
  const [edges, nodes, blocks, rec] = await Promise.all([
    api.getEdges(scope, date).then(d => d.edges || []),
    api.getNodes(scope, date).then(d => d.nodes || []),
    api.getBlockOccupancy(scope, date).then(d => d.blocks || []),
    api.getRecommendations(scope, date).then(d => d.rec_plan || []),
  ])
  const topo = projectSchematic('SEC-1', edges, blocks)
  setTopology(topo)
  const { buildKeyframesFromBlocks } = await import('../utils/keyframe')
  const tl = buildKeyframesFromBlocks(topo, blocks)
  setTimeline(tl)
  const recs: Rec[] = (rec || []).slice(0, 5).map((r: any, i: number) => ({ id: r.action_id || `R${i}`, label: r.why || r.reason || 'Rec', delta: { sumDelay: 0 }, actions: [{ op: 'hold', trainId: String(r.train_id), minutes: Number(r.minutes || 2) } as any], why: (r.binding_constraints || []).map((c: string) => String(c)) }))
  setRecs(recs)
  // Poll updates
  setInterval(async () => {
    try {
      const rec2 = await api.getRecommendations(scope, date)
      const rr: Rec[] = (rec2.rec_plan || []).slice(0, 5).map((r: any, i: number) => ({ id: r.action_id || `R${i}`, label: r.why || r.reason || 'Rec', delta: { sumDelay: 0 }, actions: [{ op: 'hold', trainId: String(r.train_id), minutes: Number(r.minutes || 2) } as any], why: (r.binding_constraints || []).map((c: string) => String(c)) }))
      setRecs(rr)
    } catch {}
  }, 5000)
}
