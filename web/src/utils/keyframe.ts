import dayjs from 'dayjs'
import utc from 'dayjs/plugin/utc'
dayjs.extend(utc)

import { Topology, Timeline, Keyframe } from '../types/domain'

export function buildKeyframesFromBlocks(topology: Topology, blocks: any[], horizonMin = 120): Timeline {
  const track = topology.tracks[0]
  // Block param lookup (center of block on track)
  const blockU: Record<string, number> = {}
  for (const b of track.blocks) blockU[b.id] = (b.s + b.e) / 2

  const times: number[] = []
  for (const r of blocks) {
    const et = dayjs.utc(r.entry_time).valueOf(); const xt = dayjs.utc(r.exit_time).valueOf()
    if (!isNaN(et)) times.push(et)
    if (!isNaN(xt)) times.push(xt)
  }
  const tMin = times.length ? Math.min(...times) : Date.now()
  const horizonMs = Math.max(1, horizonMin) * 60_000
  const tMax = tMin + horizonMs
  const t0 = tMin

  const kf: Keyframe[] = []
  for (const r of blocks) {
    const et = dayjs.utc(r.entry_time).valueOf(); const xt = dayjs.utc(r.exit_time).valueOf()
    if (isNaN(et) || isNaN(xt)) continue
    if (xt < tMin || et > tMax) continue // outside horizon
    const bid = String(r.block_id)
    const u = blockU[bid] ?? 0
    const te = Math.max(0, et - t0)
    const tx = Math.min(horizonMs, xt - t0)
    kf.push({ t: te, trainId: String(r.train_id), trackId: track.id, u })
    kf.push({ t: tx, trainId: String(r.train_id), trackId: track.id, u })
  }
  const duration = horizonMs
  return { baseClock: t0, duration, keyframes: kf }
}
