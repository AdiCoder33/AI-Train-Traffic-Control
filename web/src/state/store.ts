import create from 'zustand'
import { Topology, Plan, Timeline, Rec } from '../types/domain'

type Store = {
  topology: Topology | null
  plan: Plan | null
  timeline: Timeline | null
  recs: Rec[]
  simClock: number
  playing: boolean
  setTopology: (t: Topology) => void
  setPlan: (p: Plan) => void
  setTimeline: (tl: Timeline) => void
  setRecs: (r: Rec[]) => void
  setSimClock: (v: number) => void
  setPlaying: (v: boolean) => void
}

export const useStore = create<Store>((set) => ({
  topology: null,
  plan: null,
  timeline: null,
  recs: [],
  simClock: 0,
  playing: true,
  setTopology: (topology) => set({ topology }),
  setPlan: (plan) => set({ plan }),
  setTimeline: (timeline) => set({ timeline, simClock: timeline?.baseClock ?? 0 }),
  setRecs: (recs) => set({ recs }),
  setSimClock: (simClock) => set({ simClock }),
  setPlaying: (playing) => set({ playing }),
}))

