import * as THREE from 'three'
import { useEffect, useMemo, useRef } from 'react'
import { useFrame } from '@react-three/fiber'
import { Topology, Timeline } from '../../types/domain'
import { useStore } from '../../state/store'

export function TrainMesh({ topology, timeline }: { topology: Topology, timeline: Timeline }) {
  const playing = useStore(s => s.playing)
  const simClock = useStore(s => s.simClock)
  const setSimClock = useStore(s => s.setSimClock)
  const byTrain = useMemo(() => {
    const m: Record<string, { t: number; u: number }[]> = {}
    for (const kf of timeline.keyframes) {
      if (!m[kf.trainId]) m[kf.trainId] = []
      m[kf.trainId].push({ t: kf.t, u: kf.u })
    }
    Object.values(m).forEach(arr => arr.sort((a, b) => a.t - b.t))
    return m
  }, [timeline])
  const refs = useRef<Record<string, THREE.Mesh>>({})
  useFrame((_, delta) => {
    if (playing) {
      const next = (simClock + delta * 1000) % Math.max(1, timeline.duration)
      setSimClock(next)
    }
    const track = topology.tracks[0]
    const pts = track.points
    for (const [tid, seq] of Object.entries(byTrain)) {
      const mesh = refs.current[tid]
      if (!mesh || seq.length === 0) continue
      // find segment
      const t = simClock
      let u = seq[0].u
      for (let i = 1; i < seq.length; i++) {
        const a = seq[i - 1], b = seq[i]
        if (t >= a.t && t <= b.t) {
          const r = (t - a.t) / Math.max(1, (b.t - a.t))
          u = a.u + (b.u - a.u) * r
          break
        }
        if (t > b.t) u = b.u
      }
      const idx = Math.round(u * (pts.length - 1))
      const p = pts[Math.max(0, Math.min(idx, pts.length - 1))]
      mesh.position.set(p[0], p[1] + 2, p[2])
    }
  })
  const trains = Object.keys(byTrain)
  return (
    <group>
      {trains.map((tid, i) => (
        <mesh key={tid} ref={(m) => { if (m) (refs.current[tid] = m) }}>
          <capsuleGeometry args={[1.5, 6, 4, 6]} />
          <meshStandardMaterial color={new THREE.Color().setHSL((i * 0.1) % 1, 0.6, 0.6)} />
        </mesh>
      ))}
    </group>
  )
}

