import * as THREE from 'three'
import { useMemo } from 'react'
import { Topology } from '../../types/domain'

export function PlatformMesh({ topology }: { topology: Topology }) {
  const meshes = useMemo(() => {
    const t = topology.tracks[0]
    return topology.platforms.map((pf, i) => {
      const u = (pf.s + pf.e) / 2
      const idx = Math.round(u * (t.points.length - 1))
      const p = t.points[Math.max(0, Math.min(idx, t.points.length - 1))]
      return { id: pf.id, pos: p as [number, number, number] }
    })
  }, [topology])
  return (
    <group>
      {meshes.map(m => (
        <mesh key={m.id} position={[m.pos[0], m.pos[1], m.pos[2]]}>
          <boxGeometry args={[20, 2, 6]} />
          <meshStandardMaterial color="#1f2a52" />
        </mesh>
      ))}
    </group>
  )
}

