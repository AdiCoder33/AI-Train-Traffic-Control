import * as THREE from 'three'
import { useMemo } from 'react'
import { Topology } from '../../types/domain'

export function TrackMesh({ topology }: { topology: Topology }) {
  const geo = useMemo(() => {
    const g = new THREE.BufferGeometry()
    const pts = topology.tracks[0].points
    const arr = new Float32Array(pts.length * 3)
    pts.forEach((p, i) => { arr[i*3+0]=p[0]; arr[i*3+1]=p[1]; arr[i*3+2]=p[2] })
    g.setAttribute('position', new THREE.BufferAttribute(arr, 3))
    return g
  }, [topology])
  return (
    <line>
      {/* rails */}
      <bufferGeometry attach="geometry" {...(geo as any)} />
      <lineBasicMaterial attach="material" color="#5b6ea0" linewidth={2} />
    </line>
  )
}

