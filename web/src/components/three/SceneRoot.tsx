import { Canvas } from '@react-three/fiber'
import { OrbitControls } from '@react-three/drei'
import { useEffect } from 'react'
import { useStore } from '../../state/store'
import { TrackMesh } from './TrackMesh'
import { PlatformMesh } from './PlatformMesh'
import { TrainMesh } from './TrainMesh'

export function SceneRoot() {
  const topology = useStore(s => s.topology)
  const timeline = useStore(s => s.timeline)
  return (
    <Canvas camera={{ position: [0, 150, 260], fov: 50 }} style={{ height: 520, background: '#0b0f22', borderRadius: 6 }}>
      <ambientLight intensity={0.6} />
      <directionalLight position={[50, 80, 100]} intensity={0.6} />
      {topology && <TrackMesh topology={topology} />}
      {topology && <PlatformMesh topology={topology} />}
      {timeline && topology && <TrainMesh topology={topology} timeline={timeline} />}
      <OrbitControls enablePan enableRotate enableZoom />
    </Canvas>
  )
}

