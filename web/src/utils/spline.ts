import { Vec3 } from '../types/domain'

export function catmullRom(p0: Vec3, p1: Vec3, p2: Vec3, p3: Vec3, t: number): Vec3 {
  const v0: Vec3 = [(p2[0]-p0[0]) * 0.5, (p2[1]-p0[1]) * 0.5, (p2[2]-p0[2]) * 0.5]
  const v1: Vec3 = [(p3[0]-p1[0]) * 0.5, (p3[1]-p1[1]) * 0.5, (p3[2]-p1[2]) * 0.5]
  const t2 = t*t; const t3 = t2*t
  const x = (2*p1[0]-2*p2[0]+v0[0]+v1[0])*t3 + (-3*p1[0]+3*p2[0]-2*v0[0]-v1[0])*t2 + v0[0]*t + p1[0]
  const y = (2*p1[1]-2*p2[1]+v0[1]+v1[1])*t3 + (-3*p1[1]+3*p2[1]-2*v0[1]-v1[1])*t2 + v0[1]*t + p1[1]
  const z = (2*p1[2]-2*p2[2]+v0[2]+v1[2])*t3 + (-3*p1[2]+3*p2[2]-2*v0[2]-v1[2])*t2 + v0[2]*t + p1[2]
  return [x,y,z]
}

