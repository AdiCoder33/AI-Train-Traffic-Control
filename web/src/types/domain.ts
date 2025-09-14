export type Vec3 = [number, number, number]
export type Block = { id: string; trackId: string; s: number; e: number; lengthM: number }
export type Track = { id: string; points: Vec3[]; blocks: Block[] }
export type Platform = { id: string; trackId: string; s: number; e: number; lengthM: number; name: string }
export type Signal = { id: string; trackId: string; s: number; aspect: 'R'|'Y'|'G' }
export type Topology = { sectionId: string; tracks: Track[]; platforms: Platform[]; signals: Signal[] }

export type Train = { id: string; type: 'EXP'|'SUB'|'FRT'; priority: 1|2|3; lengthM: number }
export type Event =
  | { kind:'enterBlock'; t:number; trainId:string; blockId:string; s:number }
  | { kind:'leaveBlock'; t:number; trainId:string; blockId:string; e:number }
  | { kind:'arrivePlatform'; t:number; trainId:string; platformId:string }
  | { kind:'departPlatform'; t:number; trainId:string; platformId:string }
export type Plan = { id: string; generatedAt:number; events: Event[]; kpi:{sumDelay:number; p90Delay:number; throughput:number} }

export type Keyframe = { t:number; trainId:string; trackId:string; u:number }
export type Timeline = { baseClock:number; duration:number; keyframes: Keyframe[] }

export type Command =
  | { op:'hold'; trainId:string; minutes:number }
  | { op:'swapPrecedence'; a:string; b:string }
  | { op:'assignPlatform'; trainId:string; platformId:string }
export type Rec = { id:string; label:string; delta:{sumDelay:number}; actions: Command[]; why:string[] }

