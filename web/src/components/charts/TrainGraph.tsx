import Plot from 'react-plotly.js'

type Item = { train_id: string; station_id: string; t: string }

export function TrainGraph({ rows }: { rows: any[] }) {
  // Build ordered station list and per-train sequences from platform_occupancy
  const stations = Array.from(new Set(rows.map(r => String(r.station_id || ''))))
  stations.sort()
  const idx: Record<string, number> = {}
  stations.forEach((s, i) => { idx[s] = i })
  const byTrain: Record<string, Item[]> = {}
  rows.forEach((r: any) => {
    const tid = String(r.train_id || '')
    const sid = String(r.station_id || '')
    const t = r.arr_platform || r.dep_platform
    if (!t || !sid || !tid) return
    if (!byTrain[tid]) byTrain[tid] = []
    byTrain[tid].push({ train_id: tid, station_id: sid, t })
  })
  const series = Object.entries(byTrain).map(([tid, items]) => ({
    x: items.map(i => i.t), y: items.map(i => idx[i.station_id]), mode: 'lines+markers', name: tid,
  }))
  return (
    <Plot data={series as any}
      layout={{
        autosize: true,
        height: 360,
        margin: { l: 50, r: 20, t: 20, b: 40 },
        yaxis: { tickmode: 'array', tickvals: stations.map((_, i) => i), ticktext: stations },
        xaxis: { title: 'Time' }
      }}
      style={{ width: '100%' }}
    />
  )
}

