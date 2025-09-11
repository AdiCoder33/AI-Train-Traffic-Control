import { Plot } from '../../lib/plotly'

export function MapScatter({ nodes }: { nodes: { latitude?: number, longitude?: number, lat?: number, lon?: number, station_id?: string, name?: string }[] }) {
  const xs: number[] = []
  const ys: number[] = []
  const labels: string[] = []
  nodes.forEach(n => {
    const lat = (n as any).latitude ?? (n as any).lat
    const lon = (n as any).longitude ?? (n as any).lon
    if (typeof lat === 'number' && typeof lon === 'number') {
      ys.push(lat)
      xs.push(lon)
      labels.push(String(n.station_id || n.name || ''))
    }
  })
  return (
    <Plot
      data={[{ type: 'scatter', mode: 'markers+text', x: xs, y: ys, text: labels, textposition: 'top center', marker: { size: 8, color: '#5bc0be' } }]}
      layout={{ height: 500, paper_bgcolor: 'rgba(0,0,0,0)', plot_bgcolor: 'rgba(0,0,0,0)', font: { color: '#e0e6f8' }, xaxis: { title: 'lon' }, yaxis: { title: 'lat' } }}
      config={{ displayModeBar: false, responsive: true }}
      style={{ width: '100%' }}
    />
  )
}

