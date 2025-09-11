import { Plot } from '../../lib/plotly'

export function Heatmap({ z, x, y, title }: { z: number[][], x: string[], y: string[], title?: string }) {
  return (
    <Plot
      data={[{ type: 'heatmap', z, x, y, colorscale: 'Viridis' }]}
      layout={{
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        height: 340,
        margin: { l: 40, r: 10, t: title ? 30 : 10, b: 40 },
        title: title || undefined,
        font: { color: '#e0e6f8' },
        xaxis: { tickangle: -30 },
      }}
      config={{ displayModeBar: false, responsive: true }}
      style={{ width: '100%' }}
    />
  )
}
