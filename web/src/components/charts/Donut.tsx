import { Plot } from '../../lib/plotly'

export function Donut({ data, title }: { data: Record<string, number>, title?: string }) {
  const labels = Object.keys(data)
  const values = Object.values(data)
  return (
    <Plot
      data={[{ type: 'pie', values, labels, hole: 0.55, textinfo: 'label+percent', marker: { line: { color: '#0b132b', width: 2 } } }]}
      layout={{
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        height: 260,
        margin: { l: 10, r: 10, t: title ? 30 : 10, b: 10 },
        title: title || undefined,
        font: { color: '#e0e6f8' },
        showlegend: false,
      }}
      config={{ displayModeBar: false, responsive: true }}
      style={{ width: '100%' }}
    />
  )
}
