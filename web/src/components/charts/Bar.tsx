import { Plot } from '../../lib/plotly'

export function Bar({ x, y, title, horizontal = false }: { x: (string|number)[], y: number[], title?: string, horizontal?: boolean }) {
  const trace: any = { type: 'bar', x, y, marker: { color: '#5bc0be' } }
  if (horizontal) trace.orientation = 'h'
  return (
    <Plot
      data={[trace]}
      layout={{
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        height: 320,
        margin: { l: 60, r: 20, t: title ? 30 : 10, b: 60 },
        title: title || undefined,
        font: { color: '#e0e6f8' },
      }}
      config={{ displayModeBar: false, responsive: true }}
      style={{ width: '100%' }}
    />
  )
}

