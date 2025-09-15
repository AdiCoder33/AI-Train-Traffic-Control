import { Plot } from '../../lib/plotly'

export type Series = { name: string, x: (string|Date)[], y: number[], color?: string }

export function Timeseries({ series, title }: { series: Series[], title?: string }) {
  const traces = series.map(s => ({ type: 'scatter', mode: 'lines', name: s.name, x: s.x, y: s.y, line: { shape: 'hv', width: 2, color: s.color || undefined } }))
  return (
    <Plot
      data={traces as any}
      layout={{
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        height: 320,
        margin: { l: 60, r: 20, t: title ? 30 : 10, b: 40 },
        title: title || undefined,
        font: { color: '#e0e6f8' },
        xaxis: { type: 'date' },
      }}
      config={{ displayModeBar: false, responsive: true }}
      style={{ width: '100%' }}
    />
  )
}

