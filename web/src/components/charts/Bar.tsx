import { Plot } from '../../lib/plotly'
import { colorForKey } from '../../lib/colors'

type BarSeries = {
  name?: string
  x: (string | number)[]
  y: number[]
  colorKey?: string
}

type BarProps = {
  x?: (string | number)[]
  y?: number[]
  title?: string
  horizontal?: boolean
  series?: BarSeries[]
}

export function Bar({ x = [], y = [], title, horizontal = false, series }: BarProps) {
  const multiSeries = series && series.length > 0 ? series : null
  const traces = multiSeries
    ? multiSeries.map((s, idx) => {
        const trace: any = {
          type: 'bar',
          x: s.x,
          y: s.y,
          marker: { color: colorForKey(String(s.colorKey ?? s.name ?? idx)) },
        }
        if (horizontal) trace.orientation = 'h'
        if (s.name) trace.name = s.name
        return trace
      })
    : [(() => {
        const trace: any = { type: 'bar', x, y, marker: { color: '#5bc0be' } }
        if (horizontal) trace.orientation = 'h'
        return trace
      })()]

  return (
    <Plot
      data={traces}
      layout={{
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        height: 320,
        margin: { l: 60, r: 20, t: title ? 30 : 10, b: 60 },
        title: title || undefined,
        font: { color: '#e0e6f8' },
        barmode: multiSeries && multiSeries.length > 1 ? 'group' : undefined,
      }}
      config={{ displayModeBar: false, responsive: true }}
      style={{ width: '100%' }}
    />
  )
}
