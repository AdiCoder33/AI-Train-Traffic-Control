import { Plot } from '../../lib/plotly'

export type TimelineItem = { y: string, start: string | Date, end: string | Date, color?: string, label?: string }

export function Timeline({ items, title }: { items: TimelineItem[], title?: string }) {
  // Build a scatter trace per item as a horizontal line from start to end
  const traces = items.map((it, i) => ({
    type: 'scatter',
    mode: 'lines',
    x: [it.start, it.end],
    y: [it.y, it.y],
    line: { width: 10, color: it.color || '#5bc0be', shape: 'hv' },
    hoverinfo: 'text',
    text: it.label || `${it.y}`,
    showlegend: false,
  }))

  return (
    <Plot
      data={traces}
      layout={{
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        height: 360,
        margin: { l: 80, r: 20, t: title ? 30 : 10, b: 40 },
        title: title || undefined,
        font: { color: '#e0e6f8' },
        yaxis: { automargin: true, type: 'category' },
        xaxis: { type: 'date' },
      }}
      config={{ displayModeBar: false, responsive: true }}
      style={{ width: '100%' }}
    />
  )
}
