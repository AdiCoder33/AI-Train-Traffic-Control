import { Plot } from '../../lib/plotly'

export function Histogram({ values, nbins = 20, title }: { values: number[], nbins?: number, title?: string }) {
  return (
    <Plot
      data={[{ type: 'histogram', x: values, nbinsx: nbins, marker: { color: '#8fb4ff' } }]}
      layout={{
        paper_bgcolor: 'rgba(0,0,0,0)',
        plot_bgcolor: 'rgba(0,0,0,0)',
        height: 300,
        margin: { l: 60, r: 20, t: title ? 30 : 10, b: 60 },
        title: title || undefined,
        font: { color: '#e0e6f8' },
        bargap: 0.02,
      }}
      config={{ displayModeBar: false, responsive: true }}
      style={{ width: '100%' }}
    />
  )
}

