export function DataTable({ columns, rows }: { columns: { key: string; label: string }[]; rows: any[] }) {
  return (
    <table className="table">
      <thead>
        <tr>
          {columns.map(c => <th key={c.key}>{c.label}</th>)}
        </tr>
      </thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i}>
            {columns.map(c => <td key={c.key}>{r[c.key]}</td>)}
          </tr>
        ))}
      </tbody>
    </table>
  )
}

