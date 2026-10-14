import { useMemo, useState } from "react";
import { TextInput } from "@mantine/core";

/** Inspect exact paths without mounting the entire field inventory. */
export function ColumnPathSummary({ columns, label }: { columns: string[]; label: string }) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [page, setPage] = useState(0);
  const search = query.trim().toLowerCase();
  const matches = useMemo(() => open ? columns.filter(column => column.toLowerCase().includes(search)) : [], [open, columns, search]);
  const current = Math.min(page, Math.max(0, Math.ceil(matches.length / 50) - 1));
  const start = current * 50;
  return <>
    <div className="policy-paths column-path-preview">{columns.slice(0, 5).map(column => <code key={column} title={column}>{column}</code>)}</div>
    {columns.length > 5 && <details className="column-path-summary" onToggle={event => setOpen(event.currentTarget.open)}>
      <summary>Review all {columns.length} {label}</summary>
      {open && <div className="column-path-review">
        <TextInput type="search" label={`Search ${label}`} value={query} onChange={event => { setQuery(event.currentTarget.value); setPage(0); }} />
        <p role="status">{matches.length} of {columns.length} columns</p>
        {matches.length ? <ul tabIndex={0} aria-label={label}>{matches.slice(start, start + 50).map(column => <li key={column}><code>{column}</code></li>)}</ul> : <p>No matching columns</p>}
        {matches.length > 50 && <div className="column-path-pages">
          <button type="button" className="text-action" disabled={current === 0} onClick={() => setPage(current - 1)}>Previous columns</button>
          <span>{start + 1}–{Math.min(start + 50, matches.length)} of {matches.length} matches</span>
          <button type="button" className="text-action" disabled={start + 50 >= matches.length} onClick={() => setPage(current + 1)}>Next columns</button>
        </div>}
      </div>}
    </details>}
  </>;
}
