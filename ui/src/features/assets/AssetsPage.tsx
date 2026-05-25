const stats = [
  ["Total assets", "0"],
  ["Unowned", "0"],
  ["Missing policy", "0"],
  ["Draft changes", "0"],
];

export function AssetsPage() {
  return (
    <div className="grid gap-5">
      <header className="flex items-end justify-between gap-4">
        <div>
          <p className="text-xs font-black uppercase tracking-wide text-muted">
            Asset workspace
          </p>
          <h1 className="mt-1 text-3xl font-black tracking-normal">Assets</h1>
        </div>
        <button className="rounded-card bg-accent px-4 py-2 text-sm font-black text-white">
          Add catalog
        </button>
      </header>

      <section className="grid grid-cols-4 gap-3">
        {stats.map(([label, value]) => (
          <div className="rounded-card border border-border bg-white p-4 shadow-panel" key={label}>
            <span className="text-xs font-bold text-muted">{label}</span>
            <strong className="mt-2 block text-3xl font-black">{value}</strong>
          </div>
        ))}
      </section>

      <section className="rounded-card border border-border bg-white p-4 shadow-panel">
        <div className="grid grid-cols-[minmax(240px,1fr)_180px_180px_180px] gap-3">
          <input className="field" placeholder="Search governed assets" />
          <select className="field" defaultValue="">
            <option value="">All catalogs</option>
          </select>
          <select className="field" defaultValue="">
            <option value="">All owners</option>
          </select>
          <select className="field" defaultValue="">
            <option value="">All statuses</option>
          </select>
        </div>
        <div className="mt-4 rounded-card border border-dashed border-border p-10 text-center">
          <h2 className="text-lg font-black">No governed assets yet</h2>
          <p className="mt-2 text-sm text-muted">
            Add a catalog, then promote discovered tables into governed assets.
          </p>
        </div>
      </section>
    </div>
  );
}
