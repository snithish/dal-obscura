export function CatalogsPage() {
  return (
    <div className="grid gap-6">
      <header className="flex items-center justify-between gap-4">
        <div>
        <p className="text-xs font-black uppercase tracking-wide text-muted">Sources</p>
          <h1 className="mt-1 text-4xl font-black">Catalogs</h1>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-muted">
            Connect Iceberg catalogs, review discovered tables, and decide which
            tables enter governance.
          </p>
        </div>
        <button className="btn-primary" type="button">
          Add catalog
        </button>
      </header>

      <section className="grid grid-cols-3 gap-4">
        {[
          ["Iceberg REST", "Connect a REST catalog with warehouse credentials."],
          ["Iceberg SQL", "Use a SQL-backed pyiceberg catalog for local or simple deployments."],
          ["Local development", "Start with a file-backed development catalog."],
        ].map(([title, body]) => (
          <article className="surface p-5" key={title}>
            <div className="mb-4 h-1.5 w-14 rounded-full bg-accent" />
            <h2 className="text-lg font-black">{title}</h2>
            <p className="mt-2 min-h-[48px] text-sm leading-6 text-muted">{body}</p>
            <button className="btn-secondary mt-4" type="button">
              Configure
            </button>
          </article>
        ))}
      </section>

      <section className="surface p-5">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-lg font-black">Connected catalogs</h2>
            <p className="mt-1 text-sm text-muted">
              Connection health and discovered tables will appear here.
            </p>
          </div>
          <span className="rounded-full bg-soft px-3 py-1 text-xs font-black text-muted">
            0 configured
          </span>
        </div>
        <div className="mt-5 rounded-card border border-dashed border-border p-8 text-sm text-muted">
          No catalogs configured yet.
        </div>
      </section>
    </div>
  );
}
