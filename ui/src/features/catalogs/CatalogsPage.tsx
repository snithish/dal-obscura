export function CatalogsPage() {
  return (
    <div className="grid gap-5">
      <header>
        <p className="text-xs font-black uppercase tracking-wide text-muted">Sources</p>
        <h1 className="mt-1 text-3xl font-black">Catalogs</h1>
      </header>
      <section className="rounded-card border border-border bg-white p-5 shadow-panel">
        <h2 className="text-lg font-black">Connect a catalog</h2>
        <p className="mt-2 max-w-2xl text-sm text-muted">
          Start with Iceberg REST, Iceberg SQL catalog, or a local development
          preset. Advanced module options stay behind an explicit advanced section.
        </p>
      </section>
    </div>
  );
}
