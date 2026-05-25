export function PublishPage() {
  return (
    <div className="grid gap-5">
      <header>
        <p className="text-xs font-black uppercase tracking-wide text-muted">Release review</p>
        <h1 className="mt-1 text-3xl font-black">Publish</h1>
      </header>
      <section className="rounded-card border border-border bg-white p-5 shadow-panel">
        <h2 className="text-lg font-black">No draft changes</h2>
        <p className="mt-2 text-sm text-muted">
          Draft catalog, asset, policy, and runtime changes will be validated here
          before publication.
        </p>
      </section>
    </div>
  );
}
