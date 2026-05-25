export function PoliciesPage() {
  return (
    <div className="grid gap-5">
      <header>
        <p className="text-xs font-black uppercase tracking-wide text-muted">Work queue</p>
        <h1 className="mt-1 text-3xl font-black">Policies</h1>
      </header>
      <section className="rounded-card border border-border bg-white p-5 shadow-panel">
        <h2 className="text-lg font-black">No policy work queued</h2>
        <p className="mt-2 text-sm text-muted">
          Policy tasks will appear here when assets are missing owners, missing
          rules, or have draft changes.
        </p>
      </section>
    </div>
  );
}
