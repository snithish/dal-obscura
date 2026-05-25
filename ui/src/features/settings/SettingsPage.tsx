export function SettingsPage() {
  return (
    <div className="grid gap-5">
      <header>
        <p className="text-xs font-black uppercase tracking-wide text-muted">Operations</p>
        <h1 className="mt-1 text-3xl font-black">Settings</h1>
      </header>
      <section className="rounded-card border border-border bg-white p-5 shadow-panel">
        <h2 className="text-lg font-black">Runtime configuration</h2>
        <p className="mt-2 text-sm text-muted">
          Ticket limits, path rules, authentication mapping, and developer mode
          settings will live here.
        </p>
      </section>
    </div>
  );
}
