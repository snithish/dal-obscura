import { Link } from "react-router-dom";

const stats = [
  ["Total assets", "0", "Catalog tables promoted for governance"],
  ["Unowned", "0", "Need an accountable data owner"],
  ["Missing policy", "0", "Visible before access rules exist"],
  ["Draft changes", "0", "Ready for publish review"],
];

export function AssetsPage() {
  return (
    <div className="grid gap-6">
      <header className="flex items-center justify-between gap-4">
        <div>
          <p className="text-xs font-black uppercase tracking-wide text-muted">
            Asset workspace
          </p>
          <h1 className="mt-1 text-4xl font-black tracking-normal">Assets</h1>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-muted">
            Search governed tables, find ownership gaps, and open the policy
            workspace for each asset.
          </p>
        </div>
        <Link className="btn-primary" to="/ui/catalogs">
          Add catalog
        </Link>
      </header>

      <section className="grid grid-cols-4 gap-3">
        {stats.map(([label, value, hint]) => (
          <div className="surface p-4" key={label}>
            <span className="text-xs font-bold text-muted">{label}</span>
            <strong className="mt-2 block text-3xl font-black">{value}</strong>
            <p className="mt-2 text-xs leading-5 text-muted">{hint}</p>
          </div>
        ))}
      </section>

      <section className="surface p-4">
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
        <div className="mt-4 overflow-hidden rounded-card border border-border">
          <div className="grid grid-cols-[2fr_1fr_1fr_1fr_1fr] bg-soft px-4 py-3 text-xs font-black uppercase tracking-wide text-muted">
            <span>Asset</span>
            <span>Catalog</span>
            <span>Owner</span>
            <span>Policy</span>
            <span>Draft</span>
          </div>
          <div className="grid place-items-center px-4 py-14 text-center">
            <h2 className="text-lg font-black">No governed assets yet</h2>
            <p className="mt-2 max-w-md text-sm leading-6 text-muted">
              Connect a catalog first. Discovered tables can then be promoted into
              governed assets and assigned to owners.
            </p>
            <Link className="btn-secondary mt-5" to="/ui/catalogs">
              Connect catalog
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}
