import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

import { apiGet } from "../../api/client";

type WorkspaceSummary = {
  catalog_count: number;
  asset_count: number;
  unowned_asset_count: number;
  missing_policy_count: number;
  draft_change_count: number;
};

type Asset = {
  id: string;
  name: string;
  catalog: string;
  owner_count: number;
  policy_status: string;
  draft_status: string;
};

export function AssetsPage() {
  const [summary, setSummary] = useState<WorkspaceSummary>({
    catalog_count: 0,
    asset_count: 0,
    unowned_asset_count: 0,
    missing_policy_count: 0,
    draft_change_count: 0,
  });
  const [assets, setAssets] = useState<Asset[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    void Promise.all([
      apiGet<WorkspaceSummary>("/v1/workspace/summary"),
      apiGet<Asset[]>("/v1/assets"),
    ])
      .then(([loadedSummary, loadedAssets]) => {
        setSummary(loadedSummary);
        setAssets(loadedAssets);
      })
      .catch(() => setError("Could not load workspace inventory."));
  }, []);

  const stats = [
    ["Total assets", summary.asset_count, "Catalog tables promoted for governance"],
    ["Unowned", summary.unowned_asset_count, "Need an accountable data owner"],
    ["Missing policy", summary.missing_policy_count, "Visible before access rules exist"],
    ["Draft changes", summary.draft_change_count, "Ready for publish review"],
  ] as const;

  return (
    <div className="grid gap-6">
      <header className="flex flex-col justify-between gap-4 md:flex-row md:items-center">
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

      <section className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-4">
        {stats.map(([label, value, hint]) => (
          <div className="surface p-4" key={label}>
            <span className="text-xs font-bold text-muted">{label}</span>
            <strong className="mt-2 block text-3xl font-black">{value}</strong>
            <p className="mt-2 text-xs leading-5 text-muted">{hint}</p>
          </div>
        ))}
      </section>

      <section className="surface p-4">
        {error ? <div className="alert mb-4">{error}</div> : null}
        <div className="grid grid-cols-1 gap-3 xl:grid-cols-[minmax(240px,1fr)_180px_180px_180px]">
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
          <div className="hidden grid-cols-[2fr_1fr_1fr_1fr_1fr] bg-soft px-4 py-3 text-xs font-black uppercase tracking-wide text-muted md:grid">
            <span>Asset</span>
            <span>Catalog</span>
            <span>Owner</span>
            <span>Policy</span>
            <span>Draft</span>
          </div>
          {assets.length === 0 ? (
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
          ) : (
            assets.map((asset) => (
              <div
                className="grid grid-cols-1 gap-2 border-t border-border px-4 py-3 text-sm md:grid-cols-[2fr_1fr_1fr_1fr_1fr] md:items-center"
                key={asset.id}
              >
                <strong>{asset.name}</strong>
                <span>{asset.catalog}</span>
                <span>{asset.owner_count || "Unowned"}</span>
                <span className="badge">{asset.policy_status}</span>
                <span className="badge">{asset.draft_status}</span>
              </div>
            ))
          )}
        </div>
      </section>
    </div>
  );
}
