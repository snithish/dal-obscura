import { FormEvent, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { apiGet, apiPut } from "../../api/client";

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
  backend: string;
  table_identifier: string | null;
  owner_count: number;
  policy_status: string;
  draft_status: string;
};

type Catalog = {
  name: string;
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
  const [catalogs, setCatalogs] = useState<Catalog[]>([]);
  const [catalog, setCatalog] = useState("");
  const [target, setTarget] = useState("default.users");
  const [backend, setBackend] = useState("iceberg");
  const [tableIdentifier, setTableIdentifier] = useState("prod.users");
  const [optionsJson, setOptionsJson] = useState("{}");
  const [error, setError] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);

  async function refreshWorkspace() {
    const [loadedSummary, loadedAssets, loadedCatalogs] = await Promise.all([
      apiGet<WorkspaceSummary>("/v1/workspace/summary"),
      apiGet<Asset[]>("/v1/assets"),
      apiGet<Catalog[]>("/v1/catalogs"),
    ]);
    setSummary(loadedSummary);
    setAssets(loadedAssets);
    setCatalogs(loadedCatalogs);
    setCatalog((current) => current || loadedCatalogs[0]?.name || "");
  }

  useEffect(() => {
    void refreshWorkspace().catch(() => setError("Could not load workspace inventory."));
  }, []);

  async function submitAsset(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    setIsSaving(true);
    try {
      const options = JSON.parse(optionsJson) as Record<string, unknown>;
      await apiPut(`/v1/assets/${encodeURIComponent(catalog)}/${encodeURIComponent(target)}`, {
        backend,
        options,
        table_identifier: tableIdentifier || null,
      });
      await refreshWorkspace();
    } catch (caught) {
      setError(caught instanceof SyntaxError ? "Asset options must be valid JSON." : "Asset save failed.");
    } finally {
      setIsSaving(false);
    }
  }

  const stats = [
    ["Total assets", summary.asset_count, "Catalog tables promoted for governance"],
    ["Unowned", summary.unowned_asset_count, "Need an accountable data owner"],
    ["Missing policy", summary.missing_policy_count, "Visible before access rules exist"],
    ["Draft changes", summary.draft_change_count, "Ready for publish review"],
  ] as const;
  const catalogOptions = useMemo(() => catalogs.map((item) => item.name), [catalogs]);

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
        <div className="flex flex-wrap gap-2">
          <button
            className="btn-primary"
            type="button"
            onClick={() => document.getElementById("asset-target")?.focus()}
          >
            Promote asset
          </button>
          <Link className="btn-secondary" to="/ui/catalogs">
            Add catalog
          </Link>
        </div>
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

      <section className="grid grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1fr)_420px]">
        <div className="surface p-4">
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
                <div>
                  <strong className="block">{asset.name}</strong>
                  <span className="text-xs text-muted">
                    {asset.backend}
                    {asset.table_identifier ? ` / ${asset.table_identifier}` : ""}
                  </span>
                </div>
                <span>{asset.catalog}</span>
                <span>{asset.owner_count || "Unowned"}</span>
                <span className="badge">{asset.policy_status}</span>
                <span className="badge">{asset.draft_status}</span>
              </div>
            ))
          )}
        </div>
        </div>

        <form className="surface p-5" onSubmit={submitAsset}>
          <h2 className="text-lg font-black">Promote asset</h2>
          <p className="mt-1 text-sm leading-6 text-muted">
            Register a catalog table as a governed asset before assigning policy.
          </p>
          <label className="mt-4 block text-xs font-black uppercase tracking-wide text-muted">
            Catalog
          </label>
          {catalogOptions.length === 0 ? (
            <Link className="btn-secondary mt-2 w-full" to="/ui/catalogs">
              Configure a catalog first
            </Link>
          ) : (
            <select
              className="field mt-2"
              value={catalog}
              onChange={(event) => setCatalog(event.target.value)}
            >
              {catalogOptions.map((item) => (
                <option key={item} value={item}>
                  {item}
                </option>
              ))}
            </select>
          )}
          <label className="mt-4 block text-xs font-black uppercase tracking-wide text-muted">
            Asset target
          </label>
          <input
            className="field mt-2"
            id="asset-target"
            value={target}
            onChange={(event) => setTarget(event.target.value)}
          />
          <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-1">
            <label className="block">
              <span className="text-xs font-black uppercase tracking-wide text-muted">
                Backend
              </span>
              <select
                className="field mt-2"
                value={backend}
                onChange={(event) => setBackend(event.target.value)}
              >
                <option value="iceberg">Iceberg</option>
                <option value="file">File-backed</option>
              </select>
            </label>
            <label className="block">
              <span className="text-xs font-black uppercase tracking-wide text-muted">
                Table identifier
              </span>
              <input
                className="field mt-2"
                value={tableIdentifier}
                onChange={(event) => setTableIdentifier(event.target.value)}
              />
            </label>
          </div>
          <label className="mt-4 block text-xs font-black uppercase tracking-wide text-muted">
            Asset options JSON
          </label>
          <textarea
            className="field mt-2 min-h-[130px] py-3 font-mono text-xs leading-5"
            value={optionsJson}
            onChange={(event) => setOptionsJson(event.target.value)}
          />
          <button
            className="btn-primary mt-4 w-full"
            disabled={isSaving || catalogOptions.length === 0}
            type="submit"
          >
            {isSaving ? "Saving..." : "Save asset"}
          </button>
        </form>
      </section>
    </div>
  );
}
