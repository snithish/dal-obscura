import { FormEvent, useEffect, useMemo, useState } from "react";

import { apiGet, apiPut } from "../../api/client";
import {
  CATALOG_ADAPTERS,
  catalogAdapterLabel,
  catalogPayloadFromForm,
  formFromCatalogOptions,
  type CatalogForm,
} from "./catalogLogic";

type Catalog = {
  id: string;
  name: string;
  module: string;
  options: Record<string, unknown>;
  status: string;
  governed_asset_count: number;
};

const presets = [
  {
    adapter: "iceberg" as const,
    name: "Iceberg REST",
    options: { type: "rest", uri: "http://localhost:8181", warehouse: "warehouse" },
  },
  {
    adapter: "iceberg" as const,
    name: "Iceberg SQL",
    options: { type: "sql", uri: "sqlite:///catalog.db" },
  },
  {
    adapter: "iceberg" as const,
    name: "Local development",
    options: { type: "sql", uri: "sqlite:///local-catalog.db" },
  },
];

export function CatalogsPage() {
  const [catalogs, setCatalogs] = useState<Catalog[]>([]);
  const [name, setName] = useState("analytics");
  const [catalogForm, setCatalogForm] = useState<CatalogForm>({
    adapter: "iceberg",
    type: "sql",
    uri: "sqlite:///catalog.db",
    warehouse: "",
  });
  const [error, setError] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const configuredCount = useMemo(() => catalogs.length, [catalogs]);

  async function refreshCatalogs() {
    setCatalogs(await apiGet<Catalog[]>("/v1/catalogs"));
  }

  useEffect(() => {
    void refreshCatalogs().catch(() => setError("Could not load catalogs."));
  }, []);

  async function submitCatalog(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    setIsSaving(true);
    try {
      await apiPut(`/v1/catalogs/${encodeURIComponent(name)}`, catalogPayloadFromForm(catalogForm));
      await refreshCatalogs();
    } catch {
      setError("Catalog save failed.");
    } finally {
      setIsSaving(false);
    }
  }

  return (
    <div className="grid gap-6">
      <header className="flex flex-col justify-between gap-4 md:flex-row md:items-center">
        <div>
        <p className="text-xs font-black uppercase tracking-wide text-muted">Sources</p>
          <h1 className="mt-1 text-4xl font-black">Catalogs</h1>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-muted">
            Connect Iceberg catalogs, review discovered tables, and decide which
            tables enter governance.
          </p>
        </div>
        <button
          className="btn-primary"
          type="button"
          onClick={() => document.getElementById("catalog-name")?.focus()}
        >
          Add catalog
        </button>
      </header>

      <section className="grid grid-cols-1 gap-4 xl:grid-cols-3">
        {presets.map((preset) => (
          <article className="surface p-5" key={preset.name}>
            <div className="mb-4 h-1.5 w-14 rounded-full bg-accent" />
            <h2 className="text-lg font-black">{preset.name}</h2>
            <p className="mt-2 min-h-[48px] text-sm leading-6 text-muted">
              {preset.options.type === "rest"
                ? "Connect a REST catalog with warehouse credentials."
                : "Use a SQL-backed pyiceberg catalog for local or simple deployments."}
            </p>
            <button
              className="btn-secondary mt-4"
              type="button"
              onClick={() => {
                setCatalogForm({ ...formFromCatalogOptions(preset.options), adapter: preset.adapter });
                document.getElementById("catalog-name")?.focus();
              }}
            >
              Configure
            </button>
          </article>
        ))}
      </section>

      <section className="grid grid-cols-1 gap-5 xl:grid-cols-[minmax(0,1fr)_420px]">
        <div className="surface p-5">
          <div className="flex items-center justify-between gap-4">
            <div>
              <h2 className="text-lg font-black">Connected catalogs</h2>
              <p className="mt-1 text-sm text-muted">
                Connection definitions ready to publish into the data plane.
              </p>
            </div>
            <span className="rounded-full bg-soft px-3 py-1 text-xs font-black text-muted">
              {configuredCount} configured
            </span>
          </div>
          <div className="mt-5 overflow-hidden rounded-card border border-border">
            {catalogs.length === 0 ? (
              <div className="grid place-items-center px-4 py-12 text-center">
                <h3 className="text-base font-black">No catalogs configured yet</h3>
                <p className="mt-2 max-w-md text-sm leading-6 text-muted">
                  Choose a preset or fill out the connection details to create the first catalog.
                </p>
              </div>
            ) : (
              catalogs.map((catalog) => (
                <div
                  className="grid grid-cols-1 gap-2 border-b border-border px-4 py-3 last:border-b-0 md:grid-cols-[1fr_110px_120px] md:items-center md:gap-3"
                  key={catalog.id}
                >
                  <div className="min-w-0">
                    <strong className="block truncate text-sm">{catalog.name}</strong>
                    <span className="block truncate text-xs text-muted">
                      {catalogAdapterLabel(catalog.module)}
                    </span>
                  </div>
                  <span className="badge">{catalog.status}</span>
                  <span className="text-sm font-bold md:text-right">
                    {catalog.governed_asset_count} assets
                  </span>
                </div>
              ))
            )}
          </div>
        </div>

        <form className="surface p-5" onSubmit={submitCatalog}>
          <h2 className="text-lg font-black">Catalog configuration</h2>
          <p className="mt-1 text-sm leading-6 text-muted">
            Pick the catalog adapter and provide the connection details for this deployment.
          </p>
          {error ? <div className="alert mt-4">{error}</div> : null}
          <label className="mt-4 block text-xs font-black uppercase tracking-wide text-muted">
            Name
          </label>
          <input
            className="field mt-2"
            id="catalog-name"
            value={name}
            onChange={(event) => setName(event.target.value)}
          />
          <label className="mt-4 block text-xs font-black uppercase tracking-wide text-muted">
            Catalog adapter
          </label>
          <select
            className="field mt-2"
            value={catalogForm.adapter}
            onChange={(event) =>
              setCatalogForm({
                ...catalogForm,
                adapter: event.target.value as keyof typeof CATALOG_ADAPTERS,
              })
            }
          >
            {Object.entries(CATALOG_ADAPTERS).map(([value, adapter]) => (
              <option key={value} value={value}>
                {adapter.label}
              </option>
            ))}
          </select>
          <label className="mt-4 block text-xs font-black uppercase tracking-wide text-muted">
            Catalog type
          </label>
          <select
            className="field mt-2"
            value={catalogForm.type}
            onChange={(event) =>
              setCatalogForm({ ...catalogForm, type: event.target.value as CatalogForm["type"] })
            }
          >
            <option value="sql">Iceberg SQL</option>
            <option value="rest">Iceberg REST</option>
          </select>
          <label className="mt-4 block text-xs font-black uppercase tracking-wide text-muted">
            Catalog URI
          </label>
          <input
            className="field mt-2"
            placeholder={catalogForm.type === "rest" ? "http://localhost:8181" : "sqlite:///catalog.db"}
            value={catalogForm.uri}
            onChange={(event) => setCatalogForm({ ...catalogForm, uri: event.target.value })}
          />
          {catalogForm.type === "rest" ? (
            <>
              <label className="mt-4 block text-xs font-black uppercase tracking-wide text-muted">
                Warehouse
              </label>
              <input
                className="field mt-2"
                placeholder="warehouse"
                value={catalogForm.warehouse}
                onChange={(event) =>
                  setCatalogForm({ ...catalogForm, warehouse: event.target.value })
                }
              />
            </>
          ) : null}
          <button className="btn-primary mt-4 w-full" disabled={isSaving} type="submit">
            {isSaving ? "Saving..." : "Save catalog"}
          </button>
        </form>
      </section>

      <section className="surface p-5">
        <div className="flex items-center justify-between gap-4">
          <div>
            <h2 className="text-lg font-black">Next workflow</h2>
            <p className="mt-1 text-sm text-muted">
              Discovery, asset promotion, and owner assignment belong after the
              catalog connection is saved.
            </p>
          </div>
        </div>
        <div className="mt-5 grid grid-cols-1 gap-3 text-sm md:grid-cols-3">
          {["Connect", "Promote assets", "Assign policies"].map((step, index) => (
            <div className="rounded-card border border-border bg-soft p-4" key={step}>
              <span className="text-xs font-black text-muted">Step {index + 1}</span>
              <strong className="mt-1 block">{step}</strong>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
