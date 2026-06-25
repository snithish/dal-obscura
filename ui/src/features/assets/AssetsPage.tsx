import { FormEvent, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";

import { apiGet, apiPut } from "../../api/client";
import { routes } from "../../app/routes";
import {
  assetOptionsFromForm,
  filterAssets,
  ownersFromRows,
  schemaFieldsFromRows,
  type Asset,
  type AssetFilters,
  type AssetOptionRow,
  type OwnerRow,
  type SchemaField,
  type SchemaFieldRow,
} from "./assetLogic";

type Catalog = {
  name: string;
};

type WorkspaceSummary = {
  catalog_count: number;
  asset_count: number;
  unowned_asset_count: number;
  missing_policy_count: number;
  draft_change_count: number;
};

type AssetDetail = Asset & {
  schema_fields: SchemaField[];
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
  const [snapshot, setSnapshot] = useState("");
  const [optionRows, setOptionRows] = useState<AssetOptionRow[]>([]);
  const [selectedAssetId, setSelectedAssetId] = useState("");
  const [ownerRows, setOwnerRows] = useState<OwnerRow[]>([]);
  const [schemaRows, setSchemaRows] = useState<SchemaFieldRow[]>([]);
  const [filters, setFilters] = useState<AssetFilters>({
    catalog: "",
    owner: "",
    search: "",
    status: "",
  });
  const [error, setError] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [isSavingOwners, setIsSavingOwners] = useState(false);
  const [isSavingSchema, setIsSavingSchema] = useState(false);

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
    setSelectedAssetId((current) => current || loadedAssets[0]?.id || "");
  }

  useEffect(() => {
    void refreshWorkspace().catch(() => setError("Could not load workspace inventory."));
  }, []);

  async function submitAsset(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    setIsSaving(true);
    try {
      const result = assetOptionsFromForm(snapshot, optionRows);
      if (!result.ok) {
        setError(result.error);
        return;
      }
      await apiPut(`/v1/assets/${encodeURIComponent(catalog)}/${encodeURIComponent(target)}`, {
        backend,
        options: result.options,
        table_identifier: tableIdentifier || null,
      });
      await refreshWorkspace();
    } catch {
      setError("Asset save failed.");
    } finally {
      setIsSaving(false);
    }
  }

  async function submitOwners(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedAssetId) {
      return;
    }
    setError(null);
    setIsSavingOwners(true);
    try {
      await apiPut(`/v1/assets/${selectedAssetId}/owners`, {
        owners: ownersFromRows(ownerRows),
      });
      await refreshWorkspace();
    } catch {
      setError("Owner save failed.");
    } finally {
      setIsSavingOwners(false);
    }
  }

  async function submitSchema(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedAssetId) {
      return;
    }
    setError(null);
    setIsSavingSchema(true);
    try {
      await apiPut(`/v1/assets/${selectedAssetId}/schema-fields`, {
        fields: schemaFieldsFromRows(schemaRows),
      });
      await refreshSelectedAsset(selectedAssetId);
    } catch {
      setError("Schema save failed.");
    } finally {
      setIsSavingSchema(false);
    }
  }

  async function refreshSelectedAsset(assetId: string) {
    const detail = await apiGet<AssetDetail>(`/v1/assets/${assetId}`);
    setSchemaRows(
      detail.schema_fields.length > 0
        ? detail.schema_fields
        : [{ name: "", type: "string", nullable: true }],
    );
  }

  const stats = [
    ["Total assets", summary.asset_count, "Catalog tables promoted for governance"],
    ["Unowned", summary.unowned_asset_count, "Need an accountable data owner"],
    ["Missing policy", summary.missing_policy_count, "Visible before access rules exist"],
    ["Draft changes", summary.draft_change_count, "Ready for publish review"],
  ] as const;
  const catalogOptions = useMemo(() => catalogs.map((item) => item.name), [catalogs]);
  const filteredAssets = useMemo(() => filterAssets(assets, filters), [assets, filters]);
  const selectedAsset = useMemo(
    () => assets.find((asset) => asset.id === selectedAssetId) ?? null,
    [assets, selectedAssetId],
  );
  const policyStatuses = useMemo(
    () => Array.from(new Set(assets.map((asset) => asset.policy_status))).sort(),
    [assets],
  );

  useEffect(() => {
    if (selectedAsset) {
      setOwnerRows(
        selectedAsset.owners.length > 0
          ? selectedAsset.owners.map((principal) => ({ principal }))
          : [{ principal: "" }],
      );
      void refreshSelectedAsset(selectedAsset.id).catch(() =>
        setSchemaRows([{ name: "", type: "string", nullable: true }]),
      );
    }
  }, [selectedAsset]);

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
          <Link className="btn-secondary" to={routes.catalogs}>
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
          <input
            className="field"
            placeholder="Search governed assets"
            value={filters.search}
            onChange={(event) => setFilters({ ...filters, search: event.target.value })}
          />
          <select
            className="field"
            value={filters.catalog}
            onChange={(event) => setFilters({ ...filters, catalog: event.target.value })}
          >
            <option value="">All catalogs</option>
            {catalogOptions.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))}
          </select>
          <select
            className="field"
            value={filters.owner}
            onChange={(event) => setFilters({ ...filters, owner: event.target.value })}
          >
            <option value="">All owners</option>
            <option value="owned">Owned</option>
            <option value="unowned">Unowned</option>
          </select>
          <select
            className="field"
            value={filters.status}
            onChange={(event) => setFilters({ ...filters, status: event.target.value })}
          >
            <option value="">All statuses</option>
            {policyStatuses.map((status) => (
              <option key={status} value={status}>
                {status}
              </option>
            ))}
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
              <Link className="btn-secondary mt-5" to={routes.catalogs}>
                Connect catalog
              </Link>
            </div>
          ) : filteredAssets.length === 0 ? (
            <div className="grid place-items-center px-4 py-14 text-center">
              <h2 className="text-lg font-black">No assets match these filters</h2>
              <p className="mt-2 max-w-md text-sm leading-6 text-muted">
                Clear one or more filters to return to the full governed asset list.
              </p>
              <button
                className="btn-secondary mt-5"
                type="button"
                onClick={() => setFilters({ catalog: "", owner: "", search: "", status: "" })}
              >
                Clear filters
              </button>
            </div>
          ) : (
            filteredAssets.map((asset) => (
              <button
                className={[
                  "grid w-full grid-cols-1 gap-2 border-t border-border px-4 py-3 text-left text-sm md:grid-cols-[2fr_1fr_1fr_1fr_1fr] md:items-center",
                  selectedAssetId === asset.id ? "bg-[#eefaf6]" : "hover:bg-soft",
                ].join(" ")}
                key={asset.id}
                type="button"
                onClick={() => setSelectedAssetId(asset.id)}
              >
                <div>
                  <strong className="block">{asset.name}</strong>
                  <span className="text-xs text-muted">
                    {asset.backend}
                    {asset.table_identifier ? ` / ${asset.table_identifier}` : ""}
                  </span>
                </div>
                <span>{asset.catalog}</span>
                <span>
                  {asset.owner_count ? `${asset.owner_count} owner${asset.owner_count === 1 ? "" : "s"}` : "Unowned"}
                </span>
                <span className="badge">{asset.policy_status}</span>
                <span className="badge">{asset.draft_status}</span>
              </button>
            ))
          )}
        </div>
        </div>

        <div className="grid gap-5">
        <form className="surface p-5" onSubmit={submitOwners}>
          <h2 className="text-lg font-black">Asset owners</h2>
          <p className="mt-1 text-sm leading-6 text-muted">
            Owners are accountable for policy changes on the selected asset.
          </p>
          {selectedAsset ? (
            <>
              <div className="mt-4 rounded-card border border-border bg-soft p-3">
                <span className="text-xs font-black uppercase tracking-wide text-muted">
                  Selected asset
                </span>
                <strong className="mt-1 block text-sm">{selectedAsset.name}</strong>
                <span className="mt-1 block text-xs text-muted">{selectedAsset.catalog}</span>
              </div>
              <section className="mt-4 rounded-card border border-border bg-soft p-3">
                <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center xl:flex-col xl:items-stretch">
                  <div>
                    <h3 className="text-sm font-black">Owner principals</h3>
                    <p className="mt-1 text-xs text-muted">
                      Add users, groups, or service principals that can own policy changes.
                    </p>
                  </div>
                  <button
                    className="btn-secondary"
                    type="button"
                    onClick={() => setOwnerRows([...ownerRows, { principal: "" }])}
                  >
                    Add owner
                  </button>
                </div>
                <div className="mt-3 grid gap-3">
                  {ownerRows.map((row, index) => (
                    <div
                      className="grid grid-cols-1 gap-3 rounded-card border border-border bg-white p-3 md:grid-cols-[1fr_auto] xl:grid-cols-1"
                      key={`${row.principal}-${index}`}
                    >
                      <label className="block">
                        <span className="text-xs font-black uppercase tracking-wide text-muted">
                          Principal
                        </span>
                        <input
                          className="field mt-2"
                          placeholder="user:alice@example.com"
                          value={row.principal}
                          onChange={(event) =>
                            setOwnerRows(
                              replaceAt(ownerRows, index, { principal: event.target.value }),
                            )
                          }
                        />
                      </label>
                      <button
                        className="btn-secondary self-end"
                        type="button"
                        onClick={() =>
                          setOwnerRows(
                            ownerRows.length === 1
                              ? [{ principal: "" }]
                              : ownerRows.filter(
                                  (_, currentIndex) => currentIndex !== index,
                                ),
                          )
                        }
                      >
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
              </section>
              <button className="btn-primary mt-4 w-full" disabled={isSavingOwners} type="submit">
                {isSavingOwners ? "Saving..." : "Save owners"}
              </button>
            </>
          ) : (
            <div className="mt-5 rounded-card border border-dashed border-border p-6 text-sm text-muted">
              Select an asset to assign owners.
            </div>
          )}
        </form>

        <form className="surface p-5" onSubmit={submitSchema}>
          <h2 className="text-lg font-black">Schema fields</h2>
          <p className="mt-1 text-sm leading-6 text-muted">
            Define the governed columns used by policy builders and masking controls.
          </p>
          {selectedAsset ? (
            <>
              <section className="mt-4 rounded-card border border-border bg-soft p-3">
                <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center xl:flex-col xl:items-stretch">
                  <div>
                    <h3 className="text-sm font-black">Columns</h3>
                    <p className="mt-1 text-xs text-muted">
                      Add the table columns policy authors should choose from.
                    </p>
                  </div>
                  <button
                    className="btn-secondary"
                    type="button"
                    onClick={() =>
                      setSchemaRows([...schemaRows, { name: "", type: "string", nullable: true }])
                    }
                  >
                    Add column
                  </button>
                </div>
                <div className="mt-3 grid gap-3">
                  {schemaRows.map((row, index) => (
                    <div
                      className="grid grid-cols-1 gap-3 rounded-card border border-border bg-white p-3 md:grid-cols-[1fr_150px_130px_auto] xl:grid-cols-1"
                      key={`${row.name}-${index}`}
                    >
                      <label className="block">
                        <span className="text-xs font-black uppercase tracking-wide text-muted">
                          Column
                        </span>
                        <input
                          className="field mt-2"
                          placeholder="email"
                          value={row.name}
                          onChange={(event) =>
                            setSchemaRows(
                              replaceAt(schemaRows, index, {
                                ...row,
                                name: event.target.value,
                              }),
                            )
                          }
                        />
                      </label>
                      <label className="block">
                        <span className="text-xs font-black uppercase tracking-wide text-muted">
                          Type
                        </span>
                        <select
                          className="field mt-2"
                          value={row.type}
                          onChange={(event) =>
                            setSchemaRows(
                              replaceAt(schemaRows, index, {
                                ...row,
                                type: event.target.value,
                              }),
                            )
                          }
                        >
                          <option value="string">String</option>
                          <option value="long">Long</option>
                          <option value="double">Double</option>
                          <option value="boolean">Boolean</option>
                          <option value="timestamp">Timestamp</option>
                          <option value="date">Date</option>
                        </select>
                      </label>
                      <label className="block">
                        <span className="text-xs font-black uppercase tracking-wide text-muted">
                          Nullability
                        </span>
                        <select
                          className="field mt-2"
                          value={row.nullable ? "nullable" : "required"}
                          onChange={(event) =>
                            setSchemaRows(
                              replaceAt(schemaRows, index, {
                                ...row,
                                nullable: event.target.value === "nullable",
                              }),
                            )
                          }
                        >
                          <option value="nullable">Nullable</option>
                          <option value="required">Required</option>
                        </select>
                      </label>
                      <button
                        className="btn-secondary self-end"
                        type="button"
                        onClick={() =>
                          setSchemaRows(
                            schemaRows.length === 1
                              ? [{ name: "", type: "string", nullable: true }]
                              : schemaRows.filter(
                                  (_, currentIndex) => currentIndex !== index,
                                ),
                          )
                        }
                      >
                        Remove
                      </button>
                    </div>
                  ))}
                </div>
              </section>
              <button className="btn-primary mt-4 w-full" disabled={isSavingSchema} type="submit">
                {isSavingSchema ? "Saving..." : "Save schema"}
              </button>
            </>
          ) : (
            <div className="mt-5 rounded-card border border-dashed border-border p-6 text-sm text-muted">
              Select an asset to define schema fields.
            </div>
          )}
        </form>

        <form className="surface p-5" onSubmit={submitAsset}>
          <h2 className="text-lg font-black">Promote asset</h2>
          <p className="mt-1 text-sm leading-6 text-muted">
            Register a catalog table as a governed asset before assigning policy.
          </p>
          <label className="mt-4 block text-xs font-black uppercase tracking-wide text-muted">
            Catalog
          </label>
          {catalogOptions.length === 0 ? (
            <Link className="btn-secondary mt-2 w-full" to={routes.catalogs}>
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
            Snapshot ID
          </label>
          <input
            className="field mt-2"
            inputMode="numeric"
            placeholder="Optional"
            value={snapshot}
            onChange={(event) => setSnapshot(event.target.value)}
          />
          <section className="mt-4 rounded-card border border-border bg-soft p-3">
            <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center xl:flex-col xl:items-stretch">
              <div>
                <h3 className="text-sm font-black">Additional options</h3>
                <p className="mt-1 text-xs text-muted">
                  Add simple key/value options for handlers that need extra parameters.
                </p>
              </div>
              <button
                className="btn-secondary"
                type="button"
                onClick={() => setOptionRows([...optionRows, { key: "", value: "" }])}
              >
                Add option
              </button>
            </div>
            <div className="mt-3 grid gap-3">
              {optionRows.length === 0 ? (
                <div className="rounded-card border border-dashed border-border bg-white p-4 text-sm text-muted">
                  No additional options
                </div>
              ) : (
                optionRows.map((row, index) => (
                  <div className="grid grid-cols-1 gap-3 rounded-card border border-border bg-white p-3" key={`${row.key}-${index}`}>
                    <label className="block">
                      <span className="text-xs font-black uppercase tracking-wide text-muted">
                        Key
                      </span>
                      <input
                        className="field mt-2"
                        value={row.key}
                        onChange={(event) =>
                          setOptionRows(replaceAt(optionRows, index, { ...row, key: event.target.value }))
                        }
                      />
                    </label>
                    <label className="block">
                      <span className="text-xs font-black uppercase tracking-wide text-muted">
                        Value
                      </span>
                      <input
                        className="field mt-2"
                        value={row.value}
                        onChange={(event) =>
                          setOptionRows(replaceAt(optionRows, index, { ...row, value: event.target.value }))
                        }
                      />
                    </label>
                    <button
                      className="btn-secondary"
                      type="button"
                      onClick={() =>
                        setOptionRows(optionRows.filter((_, currentIndex) => currentIndex !== index))
                      }
                    >
                      Remove
                    </button>
                  </div>
                ))
              )}
            </div>
          </section>
          <button
            className="btn-primary mt-4 w-full"
            disabled={isSaving || catalogOptions.length === 0}
            type="submit"
          >
            {isSaving ? "Saving..." : "Save asset"}
          </button>
        </form>
        </div>
      </section>
    </div>
  );
}

function replaceAt<T>(items: T[], index: number, next: T): T[] {
  return items.map((item, currentIndex) => (currentIndex === index ? next : item));
}
