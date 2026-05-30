import { FormEvent, useEffect, useMemo, useState } from "react";

import { apiGet, apiPut } from "../../api/client";

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

type PolicyRule = {
  ordinal: number;
  effect: string;
  principals: string[];
  when: Record<string, unknown>;
  columns: string[];
  masks: Record<string, unknown>;
  row_filter: string | null;
};

type AssetDetail = Asset & {
  options: Record<string, unknown>;
  policy_rules: PolicyRule[];
};

const emptyPolicy: PolicyRule[] = [
  {
    ordinal: 1,
    effect: "allow",
    principals: ["group:data-stewards"],
    when: {},
    columns: ["*"],
    masks: {},
    row_filter: null,
  },
];

export function PoliciesPage() {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [selectedAssetId, setSelectedAssetId] = useState("");
  const [selectedAsset, setSelectedAsset] = useState<AssetDetail | null>(null);
  const [rulesJson, setRulesJson] = useState(JSON.stringify(emptyPolicy, null, 2));
  const [status, setStatus] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);

  async function refreshAssets() {
    const loadedAssets = await apiGet<Asset[]>("/v1/assets");
    setAssets(loadedAssets);
    setSelectedAssetId((current) => current || loadedAssets[0]?.id || "");
  }

  useEffect(() => {
    void refreshAssets().catch(() => setStatus("Could not load policy queue."));
  }, []);

  useEffect(() => {
    if (!selectedAssetId) {
      setSelectedAsset(null);
      setRulesJson(JSON.stringify(emptyPolicy, null, 2));
      return;
    }
    void apiGet<AssetDetail>(`/v1/assets/${selectedAssetId}`)
      .then((detail) => {
        setSelectedAsset(detail);
        setRulesJson(
          JSON.stringify(detail.policy_rules.length > 0 ? detail.policy_rules : emptyPolicy, null, 2),
        );
      })
      .catch(() => setStatus("Could not load asset policy."));
  }, [selectedAssetId]);

  async function submitPolicy(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!selectedAssetId) {
      return;
    }
    setStatus(null);
    setIsSaving(true);
    try {
      const rules = JSON.parse(rulesJson) as PolicyRule[];
      await apiPut(`/v1/assets/${selectedAssetId}/policy-rules`, { rules });
      await refreshAssets();
      const detail = await apiGet<AssetDetail>(`/v1/assets/${selectedAssetId}`);
      setSelectedAsset(detail);
      setRulesJson(JSON.stringify(detail.policy_rules, null, 2));
      setStatus("Policy saved.");
    } catch (caught) {
      setStatus(caught instanceof SyntaxError ? "Policy rules must be valid JSON." : "Policy save failed.");
    } finally {
      setIsSaving(false);
    }
  }

  const queue = useMemo(
    () =>
      assets.filter(
        (asset) => asset.policy_status === "missing" || asset.owner_count === 0,
      ),
    [assets],
  );

  return (
    <div className="grid gap-6">
      <header className="flex flex-col justify-between gap-4 md:flex-row md:items-center">
        <div>
          <p className="text-xs font-black uppercase tracking-wide text-muted">Work queue</p>
          <h1 className="mt-1 text-3xl font-black">Policies</h1>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-muted">
            Review assets that need policy attention and edit the rules published
            into the data plane.
          </p>
        </div>
        <span className="badge">{queue.length} queued</span>
      </header>

      <section className="grid grid-cols-1 gap-5 xl:grid-cols-[minmax(280px,380px)_minmax(0,1fr)]">
        <div className="surface p-5">
          <h2 className="text-lg font-black">Policy queue</h2>
          <p className="mt-1 text-sm leading-6 text-muted">
            Missing policies and unowned assets stay visible until resolved.
          </p>
          <div className="mt-5 grid gap-2">
            {assets.length === 0 ? (
              <div className="rounded-card border border-dashed border-border p-6 text-sm text-muted">
                No governed assets yet.
              </div>
            ) : (
              assets.map((asset) => (
                <button
                  className={[
                    "rounded-card border px-3 py-3 text-left",
                    selectedAssetId === asset.id
                      ? "border-accent bg-[#eefaf6]"
                      : "border-border bg-white hover:bg-soft",
                  ].join(" ")}
                  key={asset.id}
                  type="button"
                  onClick={() => setSelectedAssetId(asset.id)}
                >
                  <strong className="block text-sm">{asset.name}</strong>
                  <span className="mt-1 block text-xs text-muted">
                    {asset.catalog} / {asset.backend}
                  </span>
                  <span className="mt-3 flex flex-wrap gap-2">
                    <span className="badge">{asset.policy_status}</span>
                    <span className="badge">
                      {asset.owner_count ? `${asset.owner_count} owners` : "unowned"}
                    </span>
                  </span>
                </button>
              ))
            )}
          </div>
        </div>

        <form className="surface p-5" onSubmit={submitPolicy}>
          <div className="flex flex-col justify-between gap-3 md:flex-row md:items-start">
            <div>
              <h2 className="text-lg font-black">Policy editor</h2>
              <p className="mt-1 text-sm leading-6 text-muted">
                Rules are stored as ordered allow/deny entries with principals,
                column visibility, masks, and DuckDB row filters.
              </p>
            </div>
            {selectedAsset ? <span className="badge">{selectedAsset.policy_status}</span> : null}
          </div>
          {status ? <div className="alert mt-4">{status}</div> : null}
          {selectedAsset ? (
            <>
              <div className="mt-5 grid grid-cols-1 gap-3 md:grid-cols-3">
                <div className="rounded-card border border-border bg-soft p-3">
                  <span className="text-xs font-black text-muted">Asset</span>
                  <strong className="mt-1 block text-sm">{selectedAsset.name}</strong>
                </div>
                <div className="rounded-card border border-border bg-soft p-3">
                  <span className="text-xs font-black text-muted">Catalog</span>
                  <strong className="mt-1 block text-sm">{selectedAsset.catalog}</strong>
                </div>
                <div className="rounded-card border border-border bg-soft p-3">
                  <span className="text-xs font-black text-muted">Table</span>
                  <strong className="mt-1 block truncate text-sm">
                    {selectedAsset.table_identifier || "Not set"}
                  </strong>
                </div>
              </div>
              <label className="mt-5 block text-xs font-black uppercase tracking-wide text-muted">
                Policy rules JSON
              </label>
              <textarea
                className="field mt-2 min-h-[360px] py-3 font-mono text-xs leading-5"
                value={rulesJson}
                onChange={(event) => setRulesJson(event.target.value)}
              />
              <div className="mt-4 flex flex-wrap gap-2">
                <button className="btn-primary" disabled={isSaving} type="submit">
                  {isSaving ? "Saving..." : "Save policy"}
                </button>
                <button
                  className="btn-secondary"
                  type="button"
                  onClick={() => setRulesJson(JSON.stringify(emptyPolicy, null, 2))}
                >
                  Reset template
                </button>
              </div>
            </>
          ) : (
            <div className="mt-5 rounded-card border border-dashed border-border p-10 text-center text-sm text-muted">
              Select an asset to edit its policy.
            </div>
          )}
        </form>
      </section>
    </div>
  );
}
