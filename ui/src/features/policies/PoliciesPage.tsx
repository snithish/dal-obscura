import { FormEvent, useEffect, useMemo, useState } from "react";

import { apiGet, apiPost, apiPut } from "../../api/client";
import {
  canEditPolicy,
  defaultRule,
  formToRule,
  mergeColumnSelections,
  policyEditorResponsibility,
  previewPolicy,
  ruleToForm,
  validateRuleForms,
  type PolicyPreview,
  type PolicyRule,
  type PolicyRuleForm,
  type SessionActor,
} from "./policyLogic";
import {
  PreviewPanel,
  SummaryCard,
  type PreviewClaimRow,
} from "./PolicyPreviewPanel";
import { RuleCard } from "./PolicyRuleCard";

type Asset = {
  id: string;
  name: string;
  catalog: string;
  backend: string;
  table_identifier: string | null;
  owner_count: number;
  owners: string[];
  policy_status: string;
  draft_status: string;
};

type AssetDetail = Asset & {
  options: Record<string, unknown>;
  schema_fields: { name: string; type: string; nullable: boolean }[];
  policy_rules: PolicyRule[];
};

export function PoliciesPage() {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [selectedAssetId, setSelectedAssetId] = useState("");
  const [selectedAsset, setSelectedAsset] = useState<AssetDetail | null>(null);
  const [session, setSession] = useState<SessionActor | null>(null);
  const [rules, setRules] = useState<PolicyRuleForm[]>([defaultRule]);
  const [previewPrincipal, setPreviewPrincipal] = useState("user:alice@example.com");
  const [previewGroupsText, setPreviewGroupsText] = useState("data-stewards");
  const [previewClaims, setPreviewClaims] = useState<PreviewClaimRow[]>([]);
  const [status, setStatus] = useState<string | null>(null);
  const [isSaving, setIsSaving] = useState(false);
  const [isPublishing, setIsPublishing] = useState(false);

  async function refreshAssets() {
    const loadedAssets = await apiGet<Asset[]>("/v1/assets");
    setAssets(loadedAssets);
    setSelectedAssetId((current) => current || loadedAssets[0]?.id || "");
  }

  useEffect(() => {
    void Promise.all([refreshAssets(), apiGet<SessionActor>("/v1/session").then(setSession)])
      .catch(() => setStatus("Could not load policy queue."));
  }, []);

  useEffect(() => {
    if (!selectedAssetId) {
      setSelectedAsset(null);
      setRules([defaultRule]);
      return;
    }
    void apiGet<AssetDetail>(`/v1/assets/${selectedAssetId}`)
      .then((detail) => {
        setSelectedAsset(detail);
        setRules(
          detail.policy_rules.length > 0
            ? detail.policy_rules.map((rule) =>
                withSchemaColumns(ruleToForm(rule), detail.schema_fields),
              )
            : [withSchemaColumns(defaultRule, detail.schema_fields)],
        );
      })
      .catch(() => setStatus("Could not load asset policy."));
  }, [selectedAssetId]);

  async function submitPolicy(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    await savePolicyRules("Policy saved.");
  }

  async function savePolicyRules(successMessage: string): Promise<boolean> {
    if (!selectedAssetId) {
      return false;
    }
    const validationMessages = validateRuleForms(rules);
    if (validationMessages.length > 0) {
      setStatus(validationMessages.join(" "));
      return false;
    }
    setStatus(null);
    setIsSaving(true);
    try {
      await apiPut(`/v1/assets/${selectedAssetId}/policy-rules`, {
        rules: rules.map(formToRule),
      });
      await refreshAssets();
      const detail = await apiGet<AssetDetail>(`/v1/assets/${selectedAssetId}`);
      setSelectedAsset(detail);
      setRules(
        detail.policy_rules.map((rule) =>
          withSchemaColumns(ruleToForm(rule), detail.schema_fields),
        ),
      );
      setStatus(successMessage);
      return true;
    } catch {
      setStatus("Policy save failed.");
      return false;
    } finally {
      setIsSaving(false);
    }
  }

  async function publishPolicyVersion() {
    if (!selectedAssetId) {
      return;
    }
    setIsPublishing(true);
    try {
      const saved = await savePolicyRules("Policy saved. Publishing policy version...");
      if (!saved) {
        return;
      }
      const result = await apiPost<{ policy_version: number }>(
        `/v1/assets/${selectedAssetId}/policy-versions`,
      );
      await refreshAssets();
      setStatus(`Policy version ${result.policy_version} published for this asset.`);
    } catch {
      setStatus("Policy version publish failed.");
    } finally {
      setIsPublishing(false);
    }
  }

  const queue = useMemo(
    () =>
      assets.filter(
        (asset) => asset.policy_status === "missing" || asset.owner_count === 0,
      ),
    [assets],
  );
  const schemaColumns = selectedAsset?.schema_fields.map((field) => field.name) ?? [];
  const responsibility = selectedAsset
    ? policyEditorResponsibility(selectedAsset.owners)
    : null;
  const canEditSelectedPolicy = selectedAsset
    ? canEditPolicy(session, selectedAsset.owners)
    : false;
  const preview = useMemo<PolicyPreview>(
    () =>
      previewPolicy(
        rules.map(formToRule),
        {
          claims: Object.fromEntries(
            previewClaims
              .filter((claim) => claim.key.trim())
              .map((claim) => [claim.key.trim(), claim.value.trim()]),
          ),
          groups: splitList(previewGroupsText),
          principal: previewPrincipal.trim(),
        },
        schemaColumns,
      ),
    [previewClaims, previewGroupsText, previewPrincipal, rules, schemaColumns],
  );

  return (
    <div className="grid gap-6">
      <header className="flex flex-col justify-between gap-4 md:flex-row md:items-center">
        <div>
          <p className="text-xs font-black uppercase tracking-wide text-muted">Work queue</p>
          <h1 className="mt-1 text-3xl font-black">Policies</h1>
          <p className="mt-2 max-w-2xl text-sm leading-6 text-muted">
            Review assets that need policy attention, edit rules, and publish a
            new policy version for one asset at a time.
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
                Build ordered allow/deny rules with principals, columns, masks,
                optional match conditions, and DuckDB row filters.
              </p>
            </div>
            {selectedAsset ? <span className="badge">{selectedAsset.policy_status}</span> : null}
          </div>
          {status ? <div className="alert mt-4">{status}</div> : null}
          {selectedAsset ? (
            <>
              <div className="mt-5 grid grid-cols-1 gap-3 md:grid-cols-3">
                <SummaryCard label="Asset" value={selectedAsset.name} />
                <SummaryCard label="Catalog" value={selectedAsset.catalog} />
                <SummaryCard
                  label="Owners"
                  value={
                    selectedAsset.owners.length > 0
                      ? selectedAsset.owners.join(", ")
                      : "Unowned"
                  }
                />
              </div>

              {responsibility ? (
                <div className="mt-4 rounded-card border border-border bg-soft p-4">
                  <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
                    <strong className="text-sm">{responsibility.label}</strong>
                    <span className="badge">
                      {selectedAsset.owners.length > 0
                        ? `${selectedAsset.owners.length} owner${selectedAsset.owners.length === 1 ? "" : "s"}`
                        : "admin seeded"}
                    </span>
                  </div>
                  <p className="mt-2 text-sm leading-6 text-muted">{responsibility.message}</p>
                  {!canEditSelectedPolicy ? (
                    <p className="mt-2 text-sm font-bold text-muted">
                      You can inspect this policy, but only assigned owners can save rules,
                      row filters, or policy versions.
                    </p>
                  ) : null}
                </div>
              ) : null}

              <div className="mt-5 grid gap-4">
                {rules.map((rule, index) => (
                  <RuleCard
                    key={`${rule.ordinal}-${index}`}
                    rule={rule}
                    ruleIndex={index}
                    canRemove={rules.length > 1}
                    editable={canEditSelectedPolicy}
                    schemaColumns={schemaColumns}
                    onChange={(patch) => updateRule(index, patch, setRules)}
                    onRemove={() =>
                      setRules((current) =>
                        current
                          .filter((_, currentIndex) => currentIndex !== index)
                          .map((item, nextIndex) => ({ ...item, ordinal: nextIndex + 1 })),
                      )
                    }
                  />
                ))}
              </div>

              <div className="mt-4 flex flex-wrap gap-2">
                <button
                  className="btn-primary"
                  disabled={isSaving || !canEditSelectedPolicy}
                  type="submit"
                >
                  {isSaving ? "Saving..." : "Save policy"}
                </button>
                <button
                  className="btn-secondary"
                  disabled={isSaving || isPublishing || !canEditSelectedPolicy}
                  type="button"
                  onClick={() => void publishPolicyVersion()}
                >
                  {isPublishing ? "Publishing..." : "Publish policy version"}
                </button>
                <button
                  className="btn-secondary"
                  disabled={!canEditSelectedPolicy}
                  type="button"
                  onClick={() =>
                    setRules((current) => [
                      ...current,
                      withSchemaColumns(
                        { ...defaultRule, ordinal: current.length + 1 },
                        selectedAsset.schema_fields,
                      ),
                    ])
                  }
                >
                  Add rule
                </button>
                <button
                  className="btn-secondary"
                  disabled={!canEditSelectedPolicy}
                  type="button"
                  onClick={() =>
                    setRules([withSchemaColumns({ ...defaultRule }, selectedAsset.schema_fields)])
                  }
                >
                  Reset template
                </button>
              </div>

              <PreviewPanel
                claims={previewClaims}
                groupsText={previewGroupsText}
                preview={preview}
                principal={previewPrincipal}
                onClaimsChange={setPreviewClaims}
                onGroupsTextChange={setPreviewGroupsText}
                onPrincipalChange={setPreviewPrincipal}
              />
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

function updateRule(
  index: number,
  patch: Partial<PolicyRuleForm>,
  setRules: (updater: (current: PolicyRuleForm[]) => PolicyRuleForm[]) => void,
) {
  setRules((current) =>
    current.map((rule, currentIndex) =>
      currentIndex === index ? { ...rule, ...patch } : rule,
    ),
  );
}

function splitList(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function withSchemaColumns(
  rule: PolicyRuleForm,
  schemaFields: { name: string }[],
): PolicyRuleForm {
  return {
    ...rule,
    columnSelections: mergeColumnSelections(
      rule.columnSelections,
      schemaFields.map((field) => field.name),
    ),
  };
}
