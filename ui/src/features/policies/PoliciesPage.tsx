import { FormEvent, useEffect, useMemo, useState } from "react";

import { apiGet, apiPut } from "../../api/client";
import {
  defaultRule,
  formToRule,
  mergeColumnSelections,
  previewPolicy,
  ruleToForm,
  type ColumnSelection,
  type ConditionRow,
  type MaskRow,
  type PolicyRule,
  type PolicyRuleForm,
  type PolicyPreview,
} from "./policyLogic";

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

type PreviewClaimRow = {
  key: string;
  value: string;
};

export function PoliciesPage() {
  const [assets, setAssets] = useState<Asset[]>([]);
  const [selectedAssetId, setSelectedAssetId] = useState("");
  const [selectedAsset, setSelectedAsset] = useState<AssetDetail | null>(null);
  const [rules, setRules] = useState<PolicyRuleForm[]>([defaultRule]);
  const [previewPrincipal, setPreviewPrincipal] = useState("user:alice@example.com");
  const [previewGroupsText, setPreviewGroupsText] = useState("data-stewards");
  const [previewClaims, setPreviewClaims] = useState<PreviewClaimRow[]>([]);
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
    if (!selectedAssetId) {
      return;
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
      setStatus("Policy saved.");
    } catch {
      setStatus("Policy save failed.");
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
  const schemaColumns = selectedAsset?.schema_fields.map((field) => field.name) ?? [];
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

              <div className="mt-5 grid gap-4">
                {rules.map((rule, index) => (
                  <RuleCard
                    key={`${rule.ordinal}-${index}`}
                    rule={rule}
                    ruleIndex={index}
                    canRemove={rules.length > 1}
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
                <button className="btn-primary" disabled={isSaving} type="submit">
                  {isSaving ? "Saving..." : "Save policy"}
                </button>
                <button
                  className="btn-secondary"
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

function PreviewPanel({
  claims,
  groupsText,
  onClaimsChange,
  onGroupsTextChange,
  onPrincipalChange,
  preview,
  principal,
}: {
  claims: PreviewClaimRow[];
  groupsText: string;
  onClaimsChange: (rows: PreviewClaimRow[]) => void;
  onGroupsTextChange: (value: string) => void;
  onPrincipalChange: (value: string) => void;
  preview: PolicyPreview;
  principal: string;
}) {
  return (
    <section className="mt-5 rounded-card border border-border bg-soft p-4">
      <div className="flex flex-col justify-between gap-3 md:flex-row md:items-start">
        <div>
          <h3 className="text-base font-black">Policy preview</h3>
          <p className="mt-1 text-sm leading-6 text-muted">
            Test a principal and claims against the draft rules before publishing.
          </p>
        </div>
        <span className="badge">{preview.decision}</span>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
        <label className="block">
          <span className="text-xs font-black uppercase tracking-wide text-muted">
            Principal
          </span>
          <input
            className="field mt-2"
            value={principal}
            onChange={(event) => onPrincipalChange(event.target.value)}
          />
        </label>
        <label className="block">
          <span className="text-xs font-black uppercase tracking-wide text-muted">
            Groups
          </span>
          <input
            className="field mt-2"
            value={groupsText}
            onChange={(event) => onGroupsTextChange(event.target.value)}
          />
          <span className="mt-1 block text-xs text-muted">
            Comma-separated group names without the group: prefix.
          </span>
        </label>
      </div>

      <section className="mt-4 rounded-card border border-border bg-white p-3">
        <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center">
          <div>
            <h4 className="text-sm font-black">Preview claims</h4>
            <p className="mt-1 text-xs text-muted">
              Add identity claims used by rule conditions.
            </p>
          </div>
          <button
            className="btn-secondary"
            type="button"
            onClick={() => onClaimsChange([...claims, { key: "", value: "" }])}
          >
            Add claim
          </button>
        </div>
        <div className="mt-3 grid gap-3">
          {claims.length === 0 ? (
            <div className="rounded-card border border-dashed border-border bg-soft p-4 text-sm text-muted">
              No preview claims
            </div>
          ) : (
            claims.map((claim, index) => (
              <div
                className="grid grid-cols-1 gap-3 rounded-card border border-border bg-soft p-3 md:grid-cols-[1fr_1fr_auto]"
                key={`${claim.key}-${index}`}
              >
                <label className="block">
                  <span className="text-xs font-black uppercase tracking-wide text-muted">
                    Claim
                  </span>
                  <input
                    className="field mt-2"
                    value={claim.key}
                    onChange={(event) =>
                      onClaimsChange(
                        replaceAt(claims, index, { ...claim, key: event.target.value }),
                      )
                    }
                  />
                </label>
                <label className="block">
                  <span className="text-xs font-black uppercase tracking-wide text-muted">
                    Value
                  </span>
                  <input
                    className="field mt-2"
                    value={claim.value}
                    onChange={(event) =>
                      onClaimsChange(
                        replaceAt(claims, index, { ...claim, value: event.target.value }),
                      )
                    }
                  />
                </label>
                <button
                  className="btn-secondary self-end"
                  type="button"
                  onClick={() =>
                    onClaimsChange(claims.filter((_, currentIndex) => currentIndex !== index))
                  }
                >
                  Remove
                </button>
              </div>
            ))
          )}
        </div>
      </section>

      <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-3">
        <PreviewCard label="Matched" value={preview.reason} />
        <PreviewCard label="Row filter" value={preview.rowFilter ?? "None"} />
        <PreviewCard
          label="Masks"
          value={
            preview.masks.length > 0
              ? preview.masks.map((mask) => `${mask.column}: ${mask.type}`).join(", ")
              : "None"
          }
        />
      </div>
      <div className="mt-3 rounded-card border border-border bg-white p-3">
        <span className="text-xs font-black uppercase tracking-wide text-muted">
          Visible columns
        </span>
        <div className="mt-2 flex flex-wrap gap-2">
          {preview.visibleColumns.length === 0 ? (
            <span className="text-sm text-muted">No columns visible</span>
          ) : (
            preview.visibleColumns.map((column) => (
              <span className="badge" key={column}>
                {column}
              </span>
            ))
          )}
        </div>
      </div>
    </section>
  );
}

function PreviewCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-card border border-border bg-white p-3">
      <span className="text-xs font-black text-muted">{label}</span>
      <strong className="mt-1 block break-words text-sm">{value}</strong>
    </div>
  );
}

function RuleCard({
  canRemove,
  onChange,
  onRemove,
  rule,
  ruleIndex,
  schemaColumns,
}: {
  canRemove: boolean;
  onChange: (patch: Partial<PolicyRuleForm>) => void;
  onRemove: () => void;
  rule: PolicyRuleForm;
  ruleIndex: number;
  schemaColumns: string[];
}) {
  return (
    <article className="rounded-card border border-border bg-white p-4">
      <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center">
        <div>
          <span className="text-xs font-black uppercase tracking-wide text-muted">
            Rule {ruleIndex + 1}
          </span>
          <h3 className="mt-1 text-base font-black">{rule.effect === "allow" ? "Allow" : "Deny"} access</h3>
        </div>
        <div className="flex flex-wrap gap-2">
          <select
            className="field min-w-[120px]"
            value={rule.effect}
            onChange={(event) => onChange({ effect: event.target.value as "allow" | "deny" })}
          >
            <option value="allow">Allow</option>
            <option value="deny">Deny</option>
          </select>
          {canRemove ? (
            <button className="btn-secondary" type="button" onClick={onRemove}>
              Remove
            </button>
          ) : null}
        </div>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-2">
        <TextListField
          help="Comma-separated users, groups, or service identities."
          label="Principals"
          value={rule.principalsText}
          onChange={(value) => onChange({ principalsText: value })}
        />
        {schemaColumns.length === 0 ? (
          <TextListField
            help="Use * for all columns, or comma-separate explicit columns."
            label="Visible columns"
            value={rule.columnsText}
            onChange={(value) => onChange({ columnsText: value })}
          />
        ) : (
          <ColumnSelector
            selections={rule.columnSelections}
            onChange={(columnSelections) => onChange({ columnSelections })}
          />
        )}
      </div>

      <label className="mt-4 block">
        <span className="text-xs font-black uppercase tracking-wide text-muted">
          Row filter SQL
        </span>
        <input
          className="field mt-2"
          placeholder="region = 'us'"
          value={rule.rowFilter}
          onChange={(event) => onChange({ rowFilter: event.target.value })}
        />
        <span className="mt-1 block text-xs text-muted">
          DuckDB SQL expression applied before masking.
        </span>
      </label>

      <EditablePairs
        addLabel="Add condition"
        emptyLabel="No match conditions"
        itemLabel="Condition"
        keyLabel="Claim or attribute"
        valueLabel="Expected value"
        rows={rule.conditions}
        onChange={(conditions) => onChange({ conditions })}
      />

      <MaskEditor
        schemaColumns={schemaColumns}
        masks={rule.masks}
        onChange={(masks) => onChange({ masks })}
      />
    </article>
  );
}

function TextListField({
  help,
  label,
  onChange,
  value,
}: {
  help: string;
  label: string;
  onChange: (value: string) => void;
  value: string;
}) {
  return (
    <label className="block">
      <span className="text-xs font-black uppercase tracking-wide text-muted">{label}</span>
      <input className="field mt-2" value={value} onChange={(event) => onChange(event.target.value)} />
      <span className="mt-1 block text-xs text-muted">{help}</span>
    </label>
  );
}

function EditablePairs({
  addLabel,
  emptyLabel,
  itemLabel,
  keyLabel,
  onChange,
  rows,
  valueLabel,
}: {
  addLabel: string;
  emptyLabel: string;
  itemLabel: string;
  keyLabel: string;
  onChange: (rows: ConditionRow[]) => void;
  rows: ConditionRow[];
  valueLabel: string;
}) {
  return (
    <section className="mt-5 rounded-card border border-border bg-soft p-3">
      <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center">
        <div>
          <h4 className="text-sm font-black">{itemLabel}s</h4>
          <p className="mt-1 text-xs text-muted">{emptyLabel} unless one is added.</p>
        </div>
        <button
          className="btn-secondary"
          type="button"
          onClick={() => onChange([...rows, { key: "", value: "", valueKind: "text" }])}
        >
          {addLabel}
        </button>
      </div>
      <div className="mt-3 grid gap-3">
        {rows.length === 0 ? (
          <div className="rounded-card border border-dashed border-border bg-white p-4 text-sm text-muted">
            {emptyLabel}
          </div>
        ) : (
          rows.map((row, index) => (
            <div
              className="grid grid-cols-1 gap-3 rounded-card border border-border bg-white p-3 md:grid-cols-[1fr_1fr_140px_auto]"
              key={`${row.key}-${index}`}
            >
              <label className="block">
                <span className="text-xs font-black uppercase tracking-wide text-muted">
                  {keyLabel}
                </span>
                <input
                  className="field mt-2"
                  value={row.key}
                  onChange={(event) =>
                    onChange(replaceAt(rows, index, { ...row, key: event.target.value }))
                  }
                />
              </label>
              <label className="block">
                <span className="text-xs font-black uppercase tracking-wide text-muted">
                  {valueLabel}
                </span>
                <input
                  className="field mt-2"
                  value={row.value}
                  onChange={(event) =>
                    onChange(replaceAt(rows, index, { ...row, value: event.target.value }))
                  }
                />
              </label>
              <label className="block">
                <span className="text-xs font-black uppercase tracking-wide text-muted">
                  Type
                </span>
                <select
                  className="field mt-2"
                  value={row.valueKind}
                  onChange={(event) =>
                    onChange(
                      replaceAt(rows, index, {
                        ...row,
                        valueKind: event.target.value as "text" | "list",
                      }),
                    )
                  }
                >
                  <option value="text">Text</option>
                  <option value="list">List</option>
                </select>
              </label>
              <button
                className="btn-secondary self-end"
                type="button"
                onClick={() => onChange(rows.filter((_, currentIndex) => currentIndex !== index))}
              >
                Remove
              </button>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function MaskEditor({
  masks,
  onChange,
  schemaColumns,
}: {
  masks: MaskRow[];
  onChange: (masks: MaskRow[]) => void;
  schemaColumns: string[];
}) {
  return (
    <section className="mt-5 rounded-card border border-border bg-soft p-3">
      <div className="flex flex-col justify-between gap-3 md:flex-row md:items-center">
        <div>
          <h4 className="text-sm font-black">Column masks</h4>
          <p className="mt-1 text-xs text-muted">Mask sensitive columns after row filters run.</p>
        </div>
        <button
          className="btn-secondary"
          type="button"
          onClick={() => onChange([...masks, { column: "", type: "email" }])}
        >
          Add mask
        </button>
      </div>
      <div className="mt-3 grid gap-3">
        {masks.length === 0 ? (
          <div className="rounded-card border border-dashed border-border bg-white p-4 text-sm text-muted">
            No column masks
          </div>
        ) : (
          masks.map((mask, index) => (
            <div
              className="grid grid-cols-1 gap-3 rounded-card border border-border bg-white p-3 md:grid-cols-[1fr_180px_auto]"
              key={`${mask.column}-${index}`}
            >
              <label className="block">
                <span className="text-xs font-black uppercase tracking-wide text-muted">
                  Column
                </span>
                {schemaColumns.length === 0 ? (
                  <input
                    className="field mt-2"
                    value={mask.column}
                    onChange={(event) =>
                      onChange(replaceAt(masks, index, { ...mask, column: event.target.value }))
                    }
                  />
                ) : (
                  <select
                    className="field mt-2"
                    value={mask.column}
                    onChange={(event) =>
                      onChange(replaceAt(masks, index, { ...mask, column: event.target.value }))
                    }
                  >
                    <option value="">Choose column</option>
                    {schemaColumns.map((column) => (
                      <option key={column} value={column}>
                        {column}
                      </option>
                    ))}
                  </select>
                )}
              </label>
              <label className="block">
                <span className="text-xs font-black uppercase tracking-wide text-muted">
                  Mask type
                </span>
                <select
                  className="field mt-2"
                  value={mask.type}
                  onChange={(event) =>
                    onChange(replaceAt(masks, index, { ...mask, type: event.target.value }))
                  }
                >
                  <option value="email">Email</option>
                  <option value="hash">Hash</option>
                  <option value="null">Null</option>
                  <option value="redact">Redact</option>
                </select>
              </label>
              <button
                className="btn-secondary self-end"
                type="button"
                onClick={() => onChange(masks.filter((_, currentIndex) => currentIndex !== index))}
              >
                Remove
              </button>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function ColumnSelector({
  onChange,
  selections,
}: {
  onChange: (selections: ColumnSelection[]) => void;
  selections: ColumnSelection[];
}) {
  return (
    <section>
      <span className="text-xs font-black uppercase tracking-wide text-muted">
        Visible columns
      </span>
      <div className="mt-2 grid max-h-48 gap-2 overflow-auto rounded-card border border-border bg-soft p-3">
        {selections.map((selection, index) => (
          <label
            className="flex items-center gap-2 rounded-card bg-white px-3 py-2 text-sm font-bold"
            key={selection.column}
          >
            <input
              checked={selection.selected}
              type="checkbox"
              onChange={(event) =>
                onChange(
                  replaceAt(selections, index, {
                    ...selection,
                    selected: event.target.checked,
                  }),
                )
              }
            />
            <span>{selection.column}</span>
          </label>
        ))}
      </div>
    </section>
  );
}

function SummaryCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-card border border-border bg-soft p-3">
      <span className="text-xs font-black text-muted">{label}</span>
      <strong className="mt-1 block truncate text-sm">{value}</strong>
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

function replaceAt<T>(items: T[], index: number, next: T): T[] {
  return items.map((item, currentIndex) => (currentIndex === index ? next : item));
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
