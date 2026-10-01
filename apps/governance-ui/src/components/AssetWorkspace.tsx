import { parseTestClaims } from "../policy_test";
import { discardChanges, useConfirmation } from "./ConfirmationProvider";
import { useEffect, useMemo, useRef, useState } from "react";
import { Button, Checkbox, Modal, NativeSelect, Tabs, Textarea, TextInput } from "@mantine/core";
import type { QueryClient } from "@tanstack/react-query";
import type { Asset, AssetAccess, AssetGrant, PolicyRule, Preview, Session } from "../api";
import { controlPlane } from "../api";
import { type AssetTab } from "../navigation";
import { recoveryMessage, displayErrorField } from "../recovery";
import { isAbortError } from "../async";
import { Icon } from "./Icon";
import { PolicyRuleEditor } from "./PolicyRuleEditor";
import { TokenInput } from "./TokenInput";
import { SchemaSidebar } from "./SchemaSidebar";

type SaveState = "saved" | "saving" | "unsaved" | "failed";
function saveLabel(state: SaveState) { return ({ saved: "Saved", saving: "Saving", unsaved: "Unsaved changes", failed: "Save failed" })[state]; }
function inventoryStatusLabel(asset: Asset): string { return asset.policy_status === "configured" ? "Policy configured" : "Default NULL policy"; }

export function AssetWorkspace(props: {
  initialTab?: AssetTab; onTabChange?: (tab: AssetTab) => void; asset: Asset; access?: AssetAccess;
  grants: AssetGrant[]; onBack: () => void; rules: PolicyRule[];
  activeRevision: number;
  saveState: SaveState; notice: string; fieldErrors?: Array<{ field: string; message: string; type: string }>;
  onUpdateRule: (change: (rule: PolicyRule) => PolicyRule, index: number) => void; onAddRule: () => void;
  onRemoveRule: (index: number) => void; onDuplicateRule: (index: number) => void;
  onDiscard: () => void; onAllowAll: () => void; onSave: (revokeExistingTokens: boolean) => void;
  onPreview: () => void; previewPrincipal: string; previewGroups: string; previewClaims: string;
  onPreviewPrincipal: (value: string) => void; onPreviewGroups: (value: string) => void;
  onPreviewClaims: (value: string) => void; preview: Preview | null; previewBusy?: boolean; previewError?: string; session: Session | null;
  onReloadAccess: () => void; onRevokeTokens?: () => void; revokingTokens?: boolean; onDirtyChange?: (dirty: boolean) => void;
  queryClient: QueryClient; sessionScope: string;
}) {
  const { confirm } = useConfirmation();
  const [tab, setTab] = useState<AssetTab>(props.initialTab ?? "policy");
  const validationSummary = useRef<HTMLDivElement>(null);
  const [testOpen, setTestOpen] = useState(false);
  const [schemaOpen, setSchemaOpen] = useState(false);
  const schemaTrigger = useRef<HTMLButtonElement>(null);
  const [pending, setPending] = useState<Record<number, boolean>>({});
  const [expanded, setExpanded] = useState<Set<number>>(() => new Set(props.rules.slice(0, 1).map((rule) => rule.ordinal)));
  const [generation, setGeneration] = useState(0);
  const [accessDirty, setAccessDirty] = useState(false);
  const [revoke, setRevoke] = useState(false);
  const pendingRef = useRef(pending);
  const lastOrdinals = useRef(new Set(props.rules.map((rule) => rule.ordinal)));
  const focusAddedRule = useRef(false);
  const addRule = () => { focusAddedRule.current = true; props.onAddRule(); };
  const readOnly = !props.access?.capabilities.some((item) => item.capability === "edit" && item.allowed);
  const dirty = Object.values(pending).some(Boolean);
  const bypass = props.rules.some((rule) => rule.effect === "allow_all");
  useEffect(() => {
    const added = props.rules.filter((rule) => !lastOrdinals.current.has(rule.ordinal));
    if (added.length) {
      setExpanded((current) => new Set([...current, ...added.map((rule) => rule.ordinal)]));
      if (focusAddedRule.current) requestAnimationFrame(() => {
        const name = document.querySelector<HTMLInputElement>(`#rule-body-${added[0].ordinal} input`);
        name?.scrollIntoView({ block: "center", behavior: "instant" });
        name?.focus({ preventScroll: true });
      });
      focusAddedRule.current = false;
    }
    lastOrdinals.current = new Set(props.rules.map((rule) => rule.ordinal));
  }, [props.rules]);
  useEffect(() => { setPending({}); pendingRef.current = {}; setGeneration((value) => value + 1); setRevoke(false); setExpanded(new Set(props.rules.slice(0, 1).map((rule) => rule.ordinal))); }, [props.asset.id]);
  useEffect(() => {
    setTab(props.initialTab ?? "policy");
    pendingRef.current = {}; setPending({}); setGeneration((value) => value + 1);
    setAccessDirty(false); props.onDirtyChange?.(false);
  }, [props.initialTab, props.asset.id]);
  useEffect(() => {
    if (!props.fieldErrors?.length) return;
    setExpanded((current) => { const next = new Set(current); for (const error of props.fieldErrors ?? []) { const match = /^rules\.(\d+)/.exec(error.field); const rule = match && props.rules[Number(match[1])]; if (rule) next.add(rule.ordinal); } return next; });
    validationSummary.current?.focus();
  }, [props.fieldErrors]);
  function report(ordinal: number, value: boolean) {
    pendingRef.current = { ...pendingRef.current, [ordinal]: value };
    setPending(pendingRef.current); props.onDirtyChange?.(Object.values(pendingRef.current).some(Boolean) || accessDirty);
  }
  function discard() {
    props.onDiscard(); pendingRef.current = {}; setPending({}); setGeneration((value) => value + 1); setRevoke(false); props.onDirtyChange?.(accessDirty);
  }
  async function selectTab(next: AssetTab) {
    if (next === tab) return;
    if ((dirty || accessDirty) && !await confirm(discardChanges("form", "switch tabs"))) return;
    pendingRef.current = {}; setPending({}); setGeneration((value) => value + 1); setAccessDirty(false); props.onDirtyChange?.(false);
    setTab(next); const params = new URLSearchParams(window.location.search); if (next === "policy") params.delete("tab"); else params.set("tab", next);
    window.history.pushState(null, "", `${window.location.pathname}${params.size ? `?${params}` : ""}#assets`);
    props.onTabChange?.(next);
  }
  return <>
    <Button variant="subtle" onClick={props.onBack}>Back to assets</Button>
    <div className="asset-summary"><div className="asset-context-tags"><span className={`policy-status-tag ${props.asset.policy_status === "configured" ? "configured" : "default"}`}><Icon name="shield-check" size={14} />{inventoryStatusLabel(props.asset)}</span><span className="context-tag">Live revision {props.activeRevision}</span><span className="context-tag">{props.asset.backend}</span></div><span className="save-status"><span className={`save-dot ${props.saveState}`} />{saveLabel(props.saveState)}</span></div>
    {props.notice && <div className="notice" role="status">{props.notice}</div>}
    {Boolean(props.fieldErrors?.length) && <div ref={validationSummary} tabIndex={-1} className="validation-summary" role="alert">{props.fieldErrors!.map((item, index) => <p key={index}>{displayErrorField(item.field)}: {item.message}</p>)}</div>}
    <Tabs value={tab} onChange={(value) => { if (value) selectTab(value as AssetTab); }}><div className="policy-view-tabs"><Tabs.List aria-label="Asset views">{(["policy", "access", "consumers"] as const).map((item) => <Tabs.Tab value={item} key={item}>{item[0].toUpperCase() + item.slice(1)}</Tabs.Tab>)}</Tabs.List><div className="policy-view-actions">{tab === "policy" && <Button ref={schemaTrigger} variant={schemaOpen ? "light" : "default"} aria-expanded={schemaOpen} aria-controls="asset-schema-sidebar" onClick={() => setSchemaOpen(!schemaOpen)} leftSection={<Icon name="database" size={15} />}>{schemaOpen ? "Hide schema" : "Show schema"}</Button>}<Button variant="default" onClick={() => setTestOpen(true)} leftSection={<Icon name="play" size={15} />}>Test Policy</Button></div></div>
    <Tabs.Panel value={tab}>{tab === "policy" ? <div className={`policy-layout ${schemaOpen ? "with-schema" : ""}`}><div className="policy-authoring">
      <div className={`policy-baseline ${bypass ? "bypassed" : ""}`}><Icon name="shield-check" size={20} /><div><strong>{bypass ? "Allow all is enabled" : "Every column starts with a NULL mask"}</strong><p>{bypass ? "All authenticated readers receive every column and row, without masks. Other rules are bypassed." : "Rules reveal selected columns with no mask, or replace the default with another mask. Row filters are independent."}</p></div><Button variant="default" disabled={readOnly || bypass || dirty} onClick={props.onAllowAll}>Allow all to all users</Button></div>
      <div className="policy-list-heading"><h2>Rules <span className="context-tag">{props.rules.length}</span></h2><Button disabled={readOnly} onClick={addRule} leftSection={<Icon name="plus" size={15} />}>New rule</Button></div>
      <div className="stacked-rules">{props.rules.map((rule, index) => {
        const open = expanded.has(rule.ordinal); const title = rule.name?.trim() || `Rule ${index + 1}`;
        return <article className="collapsible-rule" key={`${props.asset.id}:${generation}:${rule.ordinal}`}>
          <button className="rule-disclosure" type="button" aria-expanded={open} aria-controls={`rule-body-${rule.ordinal}`} onClick={() => setExpanded((current) => { const next = new Set(current); if (next.has(rule.ordinal)) next.delete(rule.ordinal); else next.add(rule.ordinal); return next; })}><Icon name="chevron-down" size={18} /><span><strong>{title}</strong><span className="rule-summary-tags">{rule.effect === "allow_all" ? <span className="context-tag">Global bypass</span> : <><span className="context-tag">{rule.columns.length} columns</span>{rule.principals.length ? rule.principals.map((principal) => <span className="audience-tag" key={principal}>{principal === "*" ? "All authenticated users" : principal}</span>) : <span className="context-tag">No audience</span>}{Boolean(Object.keys(rule.masks).length) && <span className="context-tag">{Object.keys(rule.masks).length} masks</span>}{rule.row_filter && <span className="context-tag">Filtered rows</span>}</>}</span></span>{pending[rule.ordinal] && <span className="local-edit-label">Unapplied edits</span>}</button>
          <div id={`rule-body-${rule.ordinal}`} className="rule-body" hidden={!open}>
            <div className="rule-metadata"><TextInput label="Rule name" maxLength={160} value={rule.name ?? ""} disabled={readOnly} onChange={(event) => { const name = event.currentTarget.value; props.onUpdateRule((current) => ({ ...current, name }), index); }} /><Textarea label="Description" maxLength={2000} autosize value={rule.description ?? ""} disabled={readOnly} placeholder="Explain who needs this access and why" onChange={(event) => { const description = event.currentTarget.value; props.onUpdateRule((current) => ({ ...current, description }), index); }} /></div>
            {rule.effect === "allow_all" ? <p className="policy-bypass-description">This rule short-circuits all column masks and row filters. Delete it to restore the other rules and the default NULL masks.</p> : <>
              <TokenInput label="Applies to people or groups" value={rule.principals} onChange={(principals) => props.onUpdateRule((current) => ({ ...current, principals }), index)} placeholder="group:analysts or * for all authenticated users" disabled={readOnly} />
              <details className="audience-conditions"><summary>Identity claim conditions</summary><ConditionBuilder value={rule.when} disabled={readOnly} onChange={(when) => props.onUpdateRule((current) => ({ ...current, when }), index)} /></details>
              <PolicyRuleEditor assetId={props.asset.id} ruleIndex={index} rule={rule} fields={props.asset.schema?.fields} supportedMasks={props.asset.schema?.supported_masks ?? []} readOnly={readOnly || bypass} onUpdateRule={(change) => props.onUpdateRule(change, index)} onDirtyChange={(value) => report(rule.ordinal, value)} />
            </>}
            <div className="rule-footer"><Button variant="subtle" disabled={readOnly || dirty || rule.effect === "allow_all"} onClick={() => props.onDuplicateRule(index)} leftSection={<Icon name="copy" size={14} />}>Duplicate rule</Button><Button variant="subtle" color="red" disabled={readOnly || props.saveState === "saving"} onClick={() => { report(rule.ordinal, false); props.onRemoveRule(index); }} leftSection={<Icon name="trash" size={14} />}>Delete rule</Button></div>
          </div>
        </article>;
      })}</div>
      {!props.rules.length && <div className="policy-empty"><p>No overrides. Every column returns NULL for authenticated readers.</p></div>}
      <div className="policy-save-bar"><Button variant="default" onClick={() => setTestOpen(true)} leftSection={<Icon name="play" size={15} />}>Test saved policy</Button><Button variant="default" disabled={readOnly || props.saveState === "saving" || (props.saveState === "saved" && !dirty)} onClick={discard}>Discard all changes</Button>{props.access?.can_revoke_tokens && <Checkbox label="Revoke existing tokens after saving" checked={revoke} onChange={(event) => setRevoke(event.currentTarget.checked)} />}{dirty && <p className="pending-edit-note">Apply or cancel open mask and row-filter forms before saving.</p>}<Button disabled={readOnly || dirty || props.saveState === "saving"} onClick={() => props.onSave(revoke)}>{props.saveState === "saving" ? "Saving…" : "Save policy"}</Button></div>
      <details className="policy-conflicts"><summary>How overlapping rules combine</summary><p>Order does not change the result. A matching grant overrides the default NULL mask. Between explicit masks, NULL wins and keep-last uses the smaller count; incompatible masks reject the read. Row filters combine with AND. Allow all bypasses everything.</p></details>
    </div>{schemaOpen && <SchemaSidebar schema={props.asset.schema} onClose={() => { setSchemaOpen(false); schemaTrigger.current?.focus(); }} />}</div> : tab === "access" ? <AccessView asset={props.asset} access={props.access} grants={props.grants} session={props.session} onReload={props.onReloadAccess} onDirtyChange={(value) => { setAccessDirty(value); props.onDirtyChange?.(value || dirty); }} queryClient={props.queryClient} sessionScope={props.sessionScope} revokingTokens={props.revokingTokens} onRevokeTokens={props.onRevokeTokens ?? (() => undefined)} /> : <ConsumerView asset={props.asset} />}</Tabs.Panel></Tabs>
    <Modal opened={testOpen} onClose={() => setTestOpen(false)} title="Test policy" size="lg" keepMounted closeButtonProps={{ "aria-label": "Close policy test" }}><TestsView busy={props.previewBusy} error={props.previewError} unsaved={props.saveState !== "saved" || dirty} onPreview={props.onPreview} preview={props.preview} principal={props.previewPrincipal} groups={props.previewGroups} claims={props.previewClaims} onPrincipal={props.onPreviewPrincipal} onGroups={props.onPreviewGroups} onClaims={props.onPreviewClaims} />{props.preview && <div className="policy-test-result"><h3>Effective values</h3><ul>{props.preview.allowed_columns.map((column) => <li key={column}><code>{column}</code><span>{props.preview!.masks[column]?.type ?? "No mask"}</span></li>)}</ul><h3>Combined row filter</h3><code>{props.preview.row_filter ?? "All rows"}</code></div>}</Modal>
  </>;
}

function TestsView({ unsaved, busy, error, onPreview, preview, principal, groups, claims, onPrincipal, onGroups, onClaims }: {
  unsaved: boolean; busy?: boolean; error?: string; onPreview: () => void; preview: Preview | null;
  principal: string; groups: string; claims: string;
  onPrincipal: (value: string) => void; onGroups: (value: string) => void; onClaims: (value: string) => void;
}) {
  const parsed = parseTestClaims(claims);
  const principalError = principal.trim() ? undefined : "Enter a principal to test.";
  return <section className="tests-view">
    <p>Evaluate the saved policy with a sample identity. {unsaved && "Unsaved edits are not included."}</p>
    <div className="test-card persona-form">
      <TextInput label="Principal" value={principal} error={principalError} onChange={(event) => onPrincipal(event.currentTarget.value)} placeholder="user:analyst@example.com" />
      <TextInput label="Groups" value={groups} onChange={(event) => onGroups(event.currentTarget.value)} description="Separate groups with commas." placeholder="us-analysts, finance" />
      <Textarea label="Claims (JSON)" value={claims} error={parsed.error} onChange={(event) => onClaims(event.currentTarget.value)} aria-label="Synthetic persona claims" placeholder='{"region":"us"}' />
      <Button type="button" loading={busy} disabled={busy || Boolean(parsed.error || principalError)} onClick={onPreview} leftSection={<Icon name="play" size={15} />}>{busy ? "Testing…" : "Test saved policy"}</Button>
    </div>
    {busy && <p role="status">Evaluating saved policy…</p>}
    {error && <p className="field-error" role="alert">{error}</p>}
    {preview && <div role="status" className={"result-state " + (preview.decision === "deny" ? "denied" : "allowed")}>Current test: {preview.decision === "deny" ? "denied" : preview.allowed_columns.length + " fields evaluated"}</div>}
  </section>;
}
function ConditionBuilder({ value, disabled, onChange }: { value?: Record<string, string | string[]>; disabled: boolean; onChange: (value: Record<string, string | string[]>) => void }) {
  const [rows, setRows] = useState<Array<{ key: string; mode: "equals" | "one_of"; value: string }>>(() => conditionRows(value));
  const [error, setError] = useState("");
  useEffect(() => {
    const nextRows = conditionRows(value);
    setRows(nextRows);
    if (nextRows.some((row) => !row.key.trim())) setError("Every condition needs a claim name.");
    else if (nextRows.some((row) => !row.value.split(",").map((item) => item.trim()).filter(Boolean).length)) setError("Every condition needs a value.");
    else setError("");
  }, [value]);
  function commit(next: Array<{ key: string; mode: "equals" | "one_of"; value: string }>) {
    const result: Record<string, string | string[]> = {};
    let errorMessage = "";
    for (const row of next) {
      const key = row.key.trim();
      const values = row.value.split(",").map((item) => item.trim()).filter(Boolean);
      if (!key) errorMessage = "Every condition needs a claim name.";
      else if (!values.length && !errorMessage) errorMessage = "Every condition needs a value.";
      // Preserve incomplete rows while editing so they cannot disappear before
      // the control plane rejects them with a field-level validation error.
      result[key] = row.mode === "one_of" ? values : values[0] ?? "";
    }
    setError(errorMessage); onChange(result);
  }
  function update(index: number, change: Partial<{ key: string; mode: "equals" | "one_of"; value: string }>) {
    const next = rows.map((row, rowIndex) => rowIndex === index ? { ...row, ...change } : row);
    setRows(next); commit(next);
  }
  return <div className="condition-builder"><p className="help">Match authenticated claims with AND semantics. Values are text or a comma-separated list.</p>{rows.map((row, index) => <div className="condition-row" key={index}><TextInput aria-label={`Condition claim ${index + 1}`} value={row.key} onChange={(event) => update(index, { key: event.currentTarget.value })} placeholder="region" disabled={disabled} /><NativeSelect aria-label={`Condition operator ${index + 1}`} value={row.mode} onChange={(event) => update(index, { mode: event.currentTarget.value as "equals" | "one_of" })} disabled={disabled} data={[{ value: "equals", label: "equals" }, { value: "one_of", label: "is one of" }]} /><TextInput aria-label={`Condition value ${index + 1}`} value={row.value} onChange={(event) => update(index, { value: event.currentTarget.value })} placeholder={row.mode === "one_of" ? "us, eu" : "us"} disabled={disabled} /><Button className="danger compact" variant="default" size="sm" type="button" onClick={() => { const next = rows.filter((_item, rowIndex) => rowIndex !== index); setRows(next); commit(next); }} disabled={disabled} leftSection={<Icon name="trash" size={14} />}>Remove</Button></div>)}<Button className="secondary compact" variant="default" size="sm" type="button" onClick={() => { const next = [...rows, { key: "", mode: "equals" as const, value: "" }]; setRows(next); commit(next); }} disabled={disabled} leftSection={<Icon name="plus" size={14} />}>Add condition</Button>{error && <p className="auth-error" role="alert">{error}</p>}</div>;
}
function conditionRows(value?: Record<string, string | string[]>): Array<{ key: string; mode: "equals" | "one_of"; value: string }> { return Object.entries(value ?? {}).map(([key, raw]) => ({ key, mode: Array.isArray(raw) ? "one_of" : "equals", value: Array.isArray(raw) ? raw.join(", ") : raw })); }
function ConsumerView({ asset }: { asset: Asset }) {
  const catalog = JSON.stringify(asset.catalog);
  const target = JSON.stringify(asset.name);
  const python = `import os\nfrom dal_obscura.connectors import DalObscuraClient\n\nwith DalObscuraClient(os.environ["DAL_OBSCURA_FLIGHT_URI"],\n                     auth_token=lambda: os.environ["DAL_OBSCURA_TOKEN"]) as client:\n    table = client.read_table(\n        catalog=${catalog},\n        target=${target},\n        columns=["*"],\n    )`;
  const duckdb = `import os\nfrom dal_obscura.connectors import DalObscuraClient, DuckDBDalObscuraReader\n\nwith DalObscuraClient(os.environ["DAL_OBSCURA_FLIGHT_URI"],\n                     auth_token=lambda: os.environ["DAL_OBSCURA_TOKEN"]) as client:\n    with DuckDBDalObscuraReader(client) as reader:\n        relation = reader.relation(\n            catalog=${catalog},\n            target=${target},\n            columns=["*"],\n        )\n        relation.show()`;
  const spark = `import os\n\nspark.read.format("dal_obscura") \\\n    .option("dal.uri", os.environ["DAL_OBSCURA_FLIGHT_URI"]) \\\n    .option("dal.catalog", ${catalog}) \\\n    .option("dal.target", ${target}) \\\n    .option("dal.auth.token-env", "DAL_OBSCURA_TOKEN") \\\n    .option("dal.executor.auth.token-env", "DAL_OBSCURA_TOKEN") \\\n    .load()`;
  const arrow = `import os\nfrom dal_obscura.connectors import DalObscuraClient\n\nwith DalObscuraClient(os.environ["DAL_OBSCURA_FLIGHT_URI"],\n                     auth_token=lambda: os.environ["DAL_OBSCURA_TOKEN"]) as client:\n    for batch in client.read_batches(\n        catalog=${catalog}, target=${target}, columns=["*"]\n    ):\n        consume(batch)`;
  return <section className="consumer-view"><div className="management-head"><div><span className="eyebrow">CONSUMERS</span><h2>Read this governed asset</h2><p className="muted">Use the same Arrow Flight contract from Python, DuckDB, Spark, or another Arrow consumer. Credentials stay in environment or workload identity configuration.</p></div></div><div className="consumer-notice"><strong>Use a TLS Flight URI in production.</strong><span>Set <code>DAL_OBSCURA_FLIGHT_URI</code> and <code>DAL_OBSCURA_TOKEN</code> outside source control. The snippets request the governed logical target and receive only authorized nested fields and masks.</span></div><div className="consumer-grid"><CopyableCode title="Python / PyArrow" code={python} /><CopyableCode title="DuckDB relation" code={duckdb} /><CopyableCode title="Spark 3.x" code={spark} /><CopyableCode title="Raw Arrow batches" code={arrow} /></div></section>;
}

function CopyableCode({ title, code }: { title: string; code: string }) {
  const [copied, setCopied] = useState(false);
  const [copyError, setCopyError] = useState(false);
  async function copy() {
    try {
      await navigator.clipboard.writeText(code);
      setCopyError(false);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1_500);
    } catch {
      setCopied(false);
      setCopyError(true);
    }
  }
  return <article className="consumer-card"><div className="consumer-card-head"><h3>{title}</h3><Button type="button" variant="default" size="sm" className="secondary compact" onClick={() => void copy()} leftSection={<Icon name={copied ? "check" : "copy"} size={14} />}>{copied ? "Copied" : copyError ? "Retry copy" : "Copy"}</Button></div><pre><code>{code}</code></pre>{copyError && <p className="help" role="status">Clipboard access is unavailable. Select the code above or retry copying.</p>}</article>;
}

function AccessView({ asset, access, grants, session, onReload, onDirtyChange, onRevokeTokens, revokingTokens, queryClient, sessionScope }: { asset: Asset; access?: AssetAccess; grants: AssetGrant[]; session: Session | null; onReload: () => void; onDirtyChange?: (dirty: boolean) => void; onRevokeTokens?: () => void; revokingTokens?: boolean; queryClient: QueryClient; sessionScope: string }) {
  const { confirm } = useConfirmation();
  const [owners, setOwners] = useState(asset.owners.join(", "));
  const [rows, setRows] = useState<AssetGrant[]>(grants);
  const [message, setMessage] = useState("");
  const [ownersDirty, setOwnersDirty] = useState(false);
  const [grantsDirty, setGrantsDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const ownersEditEpoch = useRef(0);
  const grantsEditEpoch = useRef(0);
  const savingRef = useRef(false);
  const mutationControllers = useRef<Set<AbortController>>(new Set());
  useEffect(() => {
    if (ownersDirty) return;
    setOwners(asset.owners.join(", "));
    setOwnersDirty(false);
    ownersEditEpoch.current += 1;
  }, [asset, ownersDirty]);
  useEffect(() => {
    if (grantsDirty) return;
    setRows(grants);
    setGrantsDirty(false);
    grantsEditEpoch.current += 1;
  }, [grants, grantsDirty]);
  const dirty = ownersDirty || grantsDirty;
  useEffect(() => { onDirtyChange?.(dirty); }, [dirty, onDirtyChange]);
  useEffect(() => () => {
    for (const controller of mutationControllers.current) controller.abort();
    mutationControllers.current.clear();
  }, [sessionScope]);
  const beginMutation = (): AbortController => {
    const controller = new AbortController();
    mutationControllers.current.add(controller);
    return controller;
  };
  const finishMutation = (controller: AbortController): void => {
    mutationControllers.current.delete(controller);
  };
  const markDirty = (scope: "owners" | "grants"): void => {
    if (scope === "owners") {
      ownersEditEpoch.current += 1;
      setOwnersDirty(true);
    } else {
      grantsEditEpoch.current += 1;
      setGrantsDirty(true);
    }
  };
  const canManageOwners = Boolean(session?.platform_admin);
  const canManageGrants = Boolean(session?.platform_admin || access?.capabilities.some((item) => item.capability === "grant" && item.allowed));
  const canRevokeTokens = access?.can_revoke_tokens === true;
  async function saveOwners() {
    if (!canManageOwners) return setMessage("Only a platform administrator can change owners.");
    if (savingRef.current) return;
    const normalized = owners.split(",").map((value) => value.trim()).filter(Boolean);
    if (!normalized.length) {
      setMessage("At least one owner is required. Assign a replacement before removing the last owner.");
      return;
    }
    savingRef.current = true;
    setSaving(true);
    const operationEpoch = ownersEditEpoch.current;
    const controller = beginMutation();
    try {
      await controlPlane.saveOwners(asset.id, normalized, asset.revision, controller.signal);
      if (controller.signal.aborted || operationEpoch !== ownersEditEpoch.current) return;
      setOwnersDirty(false); void queryClient.invalidateQueries({ queryKey: ["asset", sessionScope, asset.id] }); void queryClient.invalidateQueries({ queryKey: ["asset-inventory", sessionScope] }); setMessage("Owners updated."); onReload();
    } catch (error) {
      if (!isAbortError(error)) setMessage(recoveryMessage(error, "Owner update was rejected; refresh before retrying."));
    } finally { finishMutation(controller); savingRef.current = false; setSaving(false); }
  }
  async function saveGrants() {
    if (!canManageGrants) return setMessage("Only an actor with grant-management capability can change delegated access.");
    if (savingRef.current) return;
    savingRef.current = true;
    setSaving(true);
    const normalized = rows.filter((grant) => grant.principal.trim()).map((grant) => ({ ...grant, principal: grant.principal.trim() }));
    const operationEpoch = grantsEditEpoch.current;
    const controller = beginMutation();
    try {
      await controlPlane.saveGrants(asset.id, normalized, asset.revision, controller.signal);
      if (controller.signal.aborted || operationEpoch !== grantsEditEpoch.current) return;
      setGrantsDirty(false); void queryClient.invalidateQueries({ queryKey: ["asset", sessionScope, asset.id] }); setRows(normalized); setMessage("Delegated capabilities updated. Changes take effect on the next authorized request."); onReload();
    } catch (error) {
      if (!isAbortError(error)) setMessage(recoveryMessage(error, "Capability update was rejected; refresh before retrying."));
    } finally { finishMutation(controller); savingRef.current = false; setSaving(false); }
  }
  async function reload() {
    if (dirty && !await confirm(discardChanges("access", "reload access"))) return;
    ownersEditEpoch.current += 1;
    grantsEditEpoch.current += 1;
    setOwnersDirty(false);
    setGrantsDirty(false);
    onReload();
  }
  const identityHint = session?.issuer ? `Federated identities use the exact issuer ${session.issuer}|subject and ${session.issuer}|group:name.` : "Federated identities should use the exact issuer|subject form so identical subjects from different providers stay isolated.";
  return <section className="management-view access-view"><div className="management-head"><div><span className="eyebrow">ACCESS</span><h2>Owners and delegated capabilities</h2><p className="muted">Owners can edit the live policy and revoke active tokens. Delegated access remains enforced by the control plane.</p></div><Button type="button" variant="default" className="secondary" onClick={reload} leftSection={<Icon name="refresh-cw" size={16} />}>Refresh</Button></div>{access && <div className="form-card"><h3>Your effective capabilities</h3><p className="help">Calculated by the control plane for <strong>{access.principal}</strong>. A denied capability remains unavailable even when a control is visible.</p><div className="capability-grid">{access.capabilities.map((item) => <div className={item.allowed ? "capability-card allowed" : "capability-card denied"} key={item.capability}><strong>{item.capability}</strong><span>{item.allowed ? "Allowed" : "Not granted"}</span><small>{item.reasons.length ? item.reasons.join(" · ") : "No matching owner or delegated grant"}</small></div>)}</div></div>}<div className="form-card"><h3>Owners</h3><TextInput className="form-label" label="Owner principals" value={owners} onChange={(event) => { markDirty("owners"); setOwners(event.currentTarget.value); }} placeholder="user:owner@example.com, group:data-stewards" disabled={!canManageOwners} /><p className="help">Comma-separated user or group principals. Removing the last owner is blocked while the asset is not safely reassigned. {identityHint}</p><Button type="button" className="primary" onClick={() => void saveOwners()} disabled={!canManageOwners || saving} leftSection={<Icon name="save" size={16} />}>{saving ? "Saving…" : "Save owners"}</Button></div>{canRevokeTokens && <div className="form-card"><h3>Active tokens</h3><p className="help">Policy edits do not revoke tokens. Revoke every unexpired token for this asset when you need changes to take effect for existing readers.</p><Button type="button" className="danger" variant="default" loading={revokingTokens} disabled={revokingTokens} onClick={onRevokeTokens} leftSection={<Icon name="key-round" size={16} />}>Revoke all active tokens</Button></div>}<div className="form-card"><h3>Delegated capabilities</h3>{rows.length ? <div className="grant-editor">{rows.map((grant, index) => <div className="grant-row" key={index}><TextInput aria-label={`Grant principal ${index + 1}`} value={grant.principal} disabled={!canManageGrants} onChange={(event) => { const value = event.currentTarget.value; markDirty("grants"); setRows((current) => current.map((item, row) => row === index ? { ...item, principal: value } : item)); }} placeholder="user:analyst@example.com" /><NativeSelect aria-label={`Grant capability ${index + 1}`} value={grant.capability} disabled={!canManageGrants} onChange={(event) => { const value = event.currentTarget.value as AssetGrant["capability"]; markDirty("grants"); setRows((current) => current.map((item, row) => row === index ? { ...item, capability: value } : item)); }} data={[{ value: "read", label: "Read" }, { value: "edit", label: "Edit" }, { value: "grant", label: "Grant management" }]} /><Button type="button" className="danger" variant="default" disabled={!canManageGrants || saving} onClick={() => { markDirty("grants"); setRows((current) => current.filter((_, row) => row !== index)); }} leftSection={<Icon name="trash" size={15} />}>Remove</Button></div>)}</div> : <p className="muted">No explicit delegated capabilities. Grant management is reserved for platform administrators or explicitly delegated grant managers.</p>}<div className="editor-actions"><Button type="button" className="secondary" variant="default" disabled={!canManageGrants || saving} onClick={() => { markDirty("grants"); setRows((current) => [...current, { principal: "", capability: "read" }]); }} leftSection={<Icon name="plus" size={16} />}>Add capability</Button><Button type="button" className="primary" disabled={!canManageGrants || saving} onClick={() => void saveGrants()} leftSection={<Icon name="save" size={16} />}>{saving ? "Saving…" : "Save capabilities"}</Button></div>{message && <p className="notice" role="status">{message}</p>}</div></section>;
}
