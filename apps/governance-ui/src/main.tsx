import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { createRoot } from "react-dom/client";
import type { Asset, AuditEvent, AuthProvider, Catalog, Mask, PolicyRule, PolicyVersion, Preview, RuntimeSettings, SchemaNode, Session, UiAuthConfig, WorkspaceSummary } from "./api";
import { controlPlane } from "./api";
import { demoAsset, demoRules } from "./fixtures";
import "./styles.css";

type Page = "assets" | "changes" | "activity" | "connections" | "settings";
type SaveState = "saved" | "saving" | "unsaved" | "failed";
type WorkspaceState = "loading" | "ready" | "demo" | "unavailable";
type ManagementData = { history?: PolicyVersion[]; events?: AuditEvent[]; catalogs?: Catalog[]; tables?: Array<Record<string, unknown>>; runtime?: RuntimeSettings | null; providers?: AuthProvider[]; summary?: WorkspaceSummary };

const maskOptions: Array<{ type: Mask["type"]; label: string; needsValue?: boolean }> = [
  { type: "null", label: "Null" }, { type: "redact", label: "Redact", needsValue: true },
  { type: "hash", label: "Hash" }, { type: "email", label: "Email" },
  { type: "keep_last", label: "Keep last", needsValue: true }, { type: "default", label: "Default", needsValue: true },
];
const newRule = (field: string, ordinal: number): PolicyRule => ({ ordinal, effect: "allow", principals: [], columns: field ? [field] : [], masks: {}, row_filter: null });

function App() {
  const [page, setPage] = useState<Page>("assets");
  const [workspace, setWorkspace] = useState<WorkspaceState>("loading");
  const [assets, setAssets] = useState<Asset[]>([]);
  const [asset, setAsset] = useState<Asset | null>(null);
  const [rules, setRules] = useState<PolicyRule[]>([]);
  const [draftRevision, setDraftRevision] = useState(0);
  const [selectedRule, setSelectedRule] = useState(0);
  const [selectedField, setSelectedField] = useState("");
  const [saveState, setSaveState] = useState<SaveState>("saved");
  const [preview, setPreview] = useState<Preview | null>(null);
  const [notice, setNotice] = useState("Loading workspace…");
  const [session, setSession] = useState<Session | null>(null);
  const [authConfig, setAuthConfig] = useState<UiAuthConfig | null>(null);
  const [loggingIn, setLoggingIn] = useState(false);
  const [managementData, setManagementData] = useState<ManagementData>({});
  const [managementLoading, setManagementLoading] = useState(false);
  const loadEpoch = useRef(0);
  const managementEpoch = useRef(0);
  const [logoutPending, setLogoutPending] = useState(false);
  const isDemo = workspace === "demo";

  useEffect(() => {
    if (new URLSearchParams(window.location.search).has("demo")) {
      setAssets([demoAsset]); setAsset(demoAsset); setRules(demoRules); setSelectedField(demoAsset.schema_fields[0].name);
      setWorkspace("demo"); setNotice("Demo workspace. It never writes to the control plane.");
      return;
    }
    void loadInitialWorkspace();
  }, []);

  useEffect(() => {
    if (page === "assets" || !session || isDemo) return;
    void loadManagement(page);
  }, [page, session, isDemo]);

  async function loadManagement(destination: Page) {
    const epoch = ++managementEpoch.current;
    setManagementLoading(true);
    try {
      let next: ManagementData = {};
      if (destination === "changes") next = { history: await controlPlane.listHistory() };
      if (destination === "activity") next = { history: await controlPlane.listHistory(), events: await controlPlane.listAuditEvents(), summary: await controlPlane.getSummary() };
      if (destination === "connections") next = { catalogs: await controlPlane.listCatalogs() };
      if (destination === "settings") next = { runtime: await controlPlane.getRuntimeSettings(), providers: await controlPlane.getAuthProviders() };
      if (epoch !== managementEpoch.current) return;
      setManagementData(next);
    } catch {
      if (epoch !== managementEpoch.current) return;
      setManagementData({});
      setNotice("This management view is unavailable for your current session or workspace.");
    } finally {
      if (epoch === managementEpoch.current) setManagementLoading(false);
    }
  }

  async function loadInitialWorkspace() {
    const epoch = ++loadEpoch.current;
    setWorkspace("loading");
    try {
      const loadedSession = await controlPlane.getSession();
      if (epoch !== loadEpoch.current) return;
      setSession(loadedSession);
      const loaded = await controlPlane.listAssets();
      if (epoch !== loadEpoch.current) return;
      setAssets(loaded);
      if (!loaded.length) {
        setWorkspace("ready"); setNotice("No governed assets are available in this workspace.");
        return;
      }
      await loadAsset(loaded[0].id, loaded);
      if (epoch !== loadEpoch.current) return;
      setWorkspace("ready");
    } catch {
      if (epoch !== loadEpoch.current) return;
      setWorkspace("unavailable");
      setSession(null);
      setAuthConfig(await controlPlane.getUiAuthConfig().catch(() => null));
      setNotice("Workspace unavailable. Sign in or reconnect to the control plane; no demo data is shown automatically.");
    }
  }

  async function demoLogin(loginHint: string) {
    setLoggingIn(true);
    try {
      await controlPlane.demoLogin(loginHint);
      loadEpoch.current += 1;
      await loadInitialWorkspace();
    } catch {
      setWorkspace("unavailable");
      setNotice("Sign-in failed. No policy data was loaded.");
    } finally {
      setLoggingIn(false);
    }
  }

  async function logout() {
    loadEpoch.current += 1;
    managementEpoch.current += 1;
    try {
      await controlPlane.logout();
      clearPrivateState();
      setLogoutPending(false);
      setNotice("Signed out. No policy data remains loaded in this browser.");
    } catch {
      clearPrivateState();
      setLogoutPending(true);
      setNotice("Sign out could not be confirmed. Private data is hidden; retry sign out before closing this browser.");
    }
  }

  function clearPrivateState() {
    setSession(null); setAsset(null); setAssets([]); setRules([]); setPreview(null);
    setWorkspace("unavailable");
    void controlPlane.getUiAuthConfig().then(setAuthConfig).catch(() => setAuthConfig(null));
  }

  function navigateTo(next: Page) {
    if (saveState === "unsaved" && !window.confirm("You have unsaved policy changes. Leave this editor?")) return;
    setPage(next);
  }

  async function loadAsset(assetId: string, knownAssets = assets) {
    try {
      const epoch = ++loadEpoch.current;
      const [fullAsset, loadedRules, schema, history] = await Promise.all([controlPlane.getAsset(assetId), controlPlane.listRules(assetId), controlPlane.getSchema(assetId), controlPlane.listAssetHistory(assetId).catch(() => [])]);
      if (epoch !== loadEpoch.current) return;
      fullAsset.schema = schema;
      const draft = isDemo ? null : await controlPlane.getDraft(assetId);
      const effectiveRules = draft?.rules ?? loadedRules;
      setManagementData((current) => ({ ...current, history }));
      setAssets(knownAssets); setAsset(fullAsset); setRules(effectiveRules); setDraftRevision(draft?.revision ?? 0); setSelectedRule(0);
      setSelectedField(schema.fields[0]?.human_path ?? fullAsset.schema_fields[0]?.name ?? ""); setPreview(null); setSaveState("saved");
      setNotice(effectiveRules.length ? "Loaded your policy draft." : "No policy draft exists yet. Add a rule to begin authoring.");
    } catch {
      setNotice("Could not load this asset. Your previous editor state remains unchanged.");
    }
  }

  const activeRule = rules[selectedRule];
  const selectedMask = activeRule?.masks[selectedField];
  const effectiveFields = useMemo(() => new Set(rules.flatMap((rule) => rule.columns)), [rules]);
  function updateRule(change: (rule: PolicyRule) => PolicyRule) {
    if (!activeRule) return;
    setRules((current) => current.map((rule, index) => index === selectedRule ? change(rule) : rule));
    setSaveState("unsaved"); setPreview(null); setNotice("Draft changed. Run a policy test before review.");
  }
  function addRule() {
    const ordinal = Math.max(0, ...rules.map((rule) => rule.ordinal)) + 10;
    setRules((current) => [...current, newRule(selectedField, ordinal)]);
    setSelectedRule(rules.length); setSaveState("unsaved"); setPreview(null);
    setNotice("New rule added locally. Add at least one principal before saving.");
  }
  function removeRule() {
    if (!activeRule) return;
    setRules((current) => current.filter((_, index) => index !== selectedRule));
    setSelectedRule(Math.max(0, selectedRule - 1)); setSaveState("unsaved"); setPreview(null);
    setNotice("Rule removed locally. Save the draft to persist the change.");
  }
  function toggleField(name: string) {
    updateRule((rule) => ({ ...rule, columns: rule.columns.includes(name) ? rule.columns.filter((column) => column !== name) : [...rule.columns, name] }));
  }
  function setMask(mask: Mask | undefined) {
    updateRule((rule) => {
      const masks = { ...rule.masks };
      if (mask) masks[selectedField] = mask; else delete masks[selectedField];
      return { ...rule, masks };
    });
  }
  async function saveDraft() {
    if (!asset) return;
    setSaveState("saving");
    try {
      if (!isDemo) {
        const saved = await controlPlane.saveDraft(asset.id, draftRevision, rules);
        setDraftRevision(saved.revision);
      }
      setSaveState("saved"); setNotice(isDemo ? "Demo draft resets when this page closes." : "Policy draft saved to the control plane.");
    } catch {
      setSaveState("failed"); setNotice("Save failed. The unsaved draft remains in this browser.");
    }
  }
  async function runPreview() {
    if (!asset || !activeRule) return;
    try {
      const result = isDemo ? { allowed_columns: activeRule.columns, masks: activeRule.masks, row_filter: activeRule.row_filter, policy_version: 1 } : await controlPlane.evaluate(asset.id, { principal: "analyst.alex", groups: ["us-analysts"], claims: {} });
      setPreview(result); setNotice("Server-side DuckDB evaluation is current for this saved draft revision.");
    } catch {
      setPreview(null); setNotice("Policy test could not run. This draft is not validated.");
    }
  }

  async function restorePolicyVersion(policyVersion: number) {
    if (!asset || isDemo) return;
    if (saveState === "unsaved" && !window.confirm("You have unsaved policy changes. Restore this published version over them?")) return;
    try {
      const restored = await controlPlane.restorePolicyVersion(asset.id, policyVersion, draftRevision);
      setRules(restored.rules);
      setDraftRevision(restored.revision);
      setSaveState("saved");
      setPreview(null);
      setNotice(`Version ${policyVersion} restored as draft revision ${restored.revision}. Review and publish it when ready.`);
    } catch {
      setNotice("Restore failed. The draft may have changed; reload the asset before trying again.");
    }
  }

  return <div className="app-shell">
    <aside className="sidebar" aria-label="Primary navigation">
      <a className="brand" href="#assets" onClick={() => navigateTo("assets")}>DAL OBSCURA<span>GOVERNANCE</span></a>
      <nav>{(["assets", "changes", "activity", "connections", "settings"] as Page[]).map((item) => <button key={item} className={page === item ? "nav-item active" : "nav-item"} onClick={() => navigateTo(item)}>{item}</button>)}</nav>
      <div className="sidebar-foot"><span className={"status-dot " + workspace} /> Workspace: {asset?.catalog ?? "unavailable"}<br /><small>{workspaceLabel(workspace)}</small></div>
    </aside>
    <main>
      <header className="topbar"><div><span className="eyebrow">{page === "assets" ? "ASSET WORKSPACE" : page.toUpperCase()}</span><h1>{page === "assets" ? asset?.name ?? "Assets" : titleFor(page)}</h1></div><div className="actor"><span className="avatar">{session?.principal.slice(0, 1).toUpperCase() ?? "?"}</span><div><strong>{session?.principal ?? "Not signed in"}</strong><small>{session?.platform_admin ? "Platform admin" : "Policy author"}</small></div>{session && <button className="text-button" onClick={() => void logout()}>Sign out</button>}{logoutPending && <button className="text-button" onClick={() => void logout()}>Retry sign out</button>}</div></header>
      {page !== "assets" ? <ManagementView page={page} data={managementData} loading={managementLoading} onReload={() => void loadManagement(page)} /> : workspace === "loading" ? <WorkspaceMessage title="Loading governed assets" message="Checking your workspace access and available assets." /> : workspace === "unavailable" ? <WorkspaceMessage title="Cannot load workspace" message={notice} retry={loadInitialWorkspace} authConfig={authConfig} onLogin={demoLogin} loggingIn={loggingIn} /> : !asset ? <WorkspaceMessage title="No governed assets" message={notice} /> : <AssetWorkspace assets={assets} asset={asset} history={managementData.history ?? []} onAsset={(id) => void loadAsset(id)} rules={rules} activeRule={activeRule} activeRevision={draftRevision} selectedRule={selectedRule} onRule={setSelectedRule} selectedField={selectedField} onField={setSelectedField} selectedMask={selectedMask} effectiveFields={effectiveFields} saveState={saveState} notice={notice} onToggleField={toggleField} onMask={setMask} onUpdateRule={updateRule} onAddRule={addRule} onRemoveRule={removeRule} onSave={() => void saveDraft()} onPreview={() => void runPreview()} onPublish={() => void controlPlane.publishAsset(asset.id, draftRevision).then(() => setNotice("Published the saved draft.")).catch(() => setNotice("Publish failed. Review the saved draft and active generation."))} onRestore={(version) => void restorePolicyVersion(version)} preview={preview} />}
    </main>
  </div>;
}

function AssetWorkspace(props: { assets: Asset[]; asset: Asset; history: PolicyVersion[]; onAsset: (id: string) => void; rules: PolicyRule[]; activeRule?: PolicyRule; activeRevision: number; selectedRule: number; onRule: (index: number) => void; selectedField: string; onField: (name: string) => void; selectedMask?: Mask; effectiveFields: Set<string>; saveState: SaveState; notice: string; onToggleField: (name: string) => void; onMask: (mask?: Mask) => void; onUpdateRule: (change: (rule: PolicyRule) => PolicyRule) => void; onAddRule: () => void; onRemoveRule: () => void; onSave: () => void; onPreview: () => void; onPublish: () => void; onRestore: (version: number) => void; preview: Preview | null }) {
  const [tab, setTab] = useState<"policy" | "tests" | "history">("policy");
  const fields = props.asset.schema?.fields ?? props.asset.schema_fields.map((field, index) => ({ field_id: index, name: field.name, path: { version: 1, segments: [{ kind: "field" as const, name: field.name, field_id: index }] }, human_path: field.name, type: field.type, nullable: field.nullable, kind: "scalar" as const }));
  const currentPreview = props.preview;
  return <><div className="asset-summary"><label>Asset <select value={props.asset.id} onChange={(event) => props.onAsset(event.target.value)}>{props.assets.map((item) => <option key={item.id} value={item.id}>{item.catalog} / {item.name}</option>)}</select></label><div className="save-status" aria-live="polite"><span className={"save-dot " + props.saveState} /> {saveLabel(props.saveState)}</div></div><div className="notice" role="status">{props.notice}</div><div className="asset-tabs" role="tablist" aria-label="Asset views">{(["policy", "tests", "history"] as const).map((item) => <button key={item} role="tab" aria-selected={tab === item} className={tab === item ? "selected" : ""} onClick={() => setTab(item)}>{item === "policy" ? "Policy" : item === "tests" ? "Tests" : "History"}</button>)}</div>{tab === "policy" ? <div className="studio">
    <section className="schema-panel" aria-label="Schema and field selection"><div className="panel-head"><div><span className="eyebrow">SCHEMA</span><h2>Fields & access</h2></div></div><p className="help">Fields retain server-defined paths. A checked field is visible through the selected rule.</p><ul className="field-tree">{fields.map((field) => <SchemaTree key={field.human_path} node={field} depth={0} selectedField={props.selectedField} effectiveFields={props.effectiveFields} onField={props.onField} />)}</ul><div className="schema-note"><strong>Nested fields</strong><p>Struct, list, and map paths come from the control plane. Collection nodes expose explicit <code>$element</code>, <code>$key</code>, and <code>$value</code> segments.</p></div></section>
    <section className="editor-panel" aria-label="Policy rule editor"><div className="panel-head"><div><span className="eyebrow">POLICY RULES</span><h2>{props.activeRule ? "Rule " + (props.selectedRule + 1) : "No rule selected"}</h2></div><button className="text-button" onClick={props.onAddRule}>Add rule</button></div>{props.rules.length ? <><div className="rule-list" aria-label="Policy rule list">{props.rules.map((rule, index) => <button key={index} className={index === props.selectedRule ? "selected" : ""} onClick={() => props.onRule(index)}>Rule {index + 1}<small>{rule.principals.join(", ") || "No principal"}</small></button>)}</div><EditorSection label="Who"><input value={props.activeRule?.principals.join(", ") ?? ""} onChange={(event) => props.onUpdateRule((rule) => ({ ...rule, principals: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) }))} aria-label="Principals or groups" placeholder="group:us-analysts" /></EditorSection><EditorSection label="Which fields"><label className="check-line"><input type="checkbox" checked={props.activeRule?.columns.includes(props.selectedField) ?? false} disabled={!props.selectedField} onChange={() => props.onToggleField(props.selectedField)} /> Include <strong>{props.selectedField || "a schema field"}</strong></label><p className="help">Parent/child conflicts and invalid nested paths are rejected by the control plane.</p></EditorSection><EditorSection label="Which rows"><textarea value={props.activeRule?.row_filter ?? ""} onChange={(event) => props.onUpdateRule((rule) => ({ ...rule, row_filter: event.target.value || null }))} aria-label="DuckDB row restriction" placeholder="region = 'US'" /><p className="help">Matching rules combine row restrictions with AND.</p></EditorSection><EditorSection label="How values appear"><MaskEditor mask={props.selectedMask} onChange={props.onMask} field={props.selectedField} /></EditorSection><div className="editor-actions"><button className="danger" onClick={props.onRemoveRule}>Remove rule</button><button className="secondary" onClick={props.onPreview}>Run policy test</button><button className="primary" onClick={props.onSave} disabled={props.saveState === "saving"}>{props.saveState === "saving" ? "Saving…" : "Save draft"}</button><button className="secondary" onClick={props.onPublish} disabled={props.saveState !== "saved"}>Publish reviewed draft</button></div></> : <div className="empty-result"><strong>No draft rules</strong><p>Start with one allow rule. It remains a draft until you save it and complete review.</p><button className="primary" onClick={props.onAddRule}>Add first rule</button></div>}</section>
    <section className="result-panel" aria-label="Effective access inspector"><span className="eyebrow">EFFECTIVE ACCESS</span><h2>US analyst</h2><p className="muted">Synthetic persona · not reader authentication</p>{currentPreview ? <><div className="result-state allowed">Test current</div><h3>Visible output</h3><ul>{currentPreview.allowed_columns.map((field) => <li key={field}>{field}{currentPreview.masks[field] && <small> · {currentPreview.masks[field].type} mask</small>}</li>)}</ul><h3>Row restriction</h3><code>{currentPreview.row_filter ?? "No matching row restriction"}</code></> : <div className="empty-result"><strong>Run a policy test</strong><p>See authorized output schema, masks, and row restriction for this draft.</p></div>}<div className="result-warning"><strong>Before publishing</strong><p>Review uses the exact saved draft. Changing fields, masks, or row restrictions makes this result stale.</p></div></section>
  </div> : tab === "tests" ? <TestsView onPreview={props.onPreview} preview={props.preview} /> : <HistoryView history={props.history.filter((item) => item.asset_id === props.asset.id)} onRestore={props.onRestore} disabled={props.saveState === "saving"} />}</>;
}

function SchemaTree({ node, depth, selectedField, effectiveFields, onField }: { node: SchemaNode; depth: number; selectedField: string; effectiveFields: Set<string>; onField: (name: string) => void }) {
  return <li><button className={selectedField === node.human_path ? "field-row selected" : "field-row"} style={{ paddingLeft: `${8 + depth * 16}px` }} onClick={() => onField(node.human_path)} aria-label={`Select ${node.human_path}`}><span className="field-name">{node.name}</span><span className="field-type">{node.type}{node.nullable ? " · nullable" : ""}</span>{effectiveFields.has(node.human_path) && <span className="grant">Granted</span>}</button>{node.children?.length ? <ul className="field-tree nested">{node.children.map((child) => <SchemaTree key={child.human_path} node={child} depth={depth + 1} selectedField={selectedField} effectiveFields={effectiveFields} onField={onField} />)}</ul> : null}</li>;
}

function EditorSection({ label, children }: { label: string; children: ReactNode }) { return <section className="editor-section"><h3>{label}</h3>{children}</section>; }
function MaskEditor({ mask, field, onChange }: { mask?: Mask; field: string; onChange: (mask?: Mask) => void }) { const option = maskOptions.find((candidate) => candidate.type === mask?.type); return <div className="mask-editor"><label htmlFor="mask-type">Mask for <strong>{field || "selected field"}</strong></label><select id="mask-type" value={mask?.type ?? ""} disabled={!field} onChange={(event) => { const type = event.target.value as Mask["type"] | ""; onChange(type ? { type } : undefined); }}><option value="">No mask</option>{maskOptions.map((candidate) => <option key={candidate.type} value={candidate.type}>{candidate.label}</option>)}</select>{option?.needsValue && <input aria-label="Mask value" value={mask?.value ?? ""} placeholder={mask?.type === "keep_last" ? "Characters to retain" : "Replacement value"} onChange={(event) => onChange({ ...mask!, value: mask?.type === "keep_last" ? Number(event.target.value) : event.target.value })} />}<p className="help">Mask behavior is validated by the control plane before publication.</p></div>; }
function TestsView({ onPreview, preview }: { onPreview: () => void; preview: Preview | null }) { return <section className="tests-view"><span className="eyebrow">POLICY TESTS</span><h2>Test before review</h2><p>Simulate a representative persona. This is a policy evaluation, not an impersonated read or data preview.</p><div className="test-card"><div><strong>US analyst</strong><small>group:us-analysts</small></div><button className="primary" onClick={onPreview}>Run test</button></div>{preview && <div className="result-state allowed">Current: {preview.allowed_columns.length} fields visible</div>}</section>; }
function HistoryView({ history, onRestore, disabled }: { history: PolicyVersion[]; onRestore: (version: number) => void; disabled: boolean }) { return <section className="tests-view"><span className="eyebrow">HISTORY</span><h2>Published policy revisions</h2><p>Immutable versions returned by the control plane. Restore creates a new editable draft and never changes published history.</p>{history.length ? <div className="table-wrap"><table><thead><tr><th>Version</th><th>State</th><th>Created</th><th>Action</th></tr></thead><tbody>{history.map((item) => <tr key={item.policy_version}><td><code>{item.policy_version}</code></td><td>{item.active ? "Active" : "Published"}</td><td>{new Date(item.created_at).toLocaleString()}</td><td><button className="secondary compact" disabled={disabled} onClick={() => onRestore(item.policy_version)}>Restore to draft</button></td></tr>)}</tbody></table></div> : <div className="empty-result"><strong>No historical revision loaded</strong><p>Publish a reviewed draft to create the first immutable revision.</p></div>}</section>; }
function ManagementView({ page, data, loading, onReload }: { page: Exclude<Page, "assets">; data: ManagementData; loading: boolean; onReload: () => void }) {
  if (loading) return <section className="coming-soon"><span className="eyebrow">{page.toUpperCase()}</span><h2>Loading {page}</h2><p>Checking the current workspace state and your capabilities.</p></section>;
  if (page === "changes") return <ChangesView history={data.history ?? []} onReload={onReload} />;
  if (page === "activity") return <ActivityView history={data.history ?? []} events={data.events ?? []} summary={data.summary} />;
  if (page === "connections") return <ConnectionsView catalogs={data.catalogs ?? []} onReload={onReload} />;
  return <SettingsView runtime={data.runtime} providers={data.providers ?? []} onReload={onReload} />;
}

function ChangesView({ history, onReload }: { history: PolicyVersion[]; onReload: () => void }) {
  return <section className="management-view"><div className="management-head"><div><span className="eyebrow">CHANGES</span><h2>Published policy history</h2><p className="muted">Immutable versions returned by the control plane. A publication is active only when the server says so.</p></div><button className="secondary" onClick={onReload}>Refresh</button></div>{history.length ? <div className="table-wrap"><table><thead><tr><th>Asset</th><th>Version</th><th>State</th><th>Created</th></tr></thead><tbody>{history.map((item) => <tr key={`${item.asset_id}-${item.policy_version}`}><td><strong>{item.asset_name}</strong><small>{item.catalog} / {item.target}</small></td><td><code>{item.policy_version}</code></td><td><span className={item.active ? "result-state allowed" : "pill"}>{item.active ? "Active" : "Published"}</span></td><td>{new Date(item.created_at).toLocaleString()}</td></tr>)}</tbody></table></div> : <div className="empty-result"><strong>No published changes</strong><p>Save and publish an asset draft to create an immutable version.</p></div>}</section>;
}

function ActivityView({ history, events, summary }: { history: PolicyVersion[]; events: AuditEvent[]; summary?: WorkspaceSummary }) {
  return <section className="management-view"><span className="eyebrow">ACTIVITY</span><h2>Workspace status</h2><p className="muted">Live counts and redacted audit observations from the control plane. Data is limited to the assets your session can see.</p>{summary ? <div className="metric-grid">{[["Assets", summary.asset_count], ["Catalogs", summary.catalog_count], ["Draft changes", summary.draft_change_count], ["Missing policy", summary.missing_policy_count], ["Enabled auth", summary.enabled_auth_provider_count]].map(([label, value]) => <div className="metric-card" key={String(label)}><strong>{String(value)}</strong><span>{label}</span></div>)}</div> : <div className="empty-result"><strong>Status unavailable</strong><p>Reconnect with a session that can read workspace observations.</p></div>}<h3 className="activity-title">Recent governed actions</h3>{events.length ? <ul className="activity-list">{events.slice(0, 8).map((event) => <li key={event.id}><span className={"status-dot " + (event.outcome === "success" ? "ready" : "unavailable")} /><div><strong>{event.action}</strong><small>{event.actor} · {event.resource_type} {event.resource_id.slice(0, 8)}</small></div><time>{new Date(event.created_at).toLocaleString()}</time></li>)}</ul> : history.length ? <ul className="activity-list">{history.slice(-8).reverse().map((item) => <li key={`${item.asset_id}-${item.policy_version}`}><span className="status-dot ready" /><div><strong>{item.asset_name}</strong><small>Policy version {item.policy_version} · {item.active ? "active" : "published"}</small></div><time>{new Date(item.created_at).toLocaleString()}</time></li>)}</ul> : <div className="empty-result"><strong>No activity yet</strong><p>Draft, restore, and publication actions will appear here after the first governed change.</p></div>}</section>;
}

function ConnectionsView({ catalogs, onReload }: { catalogs: Catalog[]; onReload: () => void }) {
  const [name, setName] = useState("");
  const [uri, setUri] = useState("");
  const [message, setMessage] = useState("");
  const [tables, setTables] = useState<Array<Record<string, unknown>>>([]);
  async function save() {
    if (!name.trim() || !uri.trim()) return setMessage("Connection name and catalog URI are required.");
    try { await controlPlane.saveCatalog(name.trim(), { type: "sql", uri: uri.trim() }); setMessage("Connection saved. Discovery remains bounded to this configured catalog."); setName(""); setUri(""); onReload(); } catch { setMessage("Connection was rejected by the control plane."); }
  }
  async function discover(catalog: string) {
    try { setTables((await controlPlane.discoverCatalogTables(catalog)).tables); setMessage(`Loaded table inventory for ${catalog}.`); } catch { setTables([]); setMessage("Discovery failed; source credentials and endpoint policy were not changed."); }
  }
  return <section className="management-view"><div className="management-head"><div><span className="eyebrow">CONNECTIONS</span><h2>Iceberg catalogs</h2><p className="muted">Register only the supported Iceberg catalog adapter. Credentials stay in server configuration and are never rendered.</p></div><button className="secondary" onClick={onReload}>Refresh</button></div><div className="form-card"><h3>Add or update catalog</h3><div className="form-grid"><label>Name<input value={name} onChange={(event) => setName(event.target.value)} placeholder="analytics" /></label><label>SQL catalog URI<input value={uri} onChange={(event) => setUri(event.target.value)} placeholder="postgresql+psycopg://…" type="text" /></label></div><button className="primary" onClick={() => void save()}>Save connection</button>{message && <p className="notice">{message}</p>}</div>{catalogs.length ? <div className="card-list">{catalogs.map((catalog) => <article className="management-card" key={catalog.id}><div><h3>{catalog.name}</h3><p className="muted">Iceberg catalog adapter</p></div><button className="secondary" onClick={() => void discover(catalog.name)}>Discover tables</button></article>)}</div> : <div className="empty-result"><strong>No catalogs configured</strong><p>Connect an Iceberg catalog to begin asset onboarding.</p></div>}{tables.length > 0 && <div className="table-wrap"><table><thead><tr><th>Table</th><th>Backend</th><th>Governed</th></tr></thead><tbody>{tables.map((table, index) => <tr key={String(table.name ?? index)}><td>{String(table.name ?? "Unknown")}</td><td>{String(table.backend ?? "iceberg")}</td><td>{table.governed ? "Yes" : "No"}</td></tr>)}</tbody></table></div>}</section>;
}

function SettingsView({ runtime, providers, onReload }: { runtime?: RuntimeSettings | null; providers: AuthProvider[]; onReload: () => void }) {
  const [form, setForm] = useState<RuntimeSettings>(runtime ?? { ticket_ttl_seconds: 900, max_tickets: 64, max_ticket_exchanges: 2 });
  const [message, setMessage] = useState("");
  useEffect(() => setForm(runtime ?? { ticket_ttl_seconds: 900, max_tickets: 64, max_ticket_exchanges: 2 }), [runtime]);
  async function save() { try { await controlPlane.saveRuntimeSettings(form); setMessage("Runtime settings saved as draft configuration. Publish to make worker behavior change."); onReload(); } catch { setMessage("Settings update was rejected; the previous values remain active."); } }
  return <section className="management-view"><div className="management-head"><div><span className="eyebrow">SETTINGS</span><h2>Runtime and identity</h2><p className="muted">These controls affect ticket fan-out and authentication. Changes are server-validated and do not expose secrets.</p></div><button className="secondary" onClick={onReload}>Refresh</button></div><div className="form-card"><h3>Runtime limits</h3><div className="form-grid three"><label>Ticket TTL (seconds)<input type="number" min="1" value={form.ticket_ttl_seconds} onChange={(event) => setForm({ ...form, ticket_ttl_seconds: Number(event.target.value) })} /></label><label>Max tickets<input type="number" min="1" value={form.max_tickets} onChange={(event) => setForm({ ...form, max_tickets: Number(event.target.value) })} /></label><label>Ticket exchanges<input type="number" min="1" value={form.max_ticket_exchanges} onChange={(event) => setForm({ ...form, max_ticket_exchanges: Number(event.target.value) })} /></label></div><button className="primary" onClick={() => void save()}>Save runtime settings</button>{message && <p className="notice">{message}</p>}</div><div className="form-card"><h3>Authentication providers</h3>{providers.length ? <ul className="provider-list">{providers.map((provider) => <li key={provider.id}><span className={provider.enabled ? "status-dot ready" : "status-dot unavailable"} /><div><strong>{provider.module.split(".").at(-1)}</strong><small>Order {provider.ordinal} · {provider.enabled ? "Enabled" : "Disabled"}</small></div></li>)}</ul> : <div className="empty-result"><strong>No provider configured</strong><p>Production startup must fail closed until an approved identity provider is enabled.</p></div>}</div></section>;
}
function WorkspaceMessage({ title, message, retry, authConfig, onLogin, loggingIn }: { title: string; message: string; retry?: () => void; authConfig?: UiAuthConfig | null; onLogin?: (loginHint: string) => void; loggingIn?: boolean }) { return <section className="coming-soon"><span className="eyebrow">WORKSPACE</span><h2>{title}</h2><p>{message}</p>{authConfig?.authority && <button className="primary login-shortcut" onClick={controlPlane.startLogin}>Sign in with SSO</button>}{authConfig?.login_shortcuts?.map((shortcut) => shortcut.demo_login_path && onLogin ? <button className="secondary login-shortcut" disabled={loggingIn} key={shortcut.login_hint} onClick={() => onLogin(shortcut.login_hint)}>{loggingIn ? "Signing in…" : `Use demo persona · ${shortcut.label}`}</button> : null)}{retry && <button className="secondary" onClick={() => void retry()}>Retry</button>}</section>; }
function ComingSoon({ page }: { page: Page }) { return <section className="coming-soon"><span className="eyebrow">{page.toUpperCase()}</span><h2>{titleFor(page)}</h2><p>This destination stays unavailable until its server contract and authorization checks are implemented.</p></section>; }
function titleFor(page: Page) { return ({ changes: "Changes", activity: "Activity", connections: "Connections", settings: "Settings", assets: "Assets" })[page]; }
function saveLabel(state: SaveState) { return ({ saved: "Saved draft", saving: "Saving draft", unsaved: "Unsaved changes", failed: "Save failed" })[state]; }
function workspaceLabel(state: WorkspaceState) { return ({ loading: "Checking access", ready: "Connected", demo: "Explicit demo", unavailable: "Unavailable" })[state]; }
createRoot(document.getElementById("root")!).render(<App />);
