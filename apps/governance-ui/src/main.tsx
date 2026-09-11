import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { createRoot } from "react-dom/client";
import type { Asset, Mask, PolicyRule, Preview, Session, UiAuthConfig } from "./api";
import { controlPlane } from "./api";
import { demoAsset, demoRules } from "./fixtures";
import "./styles.css";

type Page = "assets" | "changes" | "activity" | "connections" | "settings";
type SaveState = "saved" | "saving" | "unsaved" | "failed";
type WorkspaceState = "loading" | "ready" | "demo" | "unavailable";

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
  const [selectedRule, setSelectedRule] = useState(0);
  const [selectedField, setSelectedField] = useState("");
  const [saveState, setSaveState] = useState<SaveState>("saved");
  const [preview, setPreview] = useState<Preview | null>(null);
  const [notice, setNotice] = useState("Loading workspace…");
  const [session, setSession] = useState<Session | null>(null);
  const [authConfig, setAuthConfig] = useState<UiAuthConfig | null>(null);
  const [loggingIn, setLoggingIn] = useState(false);
  const isDemo = workspace === "demo";

  useEffect(() => {
    if (new URLSearchParams(window.location.search).has("demo")) {
      setAssets([demoAsset]); setAsset(demoAsset); setRules(demoRules); setSelectedField(demoAsset.schema_fields[0].name);
      setWorkspace("demo"); setNotice("Demo workspace. It never writes to the control plane.");
      return;
    }
    void loadInitialWorkspace();
  }, []);

  async function loadInitialWorkspace() {
    try {
      setWorkspace("loading");
      setSession(await controlPlane.getSession());
      const loaded = await controlPlane.listAssets();
      setAssets(loaded);
      if (!loaded.length) {
        setWorkspace("ready"); setNotice("No governed assets are available in this workspace.");
        return;
      }
      await loadAsset(loaded[0].id, loaded);
      setWorkspace("ready");
    } catch {
      setWorkspace("unavailable");
      setSession(null);
      setAuthConfig(await controlPlane.getUiAuthConfig().catch(() => null));
      setNotice("Workspace unavailable. Sign in or reconnect to the control plane; no demo data is shown automatically.");
    }
  }

  async function login(loginHint: string) {
    setLoggingIn(true);
    try {
      await controlPlane.demoLogin(loginHint);
      await loadInitialWorkspace();
    } catch {
      setWorkspace("unavailable");
      setNotice("Sign-in failed. No policy data was loaded.");
    } finally {
      setLoggingIn(false);
    }
  }

  async function loadAsset(assetId: string, knownAssets = assets) {
    try {
      const [fullAsset, loadedRules] = await Promise.all([controlPlane.getAsset(assetId), controlPlane.listRules(assetId)]);
      setAssets(knownAssets); setAsset(fullAsset); setRules(loadedRules); setSelectedRule(0);
      setSelectedField(fullAsset.schema_fields[0]?.name ?? ""); setPreview(null); setSaveState("saved");
      setNotice(loadedRules.length ? "Loaded saved policy draft." : "No policy draft exists yet. Add a rule to begin authoring.");
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
      if (!isDemo) await controlPlane.saveRules(asset.id, rules);
      setSaveState("saved"); setNotice(isDemo ? "Demo draft resets when this page closes." : "Policy draft saved to the control plane.");
    } catch {
      setSaveState("failed"); setNotice("Save failed. The unsaved draft remains in this browser.");
    }
  }
  async function runPreview() {
    if (!asset || !activeRule) return;
    try {
      const result = isDemo ? { allowed_columns: activeRule.columns, masks: activeRule.masks, row_filter: activeRule.row_filter, policy_version: 1 } : await controlPlane.preview(asset.id, { principal: "analyst.alex", groups: ["us-analysts"], claims: {} });
      setPreview(result); setNotice("Policy test is current for this saved draft revision.");
    } catch {
      setPreview(null); setNotice("Policy test could not run. This draft is not validated.");
    }
  }

  return <div className="app-shell">
    <aside className="sidebar" aria-label="Primary navigation">
      <a className="brand" href="#assets" onClick={() => setPage("assets")}>DAL OBSCURA<span>GOVERNANCE</span></a>
      <nav>{(["assets", "changes", "activity", "connections", "settings"] as Page[]).map((item) => <button key={item} className={page === item ? "nav-item active" : "nav-item"} onClick={() => setPage(item)}>{item}</button>)}</nav>
      <div className="sidebar-foot"><span className={"status-dot " + workspace} /> Workspace: analytics<br /><small>{workspaceLabel(workspace)}</small></div>
    </aside>
    <main>
      <header className="topbar"><div><span className="eyebrow">{page === "assets" ? "ASSET WORKSPACE" : page.toUpperCase()}</span><h1>{page === "assets" ? asset?.name ?? "Assets" : titleFor(page)}</h1></div><div className="actor"><span className="avatar">{session?.principal.slice(0, 1).toUpperCase() ?? "?"}</span><div><strong>{session?.principal ?? "Not signed in"}</strong><small>{session?.platform_admin ? "Platform admin" : "Policy author"}</small></div></div></header>
      {page !== "assets" ? <ComingSoon page={page} /> : workspace === "loading" ? <WorkspaceMessage title="Loading governed assets" message="Checking your workspace access and available assets." /> : workspace === "unavailable" ? <WorkspaceMessage title="Cannot load workspace" message={notice} retry={loadInitialWorkspace} authConfig={authConfig} onLogin={login} loggingIn={loggingIn} /> : !asset ? <WorkspaceMessage title="No governed assets" message={notice} /> : <AssetWorkspace assets={assets} asset={asset} onAsset={(id) => void loadAsset(id)} rules={rules} activeRule={activeRule} selectedRule={selectedRule} onRule={setSelectedRule} selectedField={selectedField} onField={setSelectedField} selectedMask={selectedMask} effectiveFields={effectiveFields} saveState={saveState} notice={notice} onToggleField={toggleField} onMask={setMask} onUpdateRule={updateRule} onAddRule={addRule} onRemoveRule={removeRule} onSave={() => void saveDraft()} onPreview={() => void runPreview()} preview={preview} />}
    </main>
  </div>;
}

function AssetWorkspace(props: { assets: Asset[]; asset: Asset; onAsset: (id: string) => void; rules: PolicyRule[]; activeRule?: PolicyRule; selectedRule: number; onRule: (index: number) => void; selectedField: string; onField: (name: string) => void; selectedMask?: Mask; effectiveFields: Set<string>; saveState: SaveState; notice: string; onToggleField: (name: string) => void; onMask: (mask?: Mask) => void; onUpdateRule: (change: (rule: PolicyRule) => PolicyRule) => void; onAddRule: () => void; onRemoveRule: () => void; onSave: () => void; onPreview: () => void; preview: Preview | null }) {
  const [tab, setTab] = useState<"policy" | "tests" | "history">("policy");
  const fields = props.asset.schema_fields;
  const currentPreview = props.preview;
  return <><div className="asset-summary"><label>Asset <select value={props.asset.id} onChange={(event) => props.onAsset(event.target.value)}>{props.assets.map((item) => <option key={item.id} value={item.id}>{item.catalog} / {item.name}</option>)}</select></label><div className="save-status" aria-live="polite"><span className={"save-dot " + props.saveState} /> {saveLabel(props.saveState)}</div></div><div className="notice" role="status">{props.notice}</div><div className="asset-tabs" role="tablist" aria-label="Asset views">{(["policy", "tests", "history"] as const).map((item) => <button key={item} role="tab" aria-selected={tab === item} className={tab === item ? "selected" : ""} onClick={() => setTab(item)}>{item === "policy" ? "Policy" : item === "tests" ? "Tests" : "History"}</button>)}</div>{tab === "policy" ? <div className="studio">
    <section className="schema-panel" aria-label="Schema and field selection"><div className="panel-head"><div><span className="eyebrow">SCHEMA</span><h2>Fields & access</h2></div></div><p className="help">Fields retain server-defined paths. A checked field is visible through the selected rule.</p><ul className="field-tree">{fields.map((field) => <li key={field.name}><button className={props.selectedField === field.name ? "field-row selected" : "field-row"} onClick={() => props.onField(field.name)}><span className="field-name">{field.name}</span><span className="field-type">{field.type}</span>{props.effectiveFields.has(field.name) && <span className="grant">Granted</span>}</button></li>)}</ul><div className="schema-note"><strong>Nested fields</strong><p>Struct, list, and map paths must come from the control plane. New fields require revalidation; wildcard grants never expand silently.</p></div></section>
    <section className="editor-panel" aria-label="Policy rule editor"><div className="panel-head"><div><span className="eyebrow">POLICY RULES</span><h2>{props.activeRule ? "Rule " + (props.selectedRule + 1) : "No rule selected"}</h2></div><button className="text-button" onClick={props.onAddRule}>Add rule</button></div>{props.rules.length ? <><div className="rule-list" aria-label="Policy rule list">{props.rules.map((rule, index) => <button key={index} className={index === props.selectedRule ? "selected" : ""} onClick={() => props.onRule(index)}>Rule {index + 1}<small>{rule.principals.join(", ") || "No principal"}</small></button>)}</div><EditorSection label="Who"><input value={props.activeRule?.principals.join(", ") ?? ""} onChange={(event) => props.onUpdateRule((rule) => ({ ...rule, principals: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) }))} aria-label="Principals or groups" placeholder="group:us-analysts" /></EditorSection><EditorSection label="Which fields"><label className="check-line"><input type="checkbox" checked={props.activeRule?.columns.includes(props.selectedField) ?? false} disabled={!props.selectedField} onChange={() => props.onToggleField(props.selectedField)} /> Include <strong>{props.selectedField || "a schema field"}</strong></label><p className="help">Parent/child conflicts and invalid nested paths are rejected by the control plane.</p></EditorSection><EditorSection label="Which rows"><textarea value={props.activeRule?.row_filter ?? ""} onChange={(event) => props.onUpdateRule((rule) => ({ ...rule, row_filter: event.target.value || null }))} aria-label="DuckDB row restriction" placeholder="region = 'US'" /><p className="help">Matching rules combine row restrictions with AND.</p></EditorSection><EditorSection label="How values appear"><MaskEditor mask={props.selectedMask} onChange={props.onMask} field={props.selectedField} /></EditorSection><div className="editor-actions"><button className="danger" onClick={props.onRemoveRule}>Remove rule</button><button className="secondary" onClick={props.onPreview}>Run policy test</button><button className="primary" onClick={props.onSave}>Save draft</button></div></> : <div className="empty-result"><strong>No draft rules</strong><p>Start with one allow rule. It remains a draft until you save it and complete review.</p><button className="primary" onClick={props.onAddRule}>Add first rule</button></div>}</section>
    <section className="result-panel" aria-label="Effective access inspector"><span className="eyebrow">EFFECTIVE ACCESS</span><h2>US analyst</h2><p className="muted">Synthetic persona · not reader authentication</p>{currentPreview ? <><div className="result-state allowed">Test current</div><h3>Visible output</h3><ul>{currentPreview.allowed_columns.map((field) => <li key={field}>{field}{currentPreview.masks[field] && <small> · {currentPreview.masks[field].type} mask</small>}</li>)}</ul><h3>Row restriction</h3><code>{currentPreview.row_filter ?? "No matching row restriction"}</code></> : <div className="empty-result"><strong>Run a policy test</strong><p>See authorized output schema, masks, and row restriction for this draft.</p></div>}<div className="result-warning"><strong>Before publishing</strong><p>Review uses the exact saved draft. Changing fields, masks, or row restrictions makes this result stale.</p></div></section>
  </div> : tab === "tests" ? <TestsView onPreview={props.onPreview} preview={props.preview} /> : <HistoryView />}</>;
}

function EditorSection({ label, children }: { label: string; children: ReactNode }) { return <section className="editor-section"><h3>{label}</h3>{children}</section>; }
function MaskEditor({ mask, field, onChange }: { mask?: Mask; field: string; onChange: (mask?: Mask) => void }) { const option = maskOptions.find((candidate) => candidate.type === mask?.type); return <div className="mask-editor"><label htmlFor="mask-type">Mask for <strong>{field || "selected field"}</strong></label><select id="mask-type" value={mask?.type ?? ""} disabled={!field} onChange={(event) => { const type = event.target.value as Mask["type"] | ""; onChange(type ? { type } : undefined); }}><option value="">No mask</option>{maskOptions.map((candidate) => <option key={candidate.type} value={candidate.type}>{candidate.label}</option>)}</select>{option?.needsValue && <input aria-label="Mask value" value={mask?.value ?? ""} placeholder={mask?.type === "keep_last" ? "Characters to retain" : "Replacement value"} onChange={(event) => onChange({ ...mask!, value: mask?.type === "keep_last" ? Number(event.target.value) : event.target.value })} />}<p className="help">Mask behavior is validated by the control plane before publication.</p></div>; }
function TestsView({ onPreview, preview }: { onPreview: () => void; preview: Preview | null }) { return <section className="tests-view"><span className="eyebrow">POLICY TESTS</span><h2>Test before review</h2><p>Simulate a representative persona. This is a policy evaluation, not an impersonated read or data preview.</p><div className="test-card"><div><strong>US analyst</strong><small>group:us-analysts</small></div><button className="primary" onClick={onPreview}>Run test</button></div>{preview && <div className="result-state allowed">Current: {preview.allowed_columns.length} fields visible</div>}</section>; }
function HistoryView() { return <section className="tests-view"><span className="eyebrow">HISTORY</span><h2>Active policy</h2><p>Published revisions will appear here with an exact diff, publisher attribution, and a restore-as-draft action.</p><div className="empty-result"><strong>No historical revision loaded</strong><p>History requires the revision API slice.</p></div></section>; }
function WorkspaceMessage({ title, message, retry, authConfig, onLogin, loggingIn }: { title: string; message: string; retry?: () => void; authConfig?: UiAuthConfig | null; onLogin?: (loginHint: string) => void; loggingIn?: boolean }) { return <section className="coming-soon"><span className="eyebrow">WORKSPACE</span><h2>{title}</h2><p>{message}</p>{authConfig?.login_shortcuts?.map((shortcut) => shortcut.demo_login_path && onLogin ? <button className="primary login-shortcut" disabled={loggingIn} key={shortcut.login_hint} onClick={() => onLogin(shortcut.login_hint)}>{loggingIn ? "Signing in…" : `Sign in as ${shortcut.label}`}</button> : null)}{retry && <button className="secondary" onClick={() => void retry()}>Retry</button>}</section>; }
function ComingSoon({ page }: { page: Page }) { return <section className="coming-soon"><span className="eyebrow">{page.toUpperCase()}</span><h2>{titleFor(page)}</h2><p>This destination stays unavailable until its server contract and authorization checks are implemented.</p></section>; }
function titleFor(page: Page) { return ({ changes: "Changes", activity: "Activity", connections: "Connections", settings: "Settings", assets: "Assets" })[page]; }
function saveLabel(state: SaveState) { return ({ saved: "Saved draft", saving: "Saving draft", unsaved: "Unsaved changes", failed: "Save failed" })[state]; }
function workspaceLabel(state: WorkspaceState) { return ({ loading: "Checking access", ready: "Connected", demo: "Explicit demo", unavailable: "Unavailable" })[state]; }
createRoot(document.getElementById("root")!).render(<App />);
