import { useEffect, useMemo, useState } from "react";
import type { ReactNode } from "react";
import { createRoot } from "react-dom/client";
import type { Asset, Mask, PolicyRule, Preview } from "./api";
import { controlPlane } from "./api";
import { demoAsset, demoRules } from "./fixtures";
import "./styles.css";

type Page = "assets" | "changes" | "activity" | "connections" | "settings";
type SaveState = "saved" | "saving" | "unsaved" | "failed";

const maskOptions: Array<{ type: Mask["type"]; label: string; needsValue?: boolean }> = [
  { type: "null", label: "Null" },
  { type: "redact", label: "Redact", needsValue: true },
  { type: "hash", label: "Hash" },
  { type: "email", label: "Email" },
  { type: "keep_last", label: "Keep last", needsValue: true },
  { type: "default", label: "Default", needsValue: true },
];

function App() {
  const [page, setPage] = useState<Page>("assets");
  const [assets, setAssets] = useState<Asset[]>([demoAsset]);
  const [asset, setAsset] = useState<Asset>(demoAsset);
  const [rules, setRules] = useState<PolicyRule[]>(demoRules);
  const [selectedRule, setSelectedRule] = useState(0);
  const [selectedField, setSelectedField] = useState("email");
  const [saveState, setSaveState] = useState<SaveState>("saved");
  const [preview, setPreview] = useState<Preview | null>(null);
  const [isDemo, setIsDemo] = useState(true);
  const [notice, setNotice] = useState("Working draft is local until you save it.");

  useEffect(() => {
    controlPlane.listAssets()
      .then(async (loaded) => {
        if (loaded.length) {
          const fullAsset = await controlPlane.getAsset(loaded[0].id);
          setAssets(loaded);
          setAsset(fullAsset);
          setIsDemo(false);
          return controlPlane.listRules(fullAsset.id).then(setRules);
        }
      })
      .catch(() => setNotice("Demo workspace shown. Connect the control plane to load live assets."));
  }, []);

  const activeRule = rules[selectedRule] ?? demoRules[0];
  const selectedMask = activeRule.masks[selectedField];
  const effectiveFields = useMemo(
    () => new Set(rules.flatMap((rule) => rule.columns)),
    [rules],
  );

  function updateRule(change: (rule: PolicyRule) => PolicyRule) {
    setRules((current) => current.map((rule, index) => (index === selectedRule ? change(rule) : rule)));
    setSaveState("unsaved");
    setPreview(null);
    setNotice("Draft changed. Run a policy test before publishing.");
  }

  function toggleField(name: string) {
    updateRule((rule) => ({
      ...rule,
      columns: rule.columns.includes(name)
        ? rule.columns.filter((column) => column !== name)
        : [...rule.columns, name],
    }));
  }

  function setMask(mask: Mask | undefined) {
    updateRule((rule) => {
      const masks = { ...rule.masks };
      if (mask) masks[selectedField] = mask;
      else delete masks[selectedField];
      return { ...rule, masks };
    });
  }

  async function saveDraft() {
    setSaveState("saving");
    try {
      if (!isDemo) await controlPlane.saveRules(asset.id, rules);
      setSaveState("saved");
      setNotice(isDemo ? "Draft saved locally in this prototype." : "Policy draft saved to the control plane.");
    } catch {
      setSaveState("failed");
      setNotice("Save failed. Your draft remains in this browser. Try again after reconnecting.");
    }
  }

  async function runPreview() {
    try {
      const result = isDemo
        ? { allowed_columns: activeRule.columns, masks: activeRule.masks, row_filter: activeRule.row_filter, policy_version: 1 }
        : await controlPlane.preview(asset.id, { principal: "analyst.alex", groups: ["us-analysts"], claims: {} });
      setPreview(result);
      setNotice("Policy test is current for this draft revision.");
    } catch {
      setPreview(null);
      setNotice("Policy test could not run. It is not safe to treat this draft as validated.");
    }
  }

  return (
    <div className="app-shell">
      <aside className="sidebar" aria-label="Primary navigation">
        <a className="brand" href="#assets" onClick={() => setPage("assets")}>DAL OBSCURA<span>GOVERNANCE</span></a>
        <nav>
          {(["assets", "changes", "activity", "connections", "settings"] as Page[]).map((item) => (
            <button key={item} className={page === item ? "nav-item active" : "nav-item"} onClick={() => setPage(item)}>
              {item}
            </button>
          ))}
        </nav>
        <div className="sidebar-foot"><span className="status-dot" /> Workspace: analytics<br /><small>{isDemo ? "Demo data" : "Connected"}</small></div>
      </aside>
      <main>
        <header className="topbar">
          <div><span className="eyebrow">{page === "assets" ? "ASSET WORKSPACE" : page.toUpperCase()}</span><h1>{page === "assets" ? asset.name : titleFor(page)}</h1></div>
          <div className="actor"><span className="avatar">A</span><div><strong>Alex Morgan</strong><small>Policy author</small></div></div>
        </header>
        {page === "assets" ? (
          <AssetWorkspace
            assets={assets} asset={asset} onAsset={setAsset} rules={rules} activeRule={activeRule}
            selectedRule={selectedRule} onRule={setSelectedRule} selectedField={selectedField} onField={setSelectedField}
            selectedMask={selectedMask} effectiveFields={effectiveFields} saveState={saveState} notice={notice}
            onToggleField={toggleField} onMask={setMask} onRule={setSelectedRule} onUpdateRule={updateRule}
            onSave={saveDraft} onPreview={runPreview} preview={preview}
          />
        ) : <ComingSoon page={page} />}
      </main>
    </div>
  );
}

function AssetWorkspace(props: {
  assets: Asset[]; asset: Asset; onAsset: (asset: Asset) => void; rules: PolicyRule[]; activeRule: PolicyRule;
  selectedRule: number; onRule: (index: number) => void; selectedField: string; onField: (name: string) => void;
  selectedMask?: Mask; effectiveFields: Set<string>; saveState: SaveState; notice: string;
  onToggleField: (name: string) => void; onMask: (mask?: Mask) => void;
  onUpdateRule: (change: (rule: PolicyRule) => PolicyRule) => void; onSave: () => void; onPreview: () => void; preview: Preview | null;
}) {
  const [tab, setTab] = useState<"policy" | "tests" | "history">("policy");
  return <>
    <div className="asset-summary">
      <div><span className="pill">Iceberg</span><span className="muted"> {props.asset.catalog} / {props.asset.table_identifier}</span></div>
      <div className="save-status" aria-live="polite"><span className={`save-dot ${props.saveState}`} /> {saveLabel(props.saveState)}</div>
    </div>
    <div className="notice" role="status">{props.notice}</div>
    <div className="asset-tabs" role="tablist" aria-label="Asset views">
      {(["policy", "tests", "history"] as const).map((item) => <button key={item} role="tab" aria-selected={tab === item} className={tab === item ? "selected" : ""} onClick={() => setTab(item)}>{item === "policy" ? "Policy" : item === "tests" ? "Tests" : "History"}</button>)}
    </div>
    {tab === "policy" ? <div className="studio">
      <section className="schema-panel" aria-label="Schema and field selection">
        <div className="panel-head"><div><span className="eyebrow">SCHEMA</span><h2>Fields & access</h2></div><input aria-label="Search schema" placeholder="Search fields" /></div>
        <p className="help">Select a field to edit its access. A checked field is visible through this rule.</p>
        <ul className="field-tree">
          {props.asset.schema_fields.map((field) => <li key={field.name}>
            <button className={props.selectedField === field.name ? "field-row selected" : "field-row"} onClick={() => props.onField(field.name)}>
              <span className="field-name">{field.name}</span><span className="field-type">{field.type}</span>
              {props.effectiveFields.has(field.name) && <span className="grant" aria-label="Granted">Granted</span>}
            </button>
          </li>)}
        </ul>
        <div className="schema-note"><strong>Nested fields</strong><p>Struct, list and map paths remain typed. New schema fields require revalidation; wildcard grants never expand silently.</p></div>
      </section>
      <section className="editor-panel" aria-label="Policy rule editor">
        <div className="panel-head"><div><span className="eyebrow">RULE {props.selectedRule + 1}</span><h2>US analyst access</h2></div><button className="text-button" onClick={() => props.onRule(0)}>Rule list</button></div>
        <EditorSection label="Who"><input value={props.activeRule.principals.join(", ")} onChange={(event) => props.onUpdateRule((rule) => ({ ...rule, principals: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) }))} aria-label="Principals or groups" /></EditorSection>
        <EditorSection label="Which fields"><label className="check-line"><input type="checkbox" checked={props.activeRule.columns.includes(props.selectedField)} onChange={() => props.onToggleField(props.selectedField)} /> Include <strong>{props.selectedField}</strong> in this rule</label><p className="help">Selected parent fields are pruned to granted children. Explicit unauthorized children are rejected.</p></EditorSection>
        <EditorSection label="Which rows"><textarea value={props.activeRule.row_filter ?? ""} onChange={(event) => props.onUpdateRule((rule) => ({ ...rule, row_filter: event.target.value || null }))} aria-label="DuckDB row restriction" placeholder="region = 'US'" /><p className="help">Matching rules combine row restrictions with AND.</p></EditorSection>
        <EditorSection label="How values appear"><MaskEditor mask={props.selectedMask} onChange={props.onMask} field={props.selectedField} /></EditorSection>
        <div className="editor-actions"><button className="secondary" onClick={props.onPreview}>Run policy test</button><button className="primary" onClick={props.onSave}>Save draft</button></div>
      </section>
      <section className="result-panel" aria-label="Effective access inspector">
        <span className="eyebrow">EFFECTIVE ACCESS</span><h2>US analyst</h2><p className="muted">Synthetic persona · not reader authentication</p>
        {props.preview ? <><div className="result-state allowed">Test current</div><h3>Visible output</h3><ul>{props.preview.allowed_columns.map((field) => <li key={field}>{field}{props.preview.masks[field] ? <small> · {props.preview.masks[field].type} mask</small> : null}</li>)}</ul><h3>Row restriction</h3><code>{props.preview.row_filter ?? "No matching row restriction"}</code></> : <div className="empty-result"><strong>Run a policy test</strong><p>See the authorized output schema, masks, and effective row restriction for this draft.</p></div>}
        <div className="result-warning"><strong>Before publishing</strong><p>Review uses the exact saved draft. Changing fields, masks, or row restrictions makes this result stale.</p></div>
      </section>
    </div> : tab === "tests" ? <TestsView onPreview={props.onPreview} preview={props.preview} /> : <HistoryView />}
  </>;
}

function EditorSection({ label, children }: { label: string; children: ReactNode }) { return <section className="editor-section"><h3>{label}</h3>{children}</section>; }

function MaskEditor({ mask, field, onChange }: { mask?: Mask; field: string; onChange: (mask?: Mask) => void }) {
  const option = maskOptions.find((candidate) => candidate.type === mask?.type);
  return <div className="mask-editor"><label htmlFor="mask-type">Mask for <strong>{field}</strong></label><select id="mask-type" value={mask?.type ?? ""} onChange={(event) => { const type = event.target.value as Mask["type"] | ""; onChange(type ? { type } : undefined); }}><option value="">No mask</option>{maskOptions.map((candidate) => <option key={candidate.type} value={candidate.type}>{candidate.label}</option>)}</select>{option?.needsValue && <input aria-label="Mask value" value={mask?.value ?? ""} placeholder={mask?.type === "keep_last" ? "Characters to retain" : "Replacement value"} onChange={(event) => onChange({ ...mask!, value: mask?.type === "keep_last" ? Number(event.target.value) : event.target.value })} />}<p className="help">{mask?.type === "hash" ? "Deterministic pseudonymization; it preserves equality disclosure." : mask?.type === "email" ? "Masks valid email values and nulls malformed values." : "Mask behavior is validated by the control plane before publication."}</p></div>;
}

function TestsView({ onPreview, preview }: { onPreview: () => void; preview: Preview | null }) { return <section className="tests-view"><span className="eyebrow">POLICY TESTS</span><h2>Test before review</h2><p>Simulate a representative persona. This is a policy evaluation, not an impersonated read or a data preview.</p><div className="test-card"><div><strong>US analyst</strong><small>group:us-analysts</small></div><button className="primary" onClick={onPreview}>Run test</button></div>{preview && <div className="result-state allowed">Current: {preview.allowed_columns.length} fields visible</div>}</section>; }
function HistoryView() { return <section className="tests-view"><span className="eyebrow">HISTORY</span><h2>Active policy</h2><p>Published revisions will appear here with an exact diff, publisher attribution, and a restore-as-draft action.</p><div className="empty-result"><strong>No historical revision loaded</strong><p>History is available after connecting this workspace to the control plane.</p></div></section>; }
function ComingSoon({ page }: { page: Page }) { return <section className="coming-soon"><span className="eyebrow">{page.toUpperCase()}</span><h2>{titleFor(page)}</h2><p>This new workspace is being built in vertical slices. Asset authoring is the first functional slice; this destination stays deliberately unavailable until its server contract and authorization checks are implemented.</p></section>; }
function titleFor(page: Page) { return ({ changes: "Changes", activity: "Activity", connections: "Connections", settings: "Settings", assets: "Assets" })[page]; }
function saveLabel(state: SaveState) { return ({ saved: "Saved draft", saving: "Saving draft", unsaved: "Unsaved changes", failed: "Save failed" })[state]; }

createRoot(document.getElementById("root")!).render(<App />);
