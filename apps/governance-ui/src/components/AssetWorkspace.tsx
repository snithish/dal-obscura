import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import type { Asset, AssetAccess, AssetGrant, Mask, PolicyRule, PolicyVersion, PolicyVersionDetail, Preview, SchemaNode, Session } from "../api";
import { controlPlane } from "../api";
import { type AssetTab } from "../navigation";
import { recoveryMessage } from "../recovery";
import { flattenSchemaTree } from "../schema_tree";

type SaveState = "saved" | "saving" | "unsaved" | "failed";

const maskOptions: Array<{ type: Mask["type"]; label: string; needsValue?: boolean }> = [
  { type: "null", label: "Null" }, { type: "redact", label: "Redact", needsValue: true },
  { type: "hash", label: "Hash" }, { type: "email", label: "Email" },
  { type: "keep_last", label: "Keep last", needsValue: true }, { type: "default", label: "Default", needsValue: true },
];

function saveLabel(state: SaveState) { return ({ saved: "Saved draft", saving: "Saving draft", unsaved: "Unsaved changes", failed: "Save failed" })[state]; }

export function AssetWorkspace(props: { initialTab?: AssetTab; initialVersion?: number; assets: Asset[]; asset: Asset; access?: AssetAccess; history: PolicyVersion[]; grants: AssetGrant[]; onAsset: (id: string) => void; assetSearch: string; assetHasMore: boolean; assetInventoryLoading: boolean; onSearch: (value: string) => void; onLoadMore: () => void; rules: PolicyRule[]; activeRule?: PolicyRule; activeRevision: number; selectedRule: number; onRule: (index: number) => void; onMoveRule: (index: number, direction: -1 | 1) => void; selectedField: string; onField: (name: string) => void; selectedMask?: Mask; effectiveFields: Set<string>; saveState: SaveState; notice: string; onToggleField: (name: string) => void; onMask: (mask?: Mask) => void; onUpdateRule: (change: (rule: PolicyRule) => PolicyRule) => void; onAddRule: () => void; onRemoveRule: () => void; onDuplicateRule: (index: number) => void; onUndo: () => void; onRedo: () => void; canUndo: boolean; canRedo: boolean; onSave: () => void; onPreview: () => void; onReview: () => void; previewPrincipal: string; previewGroups: string; previewClaims: string; onPreviewPrincipal: (value: string) => void; onPreviewGroups: (value: string) => void; onPreviewClaims: (value: string) => void; onPublish: () => void; publishing: boolean; onRestore: (version: number) => void; reviewToken?: string; preview: Preview | null; session: Session | null; onReloadAccess: () => void; reviewOnly?: boolean; draftId?: string | null }) {
  const [tab, setTab] = useState<AssetTab>(props.initialTab ?? "policy");
  const [selectedVersion, setSelectedVersion] = useState<number | undefined>(props.initialVersion);
  const [versionDetail, setVersionDetail] = useState<PolicyVersionDetail | null>(null);
  const [versionLoading, setVersionLoading] = useState(false);
  const [versionError, setVersionError] = useState("");
  const versionEpoch = useRef(0);
  const versionAbortController = useRef<AbortController | null>(null);
  const [schemaSearch, setSchemaSearch] = useState("");
  const [conditionsText, setConditionsText] = useState("{}");
  const [conditionsError, setConditionsError] = useState(false);
  useEffect(() => {
    setConditionsText(JSON.stringify(props.activeRule?.when ?? {}, null, 2));
    setConditionsError(false);
  }, [props.asset.id, props.selectedRule, props.activeRule?.when]);
  useEffect(() => setTab(props.initialTab ?? "policy"), [props.asset.id, props.initialTab]);
  useEffect(() => {
    setSelectedVersion(props.initialVersion);
    setVersionDetail(null);
    setVersionError("");
  }, [props.asset.id, props.initialVersion]);
  useEffect(() => {
    const syncLocation = () => {
      const location = new URLSearchParams(window.location.search);
      const rawTab = location.get("tab");
      const nextTab = ["policy", "tests", "history", "access", "consumers"].includes(rawTab ?? "") ? rawTab as AssetTab : "policy";
      const rawVersion = location.get("version");
      const nextVersion = rawVersion && /^\d+$/.test(rawVersion) && Number(rawVersion) > 0 ? Number(rawVersion) : undefined;
      setTab(nextTab);
      setSelectedVersion(nextVersion);
    };
    window.addEventListener("popstate", syncLocation);
    return () => window.removeEventListener("popstate", syncLocation);
  }, []);
  useEffect(() => {
    const history = props.history.filter((item) => item.asset_id === props.asset.id);
    const version = selectedVersion && history.some((item) => item.policy_version === selectedVersion) ? selectedVersion : undefined;
    versionAbortController.current?.abort();
    if (tab !== "history" || version === undefined) {
      setVersionLoading(false);
      setVersionDetail(null);
      setVersionError("");
      return;
    }
    const epoch = ++versionEpoch.current;
    const controller = new AbortController();
    versionAbortController.current = controller;
    setVersionLoading(true);
    setVersionError("");
    void controlPlane.getPolicyVersion(props.asset.id, version, controller.signal).then((detail) => {
      if (epoch === versionEpoch.current) setVersionDetail(detail);
    }).catch((error) => {
      if (controller.signal.aborted || epoch !== versionEpoch.current) return;
      setVersionDetail(null);
      setVersionError((error as { status?: number })?.status === 404 ? "That policy version is no longer available." : "Policy version details could not be loaded.");
    }).finally(() => {
      if (epoch === versionEpoch.current) setVersionLoading(false);
      if (controller === versionAbortController.current) versionAbortController.current = null;
    });
    return () => controller.abort();
  }, [props.asset.id, props.history, selectedVersion, tab]);
  const fields = props.asset.schema?.fields ?? props.asset.schema_fields.map((field, index) => ({ field_id: index, name: field.name, path: { version: 1, segments: [{ kind: "field" as const, name: field.name, field_id: index }] }, human_path: field.name, type: field.type, nullable: field.nullable, kind: "scalar" as const }));
  const visibleFields = useMemo(() => filterSchemaNodes(fields, schemaSearch), [fields, schemaSearch]);
  const currentPreview = props.preview;
  const canEdit = Boolean(props.access?.capabilities.some((item) => item.capability === "edit" && item.allowed));
  const canPublish = Boolean(props.access?.capabilities.some((item) => item.capability === "publish" && item.allowed));
  const readOnly = Boolean(props.reviewOnly || !canEdit);
  async function copyReviewLink() {
    if (!props.draftId) return;
   const url = new URL(window.location.href);
    const params = new URLSearchParams(url.search);
    params.set("asset", props.asset.id);
    params.set("draft", props.draftId);
    if (tab === "policy") params.delete("tab"); else params.set("tab", tab);
    url.search = params.toString();
   url.hash = "assets";
    try { await navigator.clipboard.writeText(url.toString()); } catch { /* clipboard is optional */ }
  }
  const listedAssets = props.assets.some((item) => item.id === props.asset.id) ? props.assets : [props.asset, ...props.assets];
  function selectTab(next: AssetTab) {
    setTab(next);
    const params = new URLSearchParams(window.location.search);
    if (next === "policy") params.delete("tab"); else params.set("tab", next);
    const query = params.toString();
    window.history.pushState(null, "", `${window.location.pathname}${query ? `?${query}` : ""}#assets`);
  }
  function selectVersion(version: number) {
    setSelectedVersion(version);
    if (tab !== "history") setTab("history");
    const params = new URLSearchParams(window.location.search);
    params.set("asset", props.asset.id);
    params.set("tab", "history");
    params.set("version", String(version));
    const query = params.toString();
    window.history.pushState(null, "", `${window.location.pathname}${query ? `?${query}` : ""}#assets`);
  }
  return <><div className="asset-summary"><div className="asset-picker"><label htmlFor="asset-search">Find governed asset<input id="asset-search" type="search" value={props.assetSearch} onChange={(event) => props.onSearch(event.target.value)} placeholder="Search catalog or asset" disabled={readOnly} /></label><label htmlFor="asset-select">Selected asset<select id="asset-select" value={props.asset.id} onChange={(event) => props.onAsset(event.target.value)} disabled={readOnly}>{listedAssets.map((item) => <option key={item.id} value={item.id}>{item.catalog} / {item.name}</option>)}</select></label><div className="asset-page-actions"><small>{props.assets.length} loaded</small>{props.assetHasMore && <button className="secondary compact" disabled={props.assetInventoryLoading} onClick={props.onLoadMore}>{props.assetInventoryLoading ? "Loading…" : "Load more"}</button>}{props.assetInventoryLoading && <span className="muted" role="status">Updating inventory…</span>}</div></div><div className="save-status" aria-live="polite"><span className={"save-dot " + props.saveState} /> {saveLabel(props.saveState)}</div></div><div className="notice" role="status">{props.notice}</div>{props.draftId && !readOnly && <button className="secondary compact" onClick={() => void copyReviewLink()}>Copy review link</button>}<div className="asset-tabs" role="tablist" aria-label="Asset views">{(["policy", "tests", "history", "access", "consumers"] as const).map((item) => <button key={item} role="tab" aria-selected={tab === item} className={tab === item ? "selected" : ""} onClick={() => selectTab(item)}>{item === "policy" ? "Policy" : item === "tests" ? "Tests" : item === "history" ? "History" : item === "access" ? "Access" : "Consumers"}</button>)}</div>{tab === "policy" ? <div className="studio">
    <section className="schema-panel" aria-label="Schema and field selection"><div className="panel-head"><div><span className="eyebrow">SCHEMA</span><h2>Fields & access</h2></div></div><p className="help">Fields retain server-defined paths. A checked field is visible through the selected rule.</p>{props.asset.schema?.stable_field_ids === false && <p className="schema-drift-warning" role="status"><strong>Schema identity requires reapproval</strong><br />This adapter does not provide stable field IDs. Any schema change must be reviewed again before access is served.</p>}<label className="schema-search">Search fields<input type="search" value={schemaSearch} onChange={(event) => setSchemaSearch(event.target.value)} placeholder="name or nested path" /></label>{visibleFields.length ? <VirtualSchemaTree nodes={visibleFields} selectedField={props.selectedField} effectiveFields={props.effectiveFields} onField={readOnly ? () => undefined : props.onField} forceExpanded={Boolean(schemaSearch)} /> : <div className="empty-result"><strong>No matching fields</strong><p>Clear the search to browse the authoritative Iceberg schema.</p></div>}<div className="schema-note"><strong>Nested fields</strong><p>Struct, list, and map paths come from the control plane. Collection nodes expose explicit <code>$element</code>, <code>$key</code>, and <code>$value</code> segments.</p></div></section>
    <section className="editor-panel" aria-label="Policy rule editor"><div className="panel-head"><div><span className="eyebrow">POLICY RULES</span><h2>{props.activeRule ? "Rule " + (props.selectedRule + 1) : "No rule selected"}</h2></div><div className="card-actions"><button className="secondary compact" onClick={props.onUndo} disabled={readOnly || !props.canUndo} aria-label="Undo policy edit">Undo</button><button className="secondary compact" onClick={props.onRedo} disabled={readOnly || !props.canRedo} aria-label="Redo policy edit">Redo</button><button className="text-button" onClick={props.onAddRule} disabled={readOnly}>Add rule</button></div></div>{props.rules.length ? <><div className="rule-list" aria-label="Policy rule list">{props.rules.map((rule, index) => <div className="rule-row" key={index}><button className={index === props.selectedRule ? "selected" : ""} onClick={() => props.onRule(index)}>Rule {index + 1}<small>{rule.principals.join(", ") || "No principal"}</small></button><div className="rule-order"><button className="secondary compact" type="button" aria-label={`Duplicate rule ${index + 1}`} onClick={() => props.onDuplicateRule(index)} disabled={readOnly}>Duplicate</button><button className="secondary compact" type="button" aria-label={`Move rule ${index + 1} up`} onClick={() => props.onMoveRule(index, -1)} disabled={readOnly || index === 0}>↑</button><button className="secondary compact" type="button" aria-label={`Move rule ${index + 1} down`} onClick={() => props.onMoveRule(index, 1)} disabled={readOnly || index === props.rules.length - 1}>↓</button></div></div>)}</div><EditorSection label="Who"><input value={props.activeRule?.principals.join(", ") ?? ""} onChange={(event) => props.onUpdateRule((rule) => ({ ...rule, principals: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) }))} aria-label="Principals or groups" placeholder="group:us-analysts" disabled={readOnly} /></EditorSection><EditorSection label="Conditions"><ConditionBuilder value={props.activeRule?.when} disabled={readOnly} onChange={(when) => props.onUpdateRule((rule) => ({ ...rule, when }))} /><details className="advanced-json"><summary>Advanced JSON</summary><textarea value={conditionsText} onChange={(event) => { const text = event.target.value; setConditionsText(text); try { const parsed = JSON.parse(text); if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") throw new Error("object required"); setConditionsError(false); props.onUpdateRule((rule) => ({ ...rule, when: parsed as Record<string, string | string[]> })); } catch { setConditionsError(true); } }} aria-invalid={conditionsError} aria-label="Advanced principal conditions JSON" placeholder='{"region":"us"}' disabled={readOnly} />{conditionsError && <p className="auth-error" role="alert">Conditions must be a JSON object before saving.</p>}<p className="help">Advanced JSON remains lossless for claims supported by the control plane.</p></details></EditorSection><EditorSection label="Which fields"><label className="check-line"><input type="checkbox" checked={props.activeRule?.columns.includes(props.selectedField) ?? false} disabled={readOnly || !props.selectedField} onChange={() => props.onToggleField(props.selectedField)} /> Include <strong>{props.selectedField || "a schema field"}</strong></label><p className="help">Parent/child conflicts and invalid nested paths are rejected by the control plane.</p></EditorSection><EditorSection label="Which rows"><textarea value={props.activeRule?.row_filter ?? ""} onChange={(event) => props.onUpdateRule((rule) => ({ ...rule, row_filter: event.target.value || null }))} aria-label="DuckDB row restriction" placeholder="region = 'US'" disabled={readOnly} /><p className="help">Matching rules combine row restrictions with AND.</p></EditorSection><EditorSection label="How values appear"><MaskEditor mask={props.selectedMask} onChange={props.onMask} field={props.selectedField} /></EditorSection><div className="editor-actions"><button className="danger" onClick={props.onRemoveRule} disabled={readOnly}>Remove rule</button><button className="secondary" onClick={props.onPreview}>Run policy test</button><button className="secondary" onClick={props.onReview} disabled={props.saveState !== "saved" || props.publishing}>{props.reviewToken ? "Review current" : "Review for publish"}</button><button className="primary" onClick={props.onSave} disabled={readOnly || props.saveState === "saving" || conditionsError}>{props.saveState === "saving" ? "Saving…" : "Save draft"}</button><button className="secondary" onClick={props.onPublish} disabled={!canPublish || props.saveState !== "saved" || !props.reviewToken || props.publishing}>{props.publishing ? "Publishing…" : "Publish reviewed draft"}</button></div></> : <div className="empty-result"><strong>No draft rules</strong><p>This is an intentional deny-all policy. Save it, test it, and complete server review before publishing.</p><div className="editor-actions"><button className="secondary" onClick={props.onPreview}>Run policy test</button><button className="secondary" onClick={props.onReview} disabled={props.saveState !== "saved" || props.publishing}>Review for publish</button><button className="primary" onClick={props.onSave} disabled={readOnly || props.saveState === "saving"}>{props.saveState === "saving" ? "Saving…" : "Save deny-all draft"}</button><button className="secondary" onClick={props.onPublish} disabled={!canPublish || props.saveState !== "saved" || !props.reviewToken || props.publishing}>{props.publishing ? "Publishing…" : "Publish reviewed deny-all"}</button></div><button className="primary" onClick={props.onAddRule} disabled={readOnly}>Add first rule</button></div>}</section>
    <section className="result-panel" aria-label="Effective access inspector"><span className="eyebrow">EFFECTIVE ACCESS</span><h2>{props.previewPrincipal || "Synthetic persona"}</h2><p className="muted">{props.previewGroups ? `Groups: ${props.previewGroups}` : "No groups"} · not reader authentication</p>{currentPreview ? <><div className={"result-state " + (currentPreview.decision === "deny" ? "denied" : "allowed")}>Test {currentPreview.decision === "deny" ? "denied" : "allowed"}</div><h3>Visible output</h3><ul>{currentPreview.allowed_columns.map((field) => <li key={field}>{field}{currentPreview.masks[field] && <small> · {currentPreview.masks[field].type} mask</small>}</li>)}</ul><h3>Row restriction</h3><code>{currentPreview.row_filter ?? "No matching row restriction"}</code></> : <div className="empty-result"><strong>Run a policy test</strong><p>See authorized output schema, masks, and row restriction for this draft.</p></div>}<div className="result-warning">{props.preview?.review_draft_author && <p><strong>Draft author:</strong> <code>{props.preview.review_draft_author}</code><br /><strong>Reviewer:</strong> <code>{props.preview.reviewer ?? "current session"}</code></p>}<strong>Before publishing</strong><p>Review uses the exact saved draft. Changing fields, masks, or row restrictions makes this result stale.</p></div></section>
  </div> : tab === "tests" ? <TestsView onPreview={props.onPreview} preview={props.preview} principal={props.previewPrincipal} groups={props.previewGroups} claims={props.previewClaims} onPrincipal={props.onPreviewPrincipal} onGroups={props.onPreviewGroups} onClaims={props.onPreviewClaims} /> : tab === "history" ? <HistoryView history={props.history.filter((item) => item.asset_id === props.asset.id)} selectedVersion={selectedVersion} versionDetail={versionDetail} versionLoading={versionLoading} versionError={versionError} onSelectVersion={selectVersion} onRestore={props.onRestore} disabled={props.saveState === "saving" || readOnly} /> : tab === "access" ? <AccessView asset={props.asset} access={props.access} grants={props.grants} session={props.session} onReload={props.onReloadAccess} /> : <ConsumerView asset={props.asset} />}</>;
}

const TREE_ROW_HEIGHT = 42;
const TREE_VIEWPORT_HEIGHT = 504;
const TREE_OVERSCAN = 8;

function VirtualSchemaTree({ nodes, selectedField, effectiveFields, onField, forceExpanded = false }: { nodes: SchemaNode[]; selectedField: string; effectiveFields: Set<string>; onField: (name: string) => void; forceExpanded?: boolean }) {
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set(nodes.filter((node) => node.children?.length).map((node) => node.human_path)));
  const [scrollTop, setScrollTop] = useState(0);
  const viewportRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    setExpanded(new Set(nodes.filter((node) => node.children?.length).map((node) => node.human_path)));
    setScrollTop(0);
  }, [nodes]);
  const flattened = useMemo(() => flattenSchemaTree(nodes, expanded, forceExpanded), [expanded, forceExpanded, nodes]);
  const first = Math.max(0, Math.floor(scrollTop / TREE_ROW_HEIGHT) - TREE_OVERSCAN);
  const last = Math.min(flattened.length, Math.ceil((scrollTop + TREE_VIEWPORT_HEIGHT) / TREE_ROW_HEIGHT) + TREE_OVERSCAN);
  const windowed = flattened.slice(first, last);
  const toggle = (path: string) => setExpanded((current) => {
    const next = new Set(current);
    if (next.has(path)) next.delete(path); else next.add(path);
    return next;
  });
  const focusIndex = (index: number) => {
    const viewport = viewportRef.current;
    if (!viewport) return;
    const targetTop = index * TREE_ROW_HEIGHT;
    const visibleTop = viewport.scrollTop;
    const visibleBottom = visibleTop + TREE_VIEWPORT_HEIGHT;
    if (targetTop < visibleTop || targetTop + TREE_ROW_HEIGHT > visibleBottom) {
      viewport.scrollTo({ top: Math.max(0, targetTop - (TREE_VIEWPORT_HEIGHT - TREE_ROW_HEIGHT) / 2) });
    }
    window.requestAnimationFrame(() => document.querySelector<HTMLElement>(`[data-schema-index="${index}"]`)?.focus());
  };
  return <div ref={viewportRef} className="field-tree-viewport" role="tree" aria-label="Schema fields" aria-setsize={flattened.length} style={{ maxHeight: TREE_VIEWPORT_HEIGHT }} onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}><div className="field-tree-window" style={{ height: flattened.length * TREE_ROW_HEIGHT }}>{windowed.map(({ node, depth, index }) => {
    const hasChildren = Boolean(node.children?.length);
    const isExpanded = hasChildren && (forceExpanded || expanded.has(node.human_path));
    return <div className="field-tree-item" key={node.human_path} data-schema-index={index} role="treeitem" aria-level={depth + 1} aria-posinset={index + 1} aria-setsize={flattened.length} aria-expanded={hasChildren ? isExpanded : undefined} style={{ top: index * TREE_ROW_HEIGHT }} tabIndex={selectedField === node.human_path ? 0 : -1} onKeyDown={(event) => {
      if (event.key === "ArrowRight" && hasChildren && !isExpanded) { event.preventDefault(); toggle(node.human_path); }
      else if (event.key === "ArrowLeft" && hasChildren && isExpanded && !forceExpanded) { event.preventDefault(); toggle(node.human_path); }
      else if (event.key === "ArrowDown" && index < flattened.length - 1) { event.preventDefault(); focusIndex(index + 1); }
      else if (event.key === "ArrowUp" && index > 0) { event.preventDefault(); focusIndex(index - 1); }
      else if (event.key === "Enter" || event.key === " ") { event.preventDefault(); onField(node.human_path); }
    }}><div className="field-row-wrap">{hasChildren ? <button className="tree-toggle" type="button" aria-label={`${isExpanded ? "Collapse" : "Expand"} ${node.human_path}`} aria-expanded={isExpanded} onClick={() => toggle(node.human_path)}>{isExpanded ? "▾" : "▸"}</button> : <span className="tree-toggle spacer" aria-hidden="true" /> }<button className={selectedField === node.human_path ? "field-row selected" : "field-row"} style={{ paddingLeft: `${8 + depth * 16}px` }} onClick={() => onField(node.human_path)} aria-label={`Select ${node.human_path}`}><span className="field-name">{node.name}</span><span className="field-type">{node.type}{node.nullable ? " · nullable" : ""}</span>{effectiveFields.has(node.human_path) && <span className="grant">Granted</span>}</button></div></div>;
  })}</div></div>;
}

function filterSchemaNodes(nodes: SchemaNode[], search: string): SchemaNode[] {
  const normalized = search.trim().toLowerCase();
  if (!normalized) return nodes;
  const visit = (node: SchemaNode): SchemaNode | null => {
    const children = (node.children ?? []).map(visit).filter((child): child is SchemaNode => child !== null);
    if (node.human_path.toLowerCase().includes(normalized) || node.name.toLowerCase().includes(normalized)) {
      return { ...node, children: node.children };
    }
    return children.length ? { ...node, children } : null;
  };
  return nodes.map(visit).filter((node): node is SchemaNode => node !== null);
}

function EditorSection({ label, children }: { label: string; children: ReactNode }) { return <section className="editor-section"><h3>{label}</h3>{children}</section>; }
function MaskEditor({ mask, field, onChange }: { mask?: Mask; field: string; onChange: (mask?: Mask) => void }) {
  const option = maskOptions.find((candidate) => candidate.type === mask?.type);
  return <div className="mask-editor"><label htmlFor="mask-type">Mask for <strong>{field || "selected field"}</strong></label><select id="mask-type" value={mask?.type ?? ""} disabled={!field} onChange={(event) => {
    const type = event.target.value as Mask["type"] | "";
    if (!type) return onChange(undefined);
    const selected = maskOptions.find((candidate) => candidate.type === type);
    const value = selected?.needsValue ? (type === "redact" ? "[REDACTED]" : type === "keep_last" ? 4 : "") : undefined;
    onChange({ type, ...(selected?.needsValue ? { value } : {}) });
  }}><option value="">No mask</option>{maskOptions.map((candidate) => <option key={candidate.type} value={candidate.type}>{candidate.label}</option>)}</select>{option?.needsValue && <input aria-label="Mask value" type={mask?.type === "keep_last" ? "number" : "text"} min={mask?.type === "keep_last" ? 0 : undefined} step={mask?.type === "keep_last" ? 1 : undefined} inputMode={mask?.type === "keep_last" ? "numeric" : undefined} value={mask?.value === undefined || mask.value === null ? "" : String(mask.value)} placeholder={mask?.type === "keep_last" ? "Characters to retain" : "Text or JSON scalar"} onChange={(event) => { if (mask?.type === "keep_last") { const value = Number(event.target.value); if (Number.isInteger(value) && value >= 0) onChange({ ...mask, value }); return; } const raw = event.target.value; try { const parsed = JSON.parse(raw); onChange({ ...mask!, value: parsed === null || ["string", "number", "boolean"].includes(typeof parsed) ? parsed : raw }); } catch { onChange({ ...mask!, value: raw }); } }} />}<p className="help">Mask behavior is validated by the control plane before publication.</p></div>;
}
function TestsView({ onPreview, preview, principal, groups, claims, onPrincipal, onGroups, onClaims }: { onPreview: () => void; preview: Preview | null; principal: string; groups: string; claims: string; onPrincipal: (value: string) => void; onGroups: (value: string) => void; onClaims: (value: string) => void }) { return <section className="tests-view"><span className="eyebrow">POLICY TESTS</span><h2>Test before review</h2><p>Simulate a representative persona. This is a policy evaluation, not an impersonated read or data preview.</p><div className="test-card persona-form"><label>Principal<input value={principal} onChange={(event) => onPrincipal(event.target.value)} placeholder="user:analyst@example.com" /></label><label>Groups<input value={groups} onChange={(event) => onGroups(event.target.value)} placeholder="us-analysts, finance" /></label><label>Claims (JSON)<textarea value={claims} onChange={(event) => onClaims(event.target.value)} aria-label="Synthetic persona claims" placeholder='{"region":"us"}' /></label><button className="primary" onClick={onPreview}>Run test</button></div>{preview && <div className={"result-state " + (preview.decision === "deny" ? "denied" : "allowed")}>Current test: {preview.decision === "deny" ? "denied" : `${preview.allowed_columns.length} fields visible`}</div>}</section>; }
function ConditionBuilder({ value, disabled, onChange }: { value?: Record<string, string | string[]>; disabled: boolean; onChange: (value: Record<string, string | string[]>) => void }) {
  const [rows, setRows] = useState<Array<{ key: string; mode: "equals" | "one_of"; value: string }>>(() => conditionRows(value));
  const [error, setError] = useState("");
  useEffect(() => { setRows(conditionRows(value)); setError(""); }, [value]);
  function commit(next: Array<{ key: string; mode: "equals" | "one_of"; value: string }>) {
    if (next.some((row) => !row.key.trim())) { setError("Every condition needs a claim name."); return; }
    const result: Record<string, string | string[]> = {};
    for (const row of next) {
      const values = row.value.split(",").map((item) => item.trim()).filter(Boolean);
      if (!values.length) { setError("Every condition needs a value."); return; }
      result[row.key.trim()] = row.mode === "one_of" ? values : values[0];
    }
    setError(""); onChange(result);
  }
  function update(index: number, change: Partial<{ key: string; mode: "equals" | "one_of"; value: string }>) {
    const next = rows.map((row, rowIndex) => rowIndex === index ? { ...row, ...change } : row);
    setRows(next); commit(next);
  }
  return <div className="condition-builder"><p className="help">Match authenticated claims with AND semantics. Values are text or a comma-separated list.</p>{rows.map((row, index) => <div className="condition-row" key={index}><input aria-label={`Condition claim ${index + 1}`} value={row.key} onChange={(event) => update(index, { key: event.target.value })} placeholder="region" disabled={disabled} /><select aria-label={`Condition operator ${index + 1}`} value={row.mode} onChange={(event) => update(index, { mode: event.target.value as "equals" | "one_of" })} disabled={disabled}><option value="equals">equals</option><option value="one_of">is one of</option></select><input aria-label={`Condition value ${index + 1}`} value={row.value} onChange={(event) => update(index, { value: event.target.value })} placeholder={row.mode === "one_of" ? "us, eu" : "us"} disabled={disabled} /><button className="danger compact" type="button" onClick={() => { const next = rows.filter((_item, rowIndex) => rowIndex !== index); setRows(next); commit(next); }} disabled={disabled}>Remove</button></div>)}<button className="secondary compact" type="button" onClick={() => setRows((current) => [...current, { key: "", mode: "equals", value: "" }])} disabled={disabled}>Add condition</button>{error && <p className="auth-error" role="alert">{error}</p>}</div>;
}
function conditionRows(value?: Record<string, string | string[]>): Array<{ key: string; mode: "equals" | "one_of"; value: string }> { return Object.entries(value ?? {}).map(([key, raw]) => ({ key, mode: Array.isArray(raw) ? "one_of" : "equals", value: Array.isArray(raw) ? raw.join(", ") : raw })); }
function HistoryView({ history, selectedVersion, versionDetail, versionLoading, versionError, onSelectVersion, onRestore, disabled }: { history: PolicyVersion[]; selectedVersion?: number; versionDetail: PolicyVersionDetail | null; versionLoading: boolean; versionError: string; onSelectVersion: (version: number) => void; onRestore: (version: number) => void; disabled: boolean }) { return <section className="tests-view"><span className="eyebrow">HISTORY</span><h2>Published policy revisions</h2><p>Immutable versions returned by the control plane. Restore creates a new editable draft and never changes published history.</p>{history.length ? <div className="table-wrap"><table><thead><tr><th>Version</th><th>State</th><th>Created</th><th>Action</th></tr></thead><tbody>{history.map((item) => <tr key={item.policy_version} className={selectedVersion === item.policy_version ? "selected-row" : undefined}><td><button className="link-button" onClick={() => onSelectVersion(item.policy_version)} aria-label={`View policy version ${item.policy_version}`}><code>{item.policy_version}</code></button></td><td>{item.active ? "Active" : "Published"}</td><td>{new Date(item.created_at).toLocaleString()}</td><td><button className="secondary compact" onClick={() => onSelectVersion(item.policy_version)}>View details</button><button className="secondary compact" disabled={disabled} onClick={() => onRestore(item.policy_version)}>Restore to draft</button></td></tr>)}</tbody></table></div> : <div className="empty-result"><strong>No historical revision loaded</strong><p>Publish a reviewed draft to create the first immutable revision.</p></div>}{selectedVersion !== undefined && <div className="form-card version-detail" aria-live="polite"><h3>Version {selectedVersion} details</h3>{versionLoading ? <p role="status">Loading immutable policy rules…</p> : versionError ? <p className="auth-error" role="alert">{versionError}</p> : versionDetail ? <><p className="help">This is the server snapshot used for the published revision. Rules: {versionDetail.rules.length}.</p><div className="table-wrap"><table><thead><tr><th>Rule</th><th>Principals</th><th>Fields</th><th>Rows</th></tr></thead><tbody>{versionDetail.rules.map((rule, index) => <tr key={`${rule.ordinal}-${index}`}><td>{index + 1}</td><td>{rule.principals.join(", ") || "No principal"}</td><td>{rule.columns.join(", ") || "No fields"}</td><td><code>{rule.row_filter ?? "None"}</code></td></tr>)}</tbody></table></div></> : <p className="muted">Choose a published version to inspect its immutable rules.</p>}</div>}</section>; }
function ConsumerView({ asset }: { asset: Asset }) {
  const catalog = JSON.stringify(asset.catalog);
  const target = JSON.stringify(asset.name);
  const python = `import os\nfrom dal_obscura.connectors import DalObscuraClient\n\nwith DalObscuraClient(os.environ["DAL_OBSCURA_FLIGHT_URI"],\n                     auth_token=lambda: os.environ["DAL_OBSCURA_TOKEN"]) as client:\n    table = client.read_table(\n        catalog=${catalog},\n        target=${target},\n        columns=["*"],\n    )`;
  const duckdb = `import os\nfrom dal_obscura.connectors import DalObscuraClient, DuckDBDalObscuraReader\n\nwith DalObscuraClient(os.environ["DAL_OBSCURA_FLIGHT_URI"],\n                     auth_token=lambda: os.environ["DAL_OBSCURA_TOKEN"]) as client:\n    reader = DuckDBDalObscuraReader(client)\n    relation = reader.relation(\n        catalog=${catalog},\n        target=${target},\n        columns=["*"],\n    )\n    relation.show()`;
  const spark = `import os\n\nspark.read.format("dal_obscura") \\\n    .option("dal.uri", os.environ["DAL_OBSCURA_FLIGHT_URI"]) \\\n    .option("dal.catalog", ${catalog}) \\\n    .option("dal.target", ${target}) \\\n    .option("dal.auth.token-env", "DAL_OBSCURA_TOKEN") \\\n    .option("dal.executor.auth.token-env", "DAL_OBSCURA_TOKEN") \\\n    .load()`;
  const arrow = `import os\nimport pyarrow.flight as flight\nfrom dal_obscura.connectors import DalObscuraClient\n\nwith DalObscuraClient(os.environ["DAL_OBSCURA_FLIGHT_URI"],\n                     auth_token=lambda: os.environ["DAL_OBSCURA_TOKEN"]) as client:\n    for batch in client.read_batches(\n        catalog=${catalog}, target=${target}, columns=["*"]\n    ):\n        consume(batch)`;
  return <section className="consumer-view"><div className="management-head"><div><span className="eyebrow">CONSUMERS</span><h2>Read this governed asset</h2><p className="muted">Use the same Arrow Flight contract from Python, DuckDB, Spark, or another Arrow consumer. Credentials stay in environment or workload identity configuration.</p></div></div><div className="consumer-notice"><strong>Use a TLS Flight URI in production.</strong><span>Set <code>DAL_OBSCURA_FLIGHT_URI</code> and <code>DAL_OBSCURA_TOKEN</code> outside source control. The snippets request the governed logical target and receive only authorized nested fields and masks.</span></div><div className="consumer-grid"><CopyableCode title="Python / PyArrow" code={python} /><CopyableCode title="DuckDB relation" code={duckdb} /><CopyableCode title="Spark 3.x" code={spark} /><CopyableCode title="Raw Arrow batches" code={arrow} /></div></section>;
}

function CopyableCode({ title, code }: { title: string; code: string }) {
  const [copied, setCopied] = useState(false);
  async function copy() {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1_500);
    } catch {
      setCopied(false);
    }
  }
  return <article className="consumer-card"><div className="consumer-card-head"><h3>{title}</h3><button className="secondary compact" onClick={() => void copy()}>{copied ? "Copied" : "Copy"}</button></div><pre><code>{code}</code></pre></article>;
}

function AccessView({ asset, access, grants, session, onReload }: { asset: Asset; access?: AssetAccess; grants: AssetGrant[]; session: Session | null; onReload: () => void }) {
  const [owners, setOwners] = useState(asset.owners.join(", "));
  const [rows, setRows] = useState<AssetGrant[]>(grants);
  const [message, setMessage] = useState("");
  useEffect(() => { setOwners(asset.owners.join(", ")); setRows(grants); }, [asset, grants]);
  const canManageOwners = Boolean(session?.platform_admin);
  const canManageGrants = Boolean(session?.platform_admin || access?.capabilities.some((item) => item.capability === "grant" && item.allowed));
  async function saveOwners() {
    if (!canManageOwners) return setMessage("Only a platform administrator can change owners.");
    try { await controlPlane.saveOwners(asset.id, owners.split(",").map((value) => value.trim()).filter(Boolean), asset.revision); setMessage("Owners updated. Existing drafts and publications are unchanged."); onReload(); } catch (error) { setMessage(recoveryMessage(error, "Owner update was rejected; refresh before retrying.")); }
  }
  async function saveGrants() {
    if (!canManageGrants) return setMessage("Only an actor with grant-management capability can change delegated access.");
    const normalized = rows.filter((grant) => grant.principal.trim()).map((grant) => ({ ...grant, principal: grant.principal.trim() }));
    try { await controlPlane.saveGrants(asset.id, normalized, asset.revision); setRows(normalized); setMessage("Delegated capabilities updated. Changes take effect on the next authorized request."); onReload(); } catch (error) { setMessage(recoveryMessage(error, "Capability update was rejected; refresh before retrying.")); }
  }
  const identityHint = session?.issuer ? `Federated identities use the exact issuer ${session.issuer}|subject and ${session.issuer}|group:name.` : "Federated identities should use the exact issuer|subject form so identical subjects from different providers stay isolated.";
  return <section className="management-view access-view"><div className="management-head"><div><span className="eyebrow">ACCESS</span><h2>Owners and delegated capabilities</h2><p className="muted">Owners receive read and edit scope. Publication and grant management are explicit capabilities enforced by the control plane.</p></div><button className="secondary" onClick={onReload}>Refresh</button></div>{access && <div className="form-card"><h3>Your effective capabilities</h3><p className="help">Calculated by the control plane for <strong>{access.principal}</strong>. A denied capability remains unavailable even when a control is visible.</p><div className="capability-grid">{access.capabilities.map((item) => <div className={item.allowed ? "capability-card allowed" : "capability-card denied"} key={item.capability}><strong>{item.capability}</strong><span>{item.allowed ? "Allowed" : "Not granted"}</span><small>{item.reasons.length ? item.reasons.join(" · ") : "No matching owner or delegated grant"}</small></div>)}</div></div>}<div className="form-card"><h3>Owners</h3><label className="form-label">Owner principals<input value={owners} onChange={(event) => setOwners(event.target.value)} placeholder="user:owner@example.com, group:data-stewards" disabled={!canManageOwners} /></label><p className="help">Comma-separated user or group principals. Removing the last owner is blocked while the asset is not safely reassigned. {identityHint}</p><button className="primary" onClick={() => void saveOwners()} disabled={!canManageOwners}>Save owners</button></div><div className="form-card"><h3>Delegated capabilities</h3>{rows.length ? <div className="grant-editor">{rows.map((grant, index) => <div className="grant-row" key={`${grant.principal}-${grant.capability}-${index}`}><input aria-label={`Grant principal ${index + 1}`} value={grant.principal} disabled={!canManageGrants} onChange={(event) => setRows((current) => current.map((item, row) => row === index ? { ...item, principal: event.target.value } : item))} placeholder="user:analyst@example.com" /><select aria-label={`Grant capability ${index + 1}`} value={grant.capability} disabled={!canManageGrants} onChange={(event) => setRows((current) => current.map((item, row) => row === index ? { ...item, capability: event.target.value as AssetGrant["capability"] } : item))}><option value="read">Read</option><option value="edit">Edit</option><option value="publish">Publish</option><option value="grant">Grant management</option></select><button className="danger" disabled={!canManageGrants} onClick={() => setRows((current) => current.filter((_, row) => row !== index))}>Remove</button></div>)}</div> : <p className="muted">No explicit delegated capabilities. Owners need explicit publish or grant-management assignments for those actions.</p>}<div className="editor-actions"><button className="secondary" disabled={!canManageGrants} onClick={() => setRows((current) => [...current, { principal: "", capability: "read" }])}>Add capability</button><button className="primary" disabled={!canManageGrants} onClick={() => void saveGrants()}>Save capabilities</button></div>{message && <p className="notice" role="status">{message}</p>}</div></section>;
}
