import { useEffect, useRef, useState } from "react";
import type { Catalog, CatalogDiagnostic, PluginDescriptor, PluginPair, PluginState, WorkspacePublication } from "../api";
import { controlPlane } from "../api";
import { recoveryMessage } from "../recovery";

type PluginConfigField = { name: string; type: string; required: boolean; secret: boolean; options?: string[] };

const defaultCatalogFields: PluginConfigField[] = [
  { name: "uri", type: "string", required: true, secret: false },
  { name: "warehouse", type: "string", required: false, secret: false },
  { name: "user", type: "string", required: false, secret: false },
  { name: "password", type: "secret_reference", required: false, secret: true },
];

function configFields(plugin?: PluginDescriptor): PluginConfigField[] {
  const raw = plugin?.config_schema?.fields;
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return [];
    const value = item as Record<string, unknown>;
    const name = typeof value.name === "string" ? value.name.trim() : "";
    const type = typeof value.type === "string" ? value.type : "string";
    if (!name || name.length > 128 || !/^[A-Za-z][A-Za-z0-9_.-]*$/.test(name)) return [];
    const options = Array.isArray(value.options) ? value.options.filter((option): option is string => typeof option === "string" && option.length <= 128) : undefined;
    return [{ name, type, required: value.required === true, secret: value.secret === true || type === "secret_reference", options }];
  });
}

function configFieldLabel(name: string): string {
  if (name === "uri") return "SQL catalog URI";
  if (name === "user") return "Database username";
  if (name === "password") return "Password secret reference";
  return name.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function pluginIdForCatalog(catalog: Catalog, catalogPlugins: PluginDescriptor[]): string {
  if (catalog.module === "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog") {
    return "iceberg.sql";
  }
  return catalogPlugins.some((plugin) => plugin.plugin_id === catalog.module)
    ? catalog.module
    : (catalogPlugins[0]?.plugin_id ?? "iceberg.sql");
}

function safeFormValue(value: unknown, field: PluginConfigField): string {
  if (field.secret) {
    // Secret options are references, never resolved credentials. Keep the
    // reference editable while refusing to render arbitrary nested values.
    if (value && typeof value === "object" && !Array.isArray(value)) {
      const reference = (value as Record<string, unknown>).secret;
      return typeof reference === "string" ? reference : "";
    }
    return "";
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  return "";
}

function formConfigForCatalog(catalog: Catalog, plugin: PluginDescriptor | undefined, pluginId: string): Record<string, string> {
  const fields = configFields(plugin);
  const effective = fields.length ? fields : (pluginId === "iceberg.sql" ? defaultCatalogFields : []);
  const options = catalog.options ?? {};
  return Object.fromEntries(effective.map((field) => [field.name, safeFormValue(options[field.name], field)]));
}

export type ConnectionsViewProps = {
  catalogs: Catalog[];
  publications: WorkspacePublication[];
  plugins: PluginDescriptor[];
  pluginStates: PluginState[];
  pluginPairs: PluginPair[];
  canActivate: boolean;
  onReload: () => void;
};

export function ConnectionsView({ catalogs, publications, plugins, pluginStates, pluginPairs, canActivate, onReload }: ConnectionsViewProps) {
  const [name, setName] = useState("");
  const [editingCatalog, setEditingCatalog] = useState<Catalog | null>(null);
  const catalogPlugins = plugins.filter((plugin) => plugin.kind === "catalog");
  const [pluginId, setPluginId] = useState(catalogPlugins[0]?.plugin_id ?? "iceberg.sql");
  const selectedPlugin = catalogPlugins.find((plugin) => plugin.plugin_id === pluginId);
  const fields = configFields(selectedPlugin);
  const effectiveFields = fields.length ? fields : (pluginId === "iceberg.sql" ? defaultCatalogFields : []);
  const [config, setConfig] = useState<Record<string, string>>({});
  const [message, setMessage] = useState("");
  const [tables, setTables] = useState<Array<Record<string, unknown>>>([]);
  const [discoveredCatalog, setDiscoveredCatalog] = useState("");
  const [selectedFormatId, setSelectedFormatId] = useState("");
  const [diagnostics, setDiagnostics] = useState<Record<string, CatalogDiagnostic>>({});
  const [diagnosing, setDiagnosing] = useState("");
  const [publishing, setPublishing] = useState(false);
  const discoveryEpoch = useRef(0);
  const discoveryAbortController = useRef<AbortController | null>(null);
  const diagnosticAbortController = useRef<AbortController | null>(null);
  useEffect(() => () => {
    discoveryAbortController.current?.abort();
    diagnosticAbortController.current?.abort();
  }, []);
  useEffect(() => {
    setPluginId((current) => catalogPlugins.some((plugin) => plugin.plugin_id === current) ? current : (catalogPlugins[0]?.plugin_id ?? "iceberg.sql"));
  }, [plugins]);
  useEffect(() => {
    setConfig((current) => Object.fromEntries(effectiveFields.map((field) => [field.name, current[field.name] ?? ""])));
  }, [pluginId]);
  async function save() {
    if (!name.trim()) return setMessage("Connection name is required.");
    const missing = effectiveFields.filter((field) => field.required && !config[field.name]?.trim());
    if (missing.length) return setMessage(`Required configuration missing: ${missing.map((field) => field.name).join(", ")}.`);
    const options: Record<string, unknown> = pluginId === "iceberg.sql" ? { type: "sql" } : {};
    for (const field of effectiveFields) {
      const value = config[field.name]?.trim();
      if (!value) {
        // An empty secret field while editing means “keep the existing
        // server-held reference”, never “erase the credential”.
        if (field.secret && editingCatalog?.options[field.name] !== undefined) {
          options[field.name] = { secret: "[redacted]", scope: `catalog:${editingCatalog.name}` };
        }
        continue;
      }
      if (field.secret) options[field.name] = { secret: value, scope: `catalog:${name.trim()}` };
      else if (field.type === "boolean") options[field.name] = value === "true";
      else if (field.type === "integer" || field.type === "number") options[field.name] = Number(value);
      else options[field.name] = value;
    }
    const existing = editingCatalog ?? catalogs.find((catalog) => catalog.name === name.trim());
    try { await controlPlane.saveCatalog(name.trim(), pluginId, options, existing?.revision); setMessage("Connection saved. Discovery remains bounded to this configured catalog."); setName(""); setConfig({}); setEditingCatalog(null); onReload(); } catch (error) { setMessage(recoveryMessage(error, "Connection was rejected by the control plane.")); }
  }

  function editCatalog(catalog: Catalog) {
    if (catalog.module !== "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog" && !catalogPlugins.some((plugin) => plugin.plugin_id === catalog.module)) {
      setMessage(`Catalog adapter ${catalog.module} is no longer admitted; refresh plugins before editing this connection.`);
      return;
    }
    const nextPluginId = pluginIdForCatalog(catalog, catalogPlugins);
    const nextPlugin = catalogPlugins.find((plugin) => plugin.plugin_id === nextPluginId);
    setEditingCatalog(catalog);
    setName(catalog.name);
    setPluginId(nextPluginId);
    setConfig(formConfigForCatalog(catalog, nextPlugin, nextPluginId));
    setMessage(`Editing ${catalog.name}. Leave secret references unchanged to keep the existing server-held credential.`);
  }

  function cancelEdit() {
    setEditingCatalog(null);
    setName("");
    setConfig({});
    setMessage("");
  }
  async function discover(catalog: string) {
    const epoch = ++discoveryEpoch.current;
    discoveryAbortController.current?.abort();
    const controller = new AbortController();
    discoveryAbortController.current = controller;
    try {
      const catalogRow = catalogs.find((item) => item.name === catalog);
      const catalogPluginId = catalogRow?.module === "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog" ? "iceberg.sql" : catalogRow?.module;
      const choices = pluginPairs.filter((pair) => pair.catalog_plugin_id === catalogPluginId && pair.status === "admitted");
      const discovered = await controlPlane.discoverCatalogTables(catalog, controller.signal);
      if (epoch !== discoveryEpoch.current) return;
      setDiscoveredCatalog(catalog);
      setSelectedFormatId(choices.length === 1 ? choices[0].format_plugin_id : "");
      setTables(discovered.tables);
      setMessage(`Loaded table inventory for ${catalog}.`);
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      if (epoch !== discoveryEpoch.current) return;
      setTables([]); setMessage(recoveryMessage(error, "Discovery failed; source credentials and endpoint policy were not changed."));
    }
  }
  async function diagnose(catalog: string) {
    diagnosticAbortController.current?.abort();
    const controller = new AbortController();
    diagnosticAbortController.current = controller;
    setDiagnosing(catalog);
    try {
      const result = await controlPlane.diagnoseCatalog(catalog, controller.signal);
      setDiagnostics((current) => ({ ...current, [catalog]: result }));
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      setDiagnostics((current) => ({ ...current, [catalog]: { catalog, status: "unavailable", message: recoveryMessage(error, "Diagnostic request failed"), checked_at: new Date().toISOString() } }));
    } finally {
      if (controller === diagnosticAbortController.current) setDiagnosing("");
    }
  }
  async function govern(catalog: string, table: Record<string, unknown>) {
    const target = String(table.target ?? table.name ?? "").trim();
    const identifier = String(table.table_identifier ?? target).trim();
    if (!target || !identifier) return setMessage("The discovered table has no safe identifier.");
    const catalogRow = catalogs.find((item) => item.name === catalog);
    const catalogPluginId = catalogRow?.module === "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog" ? "iceberg.sql" : catalogRow?.module;
    const choices = pluginPairs.filter((pair) => pair.catalog_plugin_id === catalogPluginId && pair.status === "admitted");
    const formatId = selectedFormatId || (choices.length === 1 ? choices[0].format_plugin_id : undefined);
    if (!formatId) return setMessage("Select the table format explicitly before governing a discovered table.");
    const formatPlugin = plugins.find((plugin) => plugin.kind === "table_format" && plugin.plugin_id === formatId);
    if (!formatPlugin) return setMessage("No admitted table-format adapter is available for this catalog.");
    try { await controlPlane.saveAsset(catalog, target, formatPlugin.plugin_id, identifier); setMessage(`Governed asset ${target} registered. Assign owners and author a policy in Assets.`); await discover(catalog); } catch (error) { setMessage(recoveryMessage(error, "Asset registration was rejected; the source table was not changed.")); }
  }
  async function createPublication() {
    if (!canActivate || publishing) return;
    setPublishing(true);
    try { await controlPlane.createWorkspacePublication(); setMessage("Configuration snapshot created. Activate it when ready."); onReload(); } catch (error) { setMessage(recoveryMessage(error, "Snapshot could not be created; resolve readiness errors before retrying.")); } finally { setPublishing(false); }
  }
  async function activatePublication(id: string) {
    if (!canActivate || publishing) return;
    setPublishing(true);
    const current = publications.find((publication) => publication.active)?.id;
    try { await controlPlane.activateWorkspacePublication(id, current); setMessage("Configuration snapshot activated for new data-plane requests."); onReload(); } catch (error) { setMessage(recoveryMessage(error, "Activation was rejected; the current generation remains active. Refresh before retrying.")); } finally { setPublishing(false); }
  }
  const pluginCards = plugins.length > 0 && <div className="form-card"><h3>Admitted adapters</h3><div className="plugin-list">{plugins.map((plugin) => <article className="plugin-card" key={`${plugin.kind}:${plugin.plugin_id}`}><div><strong>{plugin.display_name}</strong><small>{plugin.kind === "catalog" ? "Catalog" : "Table format"} · {plugin.plugin_id} · v{plugin.version}</small></div><div className="capability-list">{plugin.capabilities.map((capability) => <span className="pill" key={capability}>{capability.replaceAll("_", " ")}</span>)}</div></article>)}</div></div>;
  const discoveredChoices = pluginPairs.filter((pair) => pair.catalog_plugin_id === (catalogs.find((item) => item.name === discoveredCatalog)?.module === "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog" ? "iceberg.sql" : catalogs.find((item) => item.name === discoveredCatalog)?.module) && pair.status === "admitted");
  const renderField = (field: PluginConfigField) => {
    const value = config[field.name] ?? "";
    const update = (next: string) => setConfig((current) => ({ ...current, [field.name]: next }));
    const control = field.type === "boolean"
      ? <select value={value} onChange={(event) => update(event.target.value)}><option value="">Choose…</option><option value="true">true</option><option value="false">false</option></select>
      : field.type === "enum" && field.options?.length
        ? <select value={value} onChange={(event) => update(event.target.value)}><option value="">Choose…</option>{field.options.map((option) => <option key={option} value={option}>{option}</option>)}</select>
        : <input value={value} onChange={(event) => update(event.target.value)} placeholder={field.name === "uri" ? "postgresql+psycopg://catalog.internal:5432/catalog" : field.name === "password" ? "prod/catalog/password" : undefined} type={field.secret ? "password" : field.type === "integer" || field.type === "number" ? "number" : "text"} />;
    return <label key={field.name}>{configFieldLabel(field.name)}{field.required ? "" : <span className="muted"> (optional)</span>}{control}</label>;
  };
  return <section className="management-view"><div className="management-head"><div><span className="eyebrow">CONNECTIONS</span><h2>Catalog connections</h2><p className="muted">Choose from adapters admitted by the control plane. Credentials stay in server configuration and are never rendered.</p></div><button className="secondary" onClick={onReload}>Refresh</button></div>{pluginStates.length > 0 && <p className="help">Adapter status: {pluginStates.map((state) => state.plugin_id + " " + (state.lifecycle ?? state.status)).join(", ")}</p>}{pluginCards}<div className="form-card"><div className="management-head"><div><h3>{editingCatalog ? `Edit ${editingCatalog.name}` : "Add catalog"}</h3><p className="help">{editingCatalog ? "Safe values are prefilled. Secret references remain deployment-managed." : "Create a catalog draft using an admitted adapter."}</p></div>{editingCatalog && <button className="secondary compact" onClick={cancelEdit}>Cancel edit</button>}</div><div className="form-grid"><label>Name<input value={name} disabled={Boolean(editingCatalog)} onChange={(event) => setName(event.target.value)} placeholder="analytics" /></label><>{catalogPlugins.length > 1 && <label>Catalog adapter<select value={pluginId} onChange={(event) => setPluginId(event.target.value)}>{catalogPlugins.map((plugin) => <option key={plugin.plugin_id} value={plugin.plugin_id}>{plugin.display_name || plugin.plugin_id}</option>)}</select></label>}{effectiveFields.map(renderField)}</></div><p className="help connection-note">Use a URI without userinfo or query credentials. The password field is a name resolved by the deployment secret provider; never paste a password or token here.</p><button className="primary" onClick={() => void save()}>{editingCatalog ? "Save catalog changes" : "Save connection"}</button>{message && <p className="notice">{message}</p>}</div>{canActivate && <div className="form-card"><div className="management-head"><div><h3>Configuration generations</h3><p className="help">Catalog, runtime, identity, and asset drafts become data-plane state only after an explicit snapshot activation.</p></div><button className="primary" disabled={publishing} onClick={() => void createPublication()}>{publishing ? "Working…" : "Create snapshot"}</button></div>{publications.length ? <div className="table-wrap"><table><thead><tr><th>Generation</th><th>State</th><th>Impact</th><th>Manifest</th><th>Created</th><th>Action</th></tr></thead><tbody>{publications.map((publication) => <tr key={publication.id}><td><code>{publication.id.slice(0, 12)}</code></td><td>{publication.active ? "Active" : "Staged"}</td><td>{publication.asset_count} assets · {publication.catalog_count} catalogs</td><td><code>{publication.manifest_hash.slice(0, 12)}</code></td><td>{new Date(publication.created_at).toLocaleString()}</td><td>{publication.active ? <span className="pill">Serving</span> : <button className="secondary compact" disabled={publishing} onClick={() => void activatePublication(publication.id)}>Activate</button>}</td></tr>)}</tbody></table></div> : <p className="muted">No staged generations exist yet.</p>}</div>}{discoveredChoices.length > 1 && <div className="form-card"><h3>Select table format</h3><p className="help">This catalog advertises multiple output formats. Choose one before governing a discovered table.</p><select aria-label="Selected table format" value={selectedFormatId} onChange={(event) => setSelectedFormatId(event.target.value)}><option value="">Choose a format…</option>{discoveredChoices.map((pair) => <option key={pair.format_plugin_id} value={pair.format_plugin_id}>{pair.format_plugin_id} · handle v{pair.handle_versions.join(", ")}</option>)}</select></div>}{catalogs.length ? <div className="card-list">{catalogs.map((catalog) => { const diagnostic = diagnostics[catalog.name]; return <article className="management-card" key={catalog.id}><div><h3>{catalog.name}</h3><p className="muted">{catalogDisplayName(catalog)}</p>{diagnostic && <p className={diagnostic.status === "ready" ? "diagnostic ready" : "diagnostic unavailable"} role="status">{diagnostic.message}{diagnostic.table_count !== undefined ? ` · ${diagnostic.table_count} tables` : ""}</p>}</div><div className="card-actions"><button className="secondary" onClick={() => editCatalog(catalog)}>Edit</button><button className="secondary" disabled={diagnosing === catalog.name} onClick={() => void diagnose(catalog.name)}>{diagnosing === catalog.name ? "Checking…" : "Check connection"}</button><button className="secondary" onClick={() => void discover(catalog.name)}>Discover tables</button></div></article>; })}</div> : <div className="empty-result"><strong>No catalogs configured</strong><p>Connect an admitted catalog adapter to begin asset onboarding.</p></div>}{tables.length > 0 && <div className="table-wrap"><table><thead><tr><th>Table</th><th>Backend</th><th>Governed</th><th>Action</th></tr></thead><tbody>{tables.map((table, index) => <tr key={String(table.name ?? index)}><td>{String(table.name ?? "Unknown")}</td><td>{String(table.backend ?? "iceberg")}</td><td>{table.governed ? "Yes" : "No"}</td><td>{table.governed ? <span className="pill">Registered</span> : <button className="secondary compact" onClick={() => void govern(discoveredCatalog, table)}>Govern table</button>}</td></tr>)}</tbody></table></div>}</section>;
}


function catalogDisplayName(catalog: Catalog): string {
  return catalog.module === "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
    ? "Iceberg SQL catalog"
    : `Catalog adapter · ${catalog.module}`;
}
