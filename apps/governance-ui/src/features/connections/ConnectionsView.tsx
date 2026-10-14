import { discardChanges, useConfirmation } from "../../shared/ConfirmationProvider";
import { useCallback, useEffect, useRef, useState } from "react";
import { Button, NativeSelect, TextInput } from "@mantine/core";
import type { QueryClient } from "@tanstack/react-query";
import type { Catalog, CatalogDiagnostic, CatalogTable, PluginDescriptor, PluginPair, PluginState } from "../../data/api";
import { controlPlane } from "../../data/api";
import { preserveSecretReference } from "./connection_options";
import { recoveryMessage } from "../../data/recovery";
import { isAbortError } from "../../data/async";
import { Icon } from "../../shared/Icon";

type PluginConfigField = { name: string; type: string; required: boolean; secret: boolean; options?: string[] };
type PluginLifecycle = NonNullable<PluginState["lifecycle"]>;
type DiscoveryNavigation = { cursor: string | null; index: number; history: Array<string | null> };

const lifecycleTransitions: Record<PluginLifecycle, PluginLifecycle[]> = {
  enabled: ["enabled", "draining", "disabled", "revoked"],
  draining: ["draining", "disabled", "revoked"],
  disabled: ["disabled", "enabled"],
  revoked: ["revoked", "removed"],
  removed: ["removed"],
};

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
  if (name === "uri") return "Catalog URI";
  if (name === "tls") return "TLS";
  if (name === "user") return "Database username";
  if (name === "password") return "Password secret reference";
  return name.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
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

function pluginDefaults(plugin?: PluginDescriptor): Record<string, string> {
  const raw = plugin?.config_schema?.defaults;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) return {};
  return Object.fromEntries(Object.entries(raw).flatMap(([key, value]) => {
    if (!/^[A-Za-z][A-Za-z0-9_.-]*$/.test(key)) return [];
    if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean") return [];
    return [[key, String(value)]];
  }));
}

function formConfigForCatalog(catalog: Catalog, plugin: PluginDescriptor | undefined): Record<string, string> {
  const fields = configFields(plugin);
  const options = catalog.options ?? {};
  return { ...pluginDefaults(plugin), ...Object.fromEntries(fields.map((field) => [field.name, safeFormValue(options[field.name], field)])) };
}

export type ConnectionsViewProps = {
  catalogs: Catalog[];
  plugins: PluginDescriptor[];
  pluginStates: PluginState[];
  pluginPairs: PluginPair[];
  canManagePlugins: boolean;
  onReload: () => void;
  onDirtyChange?: (dirty: boolean) => void;
  queryClient: QueryClient;
  sessionScope: string;
};

export function ConnectionsView({ catalogs, plugins, pluginStates, pluginPairs, canManagePlugins, onReload, onDirtyChange, queryClient, sessionScope }: ConnectionsViewProps) {
  const [name, setName] = useState("");
  const [editingCatalog, setEditingCatalog] = useState<Catalog | null>(null);
  const catalogPlugins = plugins.filter((plugin) => plugin.kind === "catalog");
  const [pluginId, setPluginId] = useState(catalogPlugins[0]?.plugin_id ?? "");
  const selectedPlugin = catalogPlugins.find((plugin) => plugin.plugin_id === pluginId);
  const fields = configFields(selectedPlugin);
  const effectiveFields = fields;
  const [config, setConfig] = useState<Record<string, string>>({});
  const { confirm } = useConfirmation();
  const [message, setMessage] = useState("");
  const [lifecycleTargets, setLifecycleTargets] = useState<Record<string, PluginState["lifecycle"]>>({});
  const [lifecycleBusy, setLifecycleBusy] = useState<string | null>(null);
  const [tables, setTables] = useState<CatalogTable[]>([]);
  const [discoveredCatalog, setDiscoveredCatalog] = useState("");
  const [discoveryBusy, setDiscoveryBusy] = useState(false);
  const [discoveryError, setDiscoveryError] = useState("");
  const [discoveryNavigation, setDiscoveryNavigation] = useState<DiscoveryNavigation>({ cursor: null, index: 0, history: [null] });
  const [discoveryRequest, setDiscoveryRequest] = useState<(DiscoveryNavigation & { catalog: string }) | null>(null);
  const [nextCursor, setNextCursor] = useState<string | null>(null);
  const [selectedFormatId, setSelectedFormatId] = useState("");
  const [diagnostics, setDiagnostics] = useState<Record<string, CatalogDiagnostic>>({});
  const [diagnosing, setDiagnosing] = useState("");
  const [governBusy, setGovernBusy] = useState<string | null>(null);
  const [connectionDirty, setConnectionDirty] = useState(false);
  const [lifecycleDirty, setLifecycleDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const connectionEditEpoch = useRef(0);
  const lifecycleEditEpoch = useRef(0);
  const catalogStateEpoch = useRef(0);
  const discoveryEpoch = useRef(0);
  const diagnosticEpoch = useRef(0);
  const discoveredCatalogRef = useRef("");
  const discoveredRevisionRef = useRef<number | null>(null);
  const mutationControllers = useRef<Set<AbortController>>(new Set());
  const savingRef = useRef(false);
  const governBusyRef = useRef<Set<string>>(new Set());
  const lifecycleBusyRef = useRef<Set<string>>(new Set());
  const clearDiscovery = useCallback(() => {
    discoveryEpoch.current += 1;
    void queryClient.cancelQueries({ queryKey: ["management", sessionScope, "connections", "discover"] });
    setTables([]);
    setDiscoveredCatalog("");
    discoveredCatalogRef.current = "";
    discoveredRevisionRef.current = null;
    setDiscoveryBusy(false);
    setDiscoveryError("");
    setNextCursor(null);
    setDiscoveryRequest(null);
    setDiscoveryNavigation({ cursor: null, index: 0, history: [null] });
  }, [queryClient, sessionScope]);
  useEffect(() => () => {
    discoveryEpoch.current += 1;
    diagnosticEpoch.current += 1;
    void queryClient.cancelQueries({ queryKey: ["management", sessionScope, "connections", "discover"] });
    for (const controller of mutationControllers.current) controller.abort();
    mutationControllers.current.clear();
  }, [queryClient, sessionScope]);
  useEffect(() => {
    catalogStateEpoch.current += 1;
    if (!discoveredCatalogRef.current) return;
    const current = catalogs.find((catalog) => catalog.name === discoveredCatalogRef.current);
    if (!current || current.revision !== discoveredRevisionRef.current) {
      clearDiscovery();
      setMessage("Catalog configuration changed. Discover tables again.");
    }
  }, [catalogs, pluginPairs, clearDiscovery]);
  useEffect(() => {
    setLifecycleBusy(null);
    setConnectionDirty(false);
    setLifecycleDirty(false);
    connectionEditEpoch.current += 1;
    lifecycleEditEpoch.current += 1;
    clearDiscovery();
  }, [sessionScope, clearDiscovery]);
  const dirty = connectionDirty || lifecycleDirty;
  useEffect(() => {
    onDirtyChange?.(dirty);
  }, [dirty, onDirtyChange]);
  const beginMutation = (): AbortController => {
    const controller = new AbortController();
    mutationControllers.current.add(controller);
    return controller;
  };
  const finishMutation = (controller: AbortController): void => {
    mutationControllers.current.delete(controller);
  };
  const markDirty = (scope: "connection" | "lifecycle"): void => {
    if (scope === "connection") {
      connectionEditEpoch.current += 1;
      setConnectionDirty(true);
    } else {
      lifecycleEditEpoch.current += 1;
      setLifecycleDirty(true);
    }
  };
  useEffect(() => {
    setPluginId((current) => catalogPlugins.some((plugin) => plugin.plugin_id === current) ? current : (catalogPlugins[0]?.plugin_id ?? ""));
  }, [plugins]);
  useEffect(() => {
    if (lifecycleDirty) return;
    setLifecycleTargets(Object.fromEntries(pluginStates.flatMap((state) => state.lifecycle
      ? [[`${state.kind}:${state.plugin_id}`, state.lifecycle] as const]
      : [])));
  }, [pluginStates]);
  useEffect(() => {
    setConfig((current) => ({ ...pluginDefaults(selectedPlugin), ...Object.fromEntries(effectiveFields.map((field) => [field.name, current[field.name] ?? ""])) }));
  }, [pluginId]);
  async function save() {
    if (savingRef.current) return;
    if (!name.trim()) return setMessage("Connection name is required.");
    const missing = effectiveFields.filter((field) => field.required && !config[field.name]?.trim());
    if (missing.length) return setMessage(`Required configuration missing: ${missing.map((field) => field.name).join(", ")}.`);
    const invalidNumber = effectiveFields.find((field) => {
      if ((field.type !== "integer" && field.type !== "number") || !config[field.name]?.trim()) return false;
      const parsed = Number(config[field.name]);
      return !Number.isFinite(parsed) || (field.type === "integer" && !Number.isInteger(parsed));
    });
    if (invalidNumber) return setMessage(`${configFieldLabel(invalidNumber.name)} must be a valid ${invalidNumber.type} value.`);
    if (!selectedPlugin) return setMessage("Select an admitted catalog adapter before saving.");
    savingRef.current = true;
    setSaving(true);
    const options: Record<string, unknown> = { ...pluginDefaults(selectedPlugin) };
    for (const field of effectiveFields) {
      const value = config[field.name]?.trim();
      if (!value) {
        // An empty secret field while editing means “keep the existing
        // server-held reference”, never “erase the credential”.
        if (field.secret && editingCatalog?.options[field.name] !== undefined) {
          const reference = preserveSecretReference(editingCatalog.options[field.name]);
          if (reference) options[field.name] = reference;
        }
        continue;
      }
      if (field.secret) options[field.name] = { secret: value, scope: `catalog:${name.trim()}` };
      else if (field.type === "boolean") options[field.name] = value === "true";
      else if (field.type === "integer" || field.type === "number") options[field.name] = Number(value);
      else options[field.name] = value;
    }
    const existing = editingCatalog ?? catalogs.find((catalog) => catalog.name === name.trim());
    const operationEpoch = connectionEditEpoch.current;
    const controller = beginMutation();
    try {
      await controlPlane.saveCatalog(name.trim(), pluginId, options, existing?.revision, controller.signal);
      if (controller.signal.aborted || operationEpoch !== connectionEditEpoch.current) return;
      setConnectionDirty(false);
      void queryClient.invalidateQueries({ queryKey: ["management", sessionScope, "connections"] }); setMessage("Connection saved. Discovery remains bounded to this configured catalog."); setName(""); setConfig({}); setEditingCatalog(null); onReload();
    } catch (error) {
      if (!isAbortError(error)) setMessage(recoveryMessage(error, "Connection was rejected by the control plane."));
    } finally { finishMutation(controller); savingRef.current = false; setSaving(false); }
  }

  function editCatalog(catalog: Catalog) {
    const nextPlugin = catalogPlugins.find((plugin) => plugin.plugin_id === catalog.plugin_id);
    if (!nextPlugin) {
      setMessage(`Catalog adapter ${catalog.plugin_id} is no longer admitted; refresh plugins before editing this connection.`);
      return;
    }
    setEditingCatalog(catalog);
    setName(catalog.name);
    setPluginId(catalog.plugin_id);
    setConfig(formConfigForCatalog(catalog, nextPlugin));
    setMessage(`Editing ${catalog.name}. Leave secret references unchanged to keep the existing server-held credential.`);
    connectionEditEpoch.current += 1;
    lifecycleEditEpoch.current += 1;
    setConnectionDirty(false);
    setLifecycleDirty(false);
  }

  async function beginEdit(catalog: Catalog) {
    if (dirty && !await confirm(discardChanges("connection", "edit another connection"))) return;
    editCatalog(catalog);
  }

  async function cancelEdit() {
    if (dirty && !await confirm(discardChanges("connection", "close this editor"))) return;
    setEditingCatalog(null);
    setName("");
    setConfig({});
    setMessage("");
    connectionEditEpoch.current += 1;
    lifecycleEditEpoch.current += 1;
    setConnectionDirty(false);
    setLifecycleDirty(false);
  }

  async function reload() {
    if (dirty && !await confirm(discardChanges("connection", "reload connections"))) return;
    connectionEditEpoch.current += 1;
    lifecycleEditEpoch.current += 1;
    setConnectionDirty(false);
    setLifecycleDirty(false);
    onReload();
  }
  async function discover(catalog: string, navigation: DiscoveryNavigation = { cursor: null, index: 0, history: [null] }) {
    const epoch = ++discoveryEpoch.current;
    const stateEpoch = catalogStateEpoch.current;
    const request = { ...navigation, catalog };
    setDiscoveryRequest(request);
    setDiscoveryBusy(true);
    setDiscoveryError("");
    void queryClient.cancelQueries({ queryKey: ["management", sessionScope, "connections", "discover"] });
    try {
      const catalogRow = catalogs.find((item) => item.name === catalog);
      const catalogPluginId = catalogRow?.plugin_id;
      const choices = pluginPairs.filter((pair) => pair.catalog_plugin_id === catalogPluginId && pair.status === "admitted");
      const discovered = await queryClient.fetchQuery({
        queryKey: ["management", sessionScope, "connections", "discover", catalog, catalogRow?.revision, navigation.cursor],
        queryFn: ({ signal }) => controlPlane.discoverCatalogTables(catalog, navigation.cursor, signal),
        retry: false,
      });
      if (epoch !== discoveryEpoch.current || stateEpoch !== catalogStateEpoch.current) return;
      setDiscoveredCatalog(catalog);
      if (discoveredCatalogRef.current !== catalog) {
        setSelectedFormatId(choices.length === 1 ? choices[0].format_plugin_id : "");
      }
      discoveredCatalogRef.current = catalog;
      discoveredRevisionRef.current = discovered.revision;
      setTables(discovered.tables);
      setDiscoveryNavigation(navigation);
      setNextCursor(discovered.next_cursor);
    } catch (error) {
      if (isAbortError(error)) return;
      if (epoch !== discoveryEpoch.current || stateEpoch !== catalogStateEpoch.current) return;
      setDiscoveryError(recoveryMessage(error, "Discovery failed; retry this page or restart discovery."));
    } finally {
      if (epoch === discoveryEpoch.current) setDiscoveryBusy(false);
    }
  }
  async function diagnose(catalog: string) {
    const epoch = ++diagnosticEpoch.current;
    setDiagnosing(catalog);
    try {
      const result = await queryClient.fetchQuery({
        queryKey: ["management", sessionScope, "connections", "diagnose", catalog],
        queryFn: ({ signal }) => controlPlane.diagnoseCatalog(catalog, signal),
      });
      if (epoch !== diagnosticEpoch.current) return;
      setDiagnostics((current) => ({ ...current, [catalog]: result }));
    } catch (error) {
      if (isAbortError(error) || epoch !== diagnosticEpoch.current) return;
      setDiagnostics((current) => ({ ...current, [catalog]: { catalog, status: "unavailable", message: recoveryMessage(error, "Diagnostic request failed"), checked_at: new Date().toISOString() } }));
    } finally {
      if (epoch !== diagnosticEpoch.current) return;
      setDiagnosing("");
    }
  }
  async function govern(catalog: string, table: CatalogTable) {
    const target = table.target;
    const identifier = table.table_identifier;
    if (!target || !identifier) return setMessage("The discovered table has no safe identifier.");
    const catalogRow = catalogs.find((item) => item.name === catalog);
    const catalogPluginId = catalogRow?.plugin_id;
    const choices = pluginPairs.filter((pair) => pair.catalog_plugin_id === catalogPluginId && pair.status === "admitted");
    const formatId = selectedFormatId || (choices.length === 1 ? choices[0].format_plugin_id : undefined);
    if (!formatId) return setMessage("Select the table format explicitly before governing a discovered table.");
    const formatPlugin = plugins.find((plugin) => plugin.kind === "table_format" && plugin.plugin_id === formatId);
    if (!formatPlugin) return setMessage("No admitted table-format adapter is available for this catalog.");
    const operationKey = `${catalog}\u0000${identifier}`;
    if (governBusyRef.current.has(operationKey)) return;
    governBusyRef.current.add(operationKey);
    setGovernBusy(operationKey);
    const controller = beginMutation();
    const discoveryScope = discoveryEpoch.current;
    const stateEpoch = catalogStateEpoch.current;
    try {
      await controlPlane.saveAsset(catalog, target, formatPlugin.plugin_id, identifier, controller.signal);
      if (controller.signal.aborted || discoveryScope !== discoveryEpoch.current || stateEpoch !== catalogStateEpoch.current || discoveredCatalogRef.current !== catalog) return;
      void queryClient.invalidateQueries({ queryKey: ["management", sessionScope] });
      void queryClient.invalidateQueries({ queryKey: ["asset-inventory", sessionScope] });
      setTables((current) => current.map((item) => item.table_identifier === identifier ? { ...item, governed: true } : item));
      setMessage(`Governed asset ${target} registered. Assign owners and author a policy in Assets.`);
    } catch (error) {
      if (!isAbortError(error)) setMessage(recoveryMessage(error, "Asset registration was rejected; the source table was not changed."));
    } finally {
      finishMutation(controller);
      governBusyRef.current.delete(operationKey);
      setGovernBusy((current) => current === operationKey ? null : current);
    }
  }
  async function updatePluginLifecycle(plugin: PluginDescriptor) {
    const key = `${plugin.kind}:${plugin.plugin_id}`;
    const target = lifecycleTargets[key];
    if (!target) return;
    if (lifecycleBusyRef.current.has(key)) return;
    if (target === "removed" && !await confirm({ title: "Remove plugin?", message: `Remove ${plugin.display_name} from this process? Connections depending on this plugin may become unavailable.`, confirmLabel: "Remove plugin", destructive: true })) return;
    lifecycleBusyRef.current.add(key);
    setLifecycleBusy(key);
    const controller = beginMutation();
    const operationEpoch = lifecycleEditEpoch.current;
    try {
      await controlPlane.setPluginLifecycle(plugin.kind, plugin.plugin_id, target, controller.signal);
      if (controller.signal.aborted || operationEpoch !== lifecycleEditEpoch.current) return;
      setLifecycleDirty(false);
      setMessage(`${plugin.display_name} lifecycle is now ${target}.`);
      await queryClient.invalidateQueries({ queryKey: ["management", sessionScope, "connections"] });
      onReload();
    } catch (error) {
      if (!isAbortError(error)) setMessage(recoveryMessage(error, "Plugin lifecycle change was rejected; the previous state remains active."));
    } finally {
      finishMutation(controller);
      lifecycleBusyRef.current.delete(key);
      if (!controller.signal.aborted) setLifecycleBusy(null);
    }
  }
  const pluginCards = plugins.length > 0 && <div className="form-card"><h3>Admitted adapters</h3><div className="plugin-list">{plugins.map((plugin) => {
    const key = `${plugin.kind}:${plugin.plugin_id}`;
    const state = pluginStates.find((item) => item.kind === plugin.kind && item.plugin_id === plugin.plugin_id);
    const lifecycle = state?.lifecycle ?? "enabled";
    const target = lifecycleTargets[key] ?? lifecycle;
    return <article className="plugin-card" key={key}><div><strong>{plugin.display_name}</strong><small>{plugin.kind === "catalog" ? "Catalog" : plugin.kind === "secret_backend" ? "Secret backend" : "Table format"} · {plugin.plugin_id} · v{plugin.version} · {lifecycle}</small></div><div className="capability-list">{plugin.capabilities.map((capability) => <span className="pill" key={capability}>{capability.replaceAll("_", " ")}</span>)}</div><div className="card-actions"><NativeSelect id={`lifecycle-${key}`} label={`Lifecycle for ${plugin.display_name}`} value={target} disabled={!canManagePlugins} onChange={(event) => { const value = event.currentTarget.value as PluginState["lifecycle"]; markDirty("lifecycle"); setLifecycleTargets((current) => ({ ...current, [key]: value })); }} data={lifecycleTransitions[lifecycle].map((option) => ({ value: option, label: option[0].toUpperCase() + option.slice(1) }))} /><Button type="button" variant="default" size="sm" className="secondary compact" disabled={!canManagePlugins || lifecycleBusy === key || target === lifecycle} onClick={() => void updatePluginLifecycle(plugin)} leftSection={<Icon name="check" size={15} />}>{lifecycleBusy === key ? "Applying…" : "Apply"}</Button></div></article>;
  })}</div></div>;
  const discoveredChoices = pluginPairs.filter((pair) => pair.catalog_plugin_id === catalogs.find((item) => item.name === discoveredCatalog)?.plugin_id && pair.status === "admitted");
  const renderField = (field: PluginConfigField) => {
    const value = config[field.name] ?? "";
    const update = (next: string) => { markDirty("connection"); setConfig((current) => ({ ...current, [field.name]: next })); };
    const label = <>{configFieldLabel(field.name)}{field.required ? "" : <span className="muted"> (optional)</span>}</>;
    const control = field.type === "boolean"
      ? <NativeSelect label={label} value={value} onChange={(event) => update(event.currentTarget.value)} data={[{ value: "", label: "Choose…" }, { value: "true", label: "true" }, { value: "false", label: "false" }]} />
      : field.type === "enum" && field.options?.length
        ? <NativeSelect label={label} value={value} onChange={(event) => update(event.currentTarget.value)} data={[{ value: "", label: "Choose…" }, ...field.options.map((option) => ({ value: option, label: option }))]} />
        : <TextInput label={label} value={value} onChange={(event) => update(event.currentTarget.value)} placeholder={field.name === "uri" ? "https://catalog.example" : field.name === "password" ? "prod/catalog/password" : undefined} type={field.secret ? "password" : field.type === "integer" || field.type === "number" ? "number" : "text"} />;
    return <div key={field.name}>{control}</div>;
  };
  const tableBrowser = (discoveredCatalog || discoveryError) && <section
    className="catalog-discovery form-card" aria-label={`Discovered tables in ${discoveredCatalog || discoveryRequest?.catalog}`}
    aria-busy={discoveryBusy}
  >
    <div className="management-head">
      <div><h3>Tables in {discoveredCatalog || discoveryRequest?.catalog}</h3>
        <p role="status" className="help">{discoveryBusy ? `Loading page ${(discoveryRequest?.index ?? 0) + 1}…` : `Page ${discoveryNavigation.index + 1} · ${tables.length} tables`}</p>
      </div>
      <Button variant="default" disabled={discoveryBusy || governBusy !== null} onClick={() => void discover(discoveredCatalog || discoveryRequest!.catalog)}>Refresh table discovery</Button>
    </div>
    {discoveryError && <div className="discovery-error"><p role="alert">{discoveryError}</p>
      <Button variant="default" disabled={discoveryBusy} onClick={() => discoveryRequest && void discover(discoveryRequest.catalog, discoveryRequest)}>Retry page</Button>
    </div>}
    <div className="discovery-pagination" role="group" aria-label="Discovery pages">
      <Button variant="default" disabled={discoveryBusy || governBusy !== null || discoveryNavigation.index === 0} onClick={() => {
        const index = discoveryNavigation.index - 1;
        void discover(discoveredCatalog, { ...discoveryNavigation, index, cursor: discoveryNavigation.history[index] });
      }}>Previous page</Button>
      <Button variant="default" disabled={discoveryBusy || governBusy !== null || nextCursor === null} onClick={() => {
        if (nextCursor === null) return;
        const index = discoveryNavigation.index + 1;
        const history = [...discoveryNavigation.history.slice(0, index), nextCursor];
        void discover(discoveredCatalog, { cursor: nextCursor, index, history });
      }}>Next page</Button>
    </div>
    {tables.length ? <div className="table-wrap" role="region" aria-label="Discovered catalog tables table" tabIndex={0}>
      <table><thead><tr><th>Table</th><th>Catalog adapter</th><th>Governed</th><th>Action</th></tr></thead>
        <tbody>{tables.map((table) => <tr key={table.table_identifier}>
          <td>{table.name}<small className="discovery-mobile-meta">{table.backend} · {table.governed ? "Governed" : "Not governed"}</small></td><td>{table.backend}</td><td>{table.governed ? "Yes" : "No"}</td>
          <td>{table.governed ? <span className="pill">Registered</span> : <Button variant="default" size="sm" disabled={discoveryBusy || governBusy !== null} onClick={() => void govern(discoveredCatalog, table)} leftSection={<Icon name="shield-check" size={15} />}>Govern table</Button>}</td>
        </tr>)}</tbody></table>
    </div> : <p className="schema-empty">No tables on this page.</p>}
  </section>;
  return <section className="management-view"><div className="management-head"><div><span className="eyebrow">CONNECTIONS</span><h2>Catalog connections</h2><p className="muted">Choose from adapters admitted by the control plane. Credentials stay in server configuration and are never rendered.</p></div><Button type="button" variant="default" className="secondary" onClick={reload} leftSection={<Icon name="refresh-cw" size={16} />}>Refresh</Button></div>{pluginStates.length > 0 && <p className="help">Adapter status: {pluginStates.map((state) => state.plugin_id + " " + (state.lifecycle ?? state.status)).join(", ")}</p>}{pluginCards}<div className="form-card"><div className="management-head"><div><h3>{editingCatalog ? `Edit ${editingCatalog.name}` : "Add catalog"}</h3><p className="help">{editingCatalog ? "Safe values are prefilled. Secret references remain deployment-managed." : "Create a catalog configuration using an admitted adapter."}</p></div>{editingCatalog && <Button type="button" variant="default" size="sm" className="secondary compact" onClick={cancelEdit} leftSection={<Icon name="x" size={15} />}>Cancel edit</Button>}</div><div className="form-grid"><TextInput label="Name" value={name} disabled={Boolean(editingCatalog)} onChange={(event) => { markDirty("connection"); setName(event.currentTarget.value); }} placeholder="analytics" />{catalogPlugins.length > 1 && <NativeSelect label="Catalog adapter" value={pluginId} onChange={(event) => { markDirty("connection"); setPluginId(event.currentTarget.value); }} data={catalogPlugins.map((plugin) => ({ value: plugin.plugin_id, label: plugin.display_name || plugin.plugin_id }))} />}{effectiveFields.map(renderField)}</div><p className="help connection-note">Use a URI without userinfo or query credentials. The password field is a name resolved by the deployment secret provider; never paste a password or token here.</p><Button type="button" className="primary" disabled={saving} onClick={() => void save()} leftSection={<Icon name="save" size={16} />}>{saving ? "Saving…" : editingCatalog ? "Save catalog changes" : "Save connection"}</Button>{message && <p className="notice">{message}</p>}</div>{discoveredChoices.length > 1 && <div className="form-card"><h3>Select table format</h3><p className="help">This catalog advertises multiple output formats. Choose one before governing a discovered table.</p><NativeSelect aria-label="Selected table format" value={selectedFormatId} onChange={(event) => { markDirty("connection"); setSelectedFormatId(event.currentTarget.value); }} data={[{ value: "", label: "Choose a format…" }, ...discoveredChoices.map((pair) => ({ value: pair.format_plugin_id, label: `${pair.format_plugin_id} · handle v${pair.handle_versions.join(", ")}` }))]} /></div>}{catalogs.length ? <div className="card-list">{catalogs.map((catalog) => { const diagnostic = diagnostics[catalog.name]; return <article className="management-card" key={catalog.id}><div><h3>{catalog.name}</h3><p className="muted">{catalogDisplayName(catalog)}</p><p className="diagnostic"><span className="pill pill-muted">{catalog.status ?? "unknown"}</span> · {catalog.governed_asset_count ?? 0} governed assets</p>{diagnostic && <p className={diagnostic.status === "ready" ? "diagnostic ready" : "diagnostic unavailable"} role="status">{diagnostic.message}{diagnostic.table_count !== undefined ? ` · ${diagnostic.table_count} tables` : ""}</p>}</div><div className="card-actions"><Button type="button" variant="default" className="secondary" onClick={() => beginEdit(catalog)} leftSection={<Icon name="pencil" size={16} />}>Edit</Button><Button type="button" variant="default" className="secondary" disabled={diagnosing === catalog.name} onClick={() => void diagnose(catalog.name)} leftSection={<Icon name="refresh-cw" size={15} />}>{diagnosing === catalog.name ? "Checking…" : "Check connection"}</Button><Button type="button" variant="default" className="secondary" onClick={() => void discover(catalog.name)} leftSection={<Icon name="search" size={15} />}>Discover tables</Button></div></article>; })}</div> : <div className="empty-result"><strong>No catalogs configured</strong><p>Connect an admitted catalog adapter to begin asset onboarding.</p></div>}{tableBrowser}</section>;
}


function catalogDisplayName(catalog: Catalog): string {
  return `Catalog adapter · ${catalog.plugin_id}`;
}
