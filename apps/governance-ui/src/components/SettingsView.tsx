import { useEffect, useRef, useState } from "react";
import type { QueryClient } from "@tanstack/react-query";
import type { AuthProvider, RuntimeSettings, WorkspacePublication } from "../api";
import { controlPlane } from "../api";
import { recoveryMessage } from "../recovery";
import { isAbortError } from "../async";

export type SettingsViewProps = {
  runtime?: RuntimeSettings | null;
  providers: AuthProvider[];
  providerRevision?: number;
  publications: WorkspacePublication[];
  onReload: () => void;
  queryClient: QueryClient;
  sessionScope: string;
};

const emptyRuntime: RuntimeSettings = {
  ticket_ttl_seconds: 0,
  max_tickets: 0,
  max_ticket_exchanges: 0,
  path_rules: [],
};

function providerText(provider: AuthProvider, key: string): string {
  const value = provider.args[key];
  if (Array.isArray(value)) return value.map(String).join(", ");
  if (value && typeof value === "object") {
    return Object.entries(value as Record<string, unknown>)
      .map(([name, path]) => `${name}=${String(path)}`)
      .join(", ");
  }
  return value === undefined || value === null ? "" : String(value);
}

export function SettingsView({
  runtime,
  providers,
  providerRevision,
  publications,
  onReload,
  queryClient,
  sessionScope,
}: SettingsViewProps) {
  const [form, setForm] = useState<RuntimeSettings>(runtime ?? emptyRuntime);
  const [providerRows, setProviderRows] = useState<AuthProvider[]>(providers);
  const [providerTexts, setProviderTexts] = useState<Record<string, string>>({});
  const [message, setMessage] = useState("");
  const [pathRulesText, setPathRulesText] = useState("[]");
  const mutationControllers = useRef<Set<AbortController>>(new Set());

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

  useEffect(() => { const next = runtime ?? emptyRuntime; setForm(next); setPathRulesText(JSON.stringify(next.path_rules, null, 2)); }, [runtime]);
  useEffect(() => {
    setProviderRows(providers);
    setProviderTexts(Object.fromEntries(providers.flatMap((provider) => [
      "subject_claim", "group_claims", "attribute_claims", "algorithms",
      "leeway_seconds", "jwks_refresh_interval_seconds", "max_jwks_keys",
    ].map((key) => [`${provider.id}:${key}`, providerText(provider, key)]))));
  }, [providers]);

  async function save() {
    if (form.ticket_ttl_seconds < 1 || form.max_tickets < 1 || form.max_ticket_exchanges < 1) {
      setMessage("Enter positive values for all runtime limits before saving.");
      return;
    }
    const controller = beginMutation();
    try {
      let pathRules: Array<Record<string, string>>;
      try {
        const parsed: unknown = JSON.parse(pathRulesText);
        if (!Array.isArray(parsed) || parsed.some((item) => !item || typeof item !== "object" || Object.keys(item).length !== 1 || typeof (item as { root?: unknown }).root !== "string" || !(item as { root: string }).root.trim())) throw new Error("invalid");
        pathRules = parsed as Array<Record<string, string>>;
      } catch {
        setMessage("Path rules must be a JSON array of objects with non-empty root values.");
        return;
      }
      await controlPlane.saveRuntimeSettings({ ...form, path_rules: pathRules }, controller.signal);
      if (controller.signal.aborted) return;
      void queryClient.invalidateQueries({ queryKey: ["management", sessionScope, "settings"] });
      setMessage("Runtime settings saved as draft configuration. Publish to make worker behavior change.");
      onReload();
    } catch (error) {
      if (!isAbortError(error)) setMessage(recoveryMessage(error, "Settings update was rejected; the previous values remain active."));
    } finally { finishMutation(controller); }
  }

  async function saveProviders() {
    const controller = beginMutation();
    try {
      await controlPlane.saveAuthProviders(
        providerRows.map((provider, index) => ({
          ordinal: index + 1,
          module: provider.module,
          args: provider.args,
          enabled: provider.enabled,
        })),
        providerRows[0]?.revision ?? providerRevision,
        controller.signal,
      );
      if (controller.signal.aborted) return;
      void queryClient.invalidateQueries({ queryKey: ["management", sessionScope, "settings"] });
      setMessage("Identity provider settings saved as draft configuration. Publish a snapshot to activate them.");
      onReload();
    } catch (error) {
      if (!isAbortError(error)) setMessage(recoveryMessage(error, "Identity provider update was rejected; the serving provider chain remains unchanged."));
    } finally { finishMutation(controller); }
  }

  function updateProvider(index: number, key: string, value: string) {
    setProviderRows((current) =>
      current.map((provider, row) => {
        if (row !== index) return provider;
        const args = { ...provider.args };
        if ((key === "audience" || key === "jwks_url") && !value.trim()) {
          delete args[key];
        } else {
          args[key] = key === "group_claims" || key === "attribute_claims"
            ? value.split(",").map((item) => item.trim()).filter(Boolean)
            : value;
        }
        return { ...provider, args };
      }),
    );
  }

  function updateProviderText(index: number, key: string, value: string) {
    const provider = providerRows[index];
    if (!provider) return;
    setProviderTexts((current) => ({ ...current, [`${provider.id}:${key}`]: value }));
    let parsed: unknown = value;
    if (key === "group_claims" || key === "algorithms") {
      parsed = value.split(",").map((item) => item.trim()).filter(Boolean);
    } else if (key === "attribute_claims") {
      const entries = value.split(",").map((item) => item.trim()).filter(Boolean);
      const mapping: Record<string, string> = {};
      for (const entry of entries) {
        const separator = entry.indexOf("=");
        if (separator <= 0 || separator === entry.length - 1) {
          setMessage("Attribute claims use name=claim.path entries separated by commas.");
          return;
        }
        mapping[entry.slice(0, separator).trim()] = entry.slice(separator + 1).trim();
      }
      parsed = mapping;
    } else if (["leeway_seconds", "jwks_refresh_interval_seconds", "max_jwks_keys"].includes(key)) {
      parsed = value.trim() === "" ? undefined : Number(value);
    }
    setProviderRows((current) => current.map((item, row) => row === index ? {
      ...item,
      args: (() => {
        const args = { ...item.args };
        if (parsed === undefined) delete args[key];
        else args[key] = parsed;
        return args;
      })(),
    } : item));
  }

  const active = publications.find((publication) => publication.active);
  const stagedCount = publications.filter((publication) => !publication.active).length;
  return (
    <section className="management-view">
      <div className="management-head">
        <div>
          <span className="eyebrow">SETTINGS</span>
          <h2>Runtime and identity</h2>
          <p className="muted">These controls affect ticket fan-out and authentication. Changes are server-validated and do not expose secrets.</p>
        </div>
        <button className="secondary" onClick={onReload}>Refresh</button>
      </div>
      <div className="form-card">
        <h3>Runtime limits</h3>
        <p className="help">{runtime ? "Serving values are loaded from the control plane. Saving creates a staged configuration." : "No runtime settings are configured yet. Enter values to create the first staged configuration."}</p>
        <div className="form-grid three">
          <label>Ticket TTL (seconds)<input type="number" min="1" value={form.ticket_ttl_seconds || ""} placeholder="900" onChange={(event) => setForm({ ...form, ticket_ttl_seconds: Number(event.target.value) })} /></label>
          <label>Max tickets<input type="number" min="1" value={form.max_tickets || ""} placeholder="64" onChange={(event) => setForm({ ...form, max_tickets: Number(event.target.value) })} /></label>
          <label>Ticket exchanges<input type="number" min="1" value={form.max_ticket_exchanges || ""} placeholder="2" onChange={(event) => setForm({ ...form, max_ticket_exchanges: Number(event.target.value) })} /></label>
        </div>
        <label className="runtime-path-rules">Storage path roots (JSON)<textarea value={pathRulesText} onChange={(event) => setPathRulesText(event.target.value)} aria-label="Storage path roots JSON" spellCheck={false} placeholder={'[{"root":"s3://warehouse/curated"}]'} /></label>
        <p className="help">Every metadata and data location must stay under one of these roots. Leave the list empty only for an explicitly local development profile.</p>
        <button className="primary" onClick={() => void save()}>Save runtime settings</button>
        {message && <p className="notice" role="status">{message}</p>}
      </div>
      <div className="form-card">
        <h3>Configuration state</h3>
        {active ? <p><span className="status-dot ready" /> Serving generation <code>{active.id.slice(0, 12)}</code> · {active.asset_count} assets · {active.catalog_count} catalogs</p> : <p><span className="status-dot unavailable" /> No generation is serving yet.</p>}
        <p className="help">{stagedCount ? `${stagedCount} staged generation${stagedCount === 1 ? " is" : "s are"} waiting for explicit administrator activation.` : "Save settings creates draft configuration; create and activate a snapshot from Connections when ready."}</p>
      </div>
      <div className="form-card">
        <h3>Authentication providers</h3>
        {providerRows.length ? providerRows.map((provider, index) => <div className="provider-editor" key={provider.id}><div className="management-head"><div><strong>OIDC provider {index + 1}</strong><small>{provider.module}</small></div><label className="checkbox-label"><input type="checkbox" checked={provider.enabled} onChange={(event) => setProviderRows((current) => current.map((item, row) => row === index ? { ...item, enabled: event.target.checked } : item))} /> Enabled</label></div><div className="form-grid"><label>Issuer<input value={String(provider.args.issuer ?? "")} onChange={(event) => updateProvider(index, "issuer", event.target.value)} /></label><label>Audience<input value={String(provider.args.audience ?? "")} onChange={(event) => updateProvider(index, "audience", event.target.value)} /></label><label>JWKS URL<input value={String(provider.args.jwks_url ?? "")} onChange={(event) => updateProvider(index, "jwks_url", event.target.value)} /></label><label>Group claims<input value={providerTexts[`${provider.id}:group_claims`] ?? ""} onChange={(event) => updateProviderText(index, "group_claims", event.target.value)} /></label><label>Subject claim<input value={providerTexts[`${provider.id}:subject_claim`] ?? "sub"} onChange={(event) => updateProviderText(index, "subject_claim", event.target.value)} /></label><label>Attribute claims<input value={providerTexts[`${provider.id}:attribute_claims`] ?? ""} onChange={(event) => updateProviderText(index, "attribute_claims", event.target.value)} placeholder="tenant=tenant.id" /></label><label>Algorithms<input value={providerTexts[`${provider.id}:algorithms`] ?? ""} onChange={(event) => updateProviderText(index, "algorithms", event.target.value)} placeholder="RS256, RS384" /></label><label>Clock leeway (seconds)<input type="number" min="0" max="300" value={providerTexts[`${provider.id}:leeway_seconds`] ?? ""} onChange={(event) => updateProviderText(index, "leeway_seconds", event.target.value)} /></label><label>JWKS refresh (seconds)<input type="number" min="1" max="86400" value={providerTexts[`${provider.id}:jwks_refresh_interval_seconds`] ?? ""} onChange={(event) => updateProviderText(index, "jwks_refresh_interval_seconds", event.target.value)} /></label><label>Max JWKS keys<input type="number" min="1" max="4096" value={providerTexts[`${provider.id}:max_jwks_keys`] ?? ""} onChange={(event) => updateProviderText(index, "max_jwks_keys", event.target.value)} /></label></div><p className="help">Secret references remain redacted and are preserved by the server. Provider changes stay staged until an administrator activates a reviewed snapshot.</p></div>) : <div className="empty-result"><strong>No provider configured</strong><p>Production startup must fail closed until an approved identity provider is enabled.</p></div>}
        <button className="primary" onClick={() => void saveProviders()}>Save identity providers</button>
      </div>
    </section>
  );
}
