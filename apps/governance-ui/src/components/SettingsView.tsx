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

const OIDC_IDENTITY_MODULE = "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider";

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
  const [providerErrors, setProviderErrors] = useState<Record<string, string>>({});
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
    setProviderErrors({});
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
    if (Object.keys(providerErrors).length) {
      setMessage("Fix the highlighted identity provider fields before saving.");
      return;
    }
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

  function updateProviderText(index: number, key: string, value: string) {
    const provider = providerRows[index];
    if (!provider) return;
    const fieldKey = `${provider.id}:${key}`;
    setProviderTexts((current) => ({ ...current, [fieldKey]: value }));
    let parsed: unknown = value;
    if (key === "group_claims" || key === "algorithms") {
      parsed = value.split(",").map((item) => item.trim()).filter(Boolean);
    } else if (key === "attribute_claims") {
      const entries = value.split(",").map((item) => item.trim()).filter(Boolean);
      const mapping: Record<string, string> = {};
      for (const entry of entries) {
        const separator = entry.indexOf("=");
        if (separator <= 0 || separator === entry.length - 1) {
          setProviderErrors((current) => ({ ...current, [fieldKey]: "Use name=claim.path entries separated by commas." }));
          return;
        }
        mapping[entry.slice(0, separator).trim()] = entry.slice(separator + 1).trim();
      }
      parsed = mapping;
    } else if (["leeway_seconds", "jwks_refresh_interval_seconds", "max_jwks_keys"].includes(key)) {
      if (value.trim() === "") parsed = undefined;
      else {
        parsed = Number(value);
        if (!Number.isFinite(parsed)) {
          setProviderErrors((current) => ({ ...current, [fieldKey]: "Enter a finite number." }));
          return;
        }
      }
    }
    setProviderErrors((current) => { const next = { ...current }; delete next[fieldKey]; return next; });
    setProviderRows((current) => current.map((item, row) => row === index ? {
      ...item,
      args: (() => {
        const args = { ...item.args };
        if (parsed === undefined || ((key === "audience" || key === "jwks_url") && !value.trim())) delete args[key];
        else args[key] = parsed;
        return args;
      })(),
    } : item));
  }

  function updateProviderEnabled(index: number, enabled: boolean) {
    setProviderRows((current) => current.map((provider, row) => row === index ? { ...provider, enabled } : provider));
  }

  function addProvider() {
    const ordinal = providerRows.reduce((highest, provider) => Math.max(highest, provider.ordinal), 0) + 1;
    setProviderRows((current) => [...current, {
      id: `draft-${Date.now()}-${ordinal}`,
      ordinal,
      module: OIDC_IDENTITY_MODULE,
      args: {
        issuer: "",
        subject_claim: "sub",
        group_claims: ["groups"],
        algorithms: ["RS256"],
        leeway_seconds: 0,
        jwks_refresh_interval_seconds: 30,
        max_jwks_keys: 256,
      },
      enabled: true,
      revision: providerRevision ?? 0,
    }]);
    setMessage("New OIDC provider staged. Enter an issuer URL before saving.");
  }

  function removeProvider(index: number) {
    setProviderRows((current) => current.filter((_, row) => row !== index));
    const provider = providerRows[index];
    if (!provider) return;
    setProviderErrors((current) => Object.fromEntries(Object.entries(current).filter(([key]) => !key.startsWith(`${provider.id}:`))));
    setProviderTexts((current) => Object.fromEntries(Object.entries(current).filter(([key]) => !key.startsWith(`${provider.id}:`))));
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
        <div className="form-card-head"><h3>Authentication providers</h3><button className="secondary" type="button" onClick={addProvider}>Add OIDC provider</button></div>
        <p className="help">The ordered provider chain is evaluated top to bottom. Secrets and static JWKS material are never editable in this browser.</p>
        {providerRows.length ? <div className="provider-editor-list">{providerRows.map((provider, index) => {
          const providerField = (key: string, label: string, options: { type?: string; placeholder?: string; help?: string } = {}) => {
            const fieldKey = `${provider.id}:${key}`;
            const error = providerErrors[fieldKey];
            return <label className="provider-field" key={key}>{label}
              <input
                type={options.type ?? "text"}
                value={providerTexts[fieldKey] ?? providerText(provider, key)}
                placeholder={options.placeholder}
                aria-invalid={error ? "true" : undefined}
                aria-describedby={error ? `${fieldKey}-error` : undefined}
                onChange={(event) => updateProviderText(index, key, event.target.value)}
              />
              {options.help && <small>{options.help}</small>}
              {error && <span className="auth-error" id={`${fieldKey}-error`} role="alert">{error}</span>}
            </label>;
          };
          return <fieldset className="provider-editor" key={provider.id}>
            <legend><span>{provider.module.split(".").at(-1) ?? provider.module}</span><small>Order {provider.ordinal}</small><button className="danger" type="button" onClick={() => removeProvider(index)}>Remove</button></legend>
            <label className="provider-enabled"><input type="checkbox" checked={provider.enabled} onChange={(event) => updateProviderEnabled(index, event.target.checked)} /> Enabled</label>
            <div className="form-grid">
              {providerField("issuer", "Issuer URL", { placeholder: "https://id.example.com/" })}
              {providerField("audience", "Audience", { placeholder: "optional or comma-separated" })}
              {providerField("jwks_url", "JWKS URL", { placeholder: "optional; discovered from issuer" })}
              {providerField("subject_claim", "Subject claim", { placeholder: "sub" })}
              {providerField("group_claims", "Group claims", { placeholder: "groups, realm_access.roles" })}
              {providerField("attribute_claims", "Attribute claims", { placeholder: "tenant=tenant.id", help: "Use name=claim.path entries separated by commas." })}
              {providerField("algorithms", "Signing algorithms", { placeholder: "RS256" })}
              {providerField("leeway_seconds", "Clock leeway (seconds)", { type: "number", placeholder: "0" })}
              {providerField("jwks_refresh_interval_seconds", "JWKS refresh (seconds)", { type: "number", placeholder: "30" })}
              {providerField("max_jwks_keys", "Maximum JWKS keys", { type: "number", placeholder: "256" })}
            </div>
          </fieldset>;
        })}</div> : <p className="empty-result"><strong>No identity providers configured.</strong><br />Add the first OIDC provider through the control-plane bootstrap or API before publishing.</p>}
        <button className="primary" disabled={Object.keys(providerErrors).length > 0} onClick={() => void saveProviders()}>Save identity providers</button>
      </div>
    </section>
  );
}
