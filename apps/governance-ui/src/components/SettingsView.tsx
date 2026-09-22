import { useEffect, useRef, useState } from "react";
import { Button, Checkbox, TextInput } from "@mantine/core";
import type { QueryClient } from "@tanstack/react-query";
import type { AuthProvider, RuntimeSettings, WorkspacePublication } from "../api";
import { controlPlane } from "../api";
import { recoveryMessage } from "../recovery";
import { isAbortError } from "../async";
import { serializePathRules } from "../runtime_settings";

export type SettingsViewProps = {
  runtime?: RuntimeSettings | null;
  providers: AuthProvider[];
  providerRevision?: number;
  publications: WorkspacePublication[];
  onReload: () => void;
  onDirtyChange?: (dirty: boolean) => void;
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
  onDirtyChange,
  queryClient,
  sessionScope,
}: SettingsViewProps) {
  const [form, setForm] = useState<RuntimeSettings>(runtime ?? emptyRuntime);
  const [providerRows, setProviderRows] = useState<AuthProvider[]>(providers);
  const [providerTexts, setProviderTexts] = useState<Record<string, string>>({});
  const [providerErrors, setProviderErrors] = useState<Record<string, string>>({});
  const [message, setMessage] = useState("");
  const [pathRuleRoots, setPathRuleRoots] = useState<string[]>([]);
  const [runtimeDirty, setRuntimeDirty] = useState(false);
  const [providersDirty, setProvidersDirty] = useState(false);
  const [saving, setSaving] = useState(false);
  const runtimeEditEpoch = useRef(0);
  const providerEditEpoch = useRef(0);
  const mutationControllers = useRef<Set<AbortController>>(new Set());
  const savingRef = useRef(false);

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

  useEffect(() => {
    const next = runtime ?? emptyRuntime;
    if (runtimeDirty) return;
    setForm(next);
    setPathRuleRoots(next.path_rules.map((rule) => typeof rule.root === "string" ? rule.root : ""));
    setRuntimeDirty(false);
    runtimeEditEpoch.current += 1;
  }, [runtime, runtimeDirty]);
  useEffect(() => {
    if (providersDirty) return;
    setProviderRows(providers);
    setProviderErrors({});
    setProviderTexts(Object.fromEntries(providers.flatMap((provider) => [
      "subject_claim", "group_claims", "attribute_claims", "algorithms",
      "leeway_seconds", "jwks_refresh_interval_seconds", "max_jwks_keys",
    ].map((key) => [`${provider.id}:${key}`, providerText(provider, key)]))));
    setProvidersDirty(false);
    providerEditEpoch.current += 1;
  }, [providers, providersDirty]);
  const dirty = runtimeDirty || providersDirty;
  useEffect(() => {
    onDirtyChange?.(dirty);
  }, [dirty, onDirtyChange]);

  function reload() {
    if (dirty && !window.confirm("You have unsaved settings changes. Reload and discard them?")) return;
    runtimeEditEpoch.current += 1;
    providerEditEpoch.current += 1;
    setRuntimeDirty(false);
    setProvidersDirty(false);
    onReload();
  }

  function markDirty(scope: "runtime" | "providers"): void {
    if (scope === "runtime") {
      runtimeEditEpoch.current += 1;
      setRuntimeDirty(true);
    } else {
      providerEditEpoch.current += 1;
      setProvidersDirty(true);
    }
  }

  async function save() {
    if (savingRef.current) return;
    if (form.ticket_ttl_seconds < 1 || form.max_tickets < 1 || form.max_ticket_exchanges < 1) {
      setMessage("Enter positive values for all runtime limits before saving.");
      return;
    }
    const pathRules = serializePathRules(pathRuleRoots);
    if (!pathRules) {
      setMessage("Every storage path root must be non-empty before saving.");
      return;
    }
    savingRef.current = true;
    setSaving(true);
    const controller = beginMutation();
    const operationEpoch = runtimeEditEpoch.current;
    try {
      await controlPlane.saveRuntimeSettings({ ...form, path_rules: pathRules }, controller.signal);
      if (controller.signal.aborted || operationEpoch !== runtimeEditEpoch.current) return;
      setRuntimeDirty(false);
      void queryClient.invalidateQueries({ queryKey: ["management", sessionScope, "settings"] });
      setMessage("Runtime settings saved as draft configuration. Publish to make worker behavior change.");
      onReload();
    } catch (error) {
      if (!isAbortError(error)) setMessage(recoveryMessage(error, "Settings update was rejected; the previous values remain active."));
    } finally { finishMutation(controller); savingRef.current = false; setSaving(false); }
  }

  async function saveProviders() {
    if (savingRef.current) return;
    if (Object.keys(providerErrors).length) {
      setMessage("Fix the highlighted identity provider fields before saving.");
      return;
    }
    if (!providerRows.some((provider) => provider.enabled) && !window.confirm(
      "No identity provider will remain enabled. Stage this lockout configuration anyway?",
    )) {
      setMessage("Identity provider changes were not saved. Keep at least one provider enabled unless lockout is intentional.");
      return;
    }
    savingRef.current = true;
    setSaving(true);
    const controller = beginMutation();
    const operationEpoch = providerEditEpoch.current;
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
      if (controller.signal.aborted || operationEpoch !== providerEditEpoch.current) return;
      setProvidersDirty(false);
      void queryClient.invalidateQueries({ queryKey: ["management", sessionScope, "settings"] });
      setMessage("Identity provider settings saved as draft configuration. Publish a snapshot to activate them.");
      onReload();
    } catch (error) {
      if (!isAbortError(error)) setMessage(recoveryMessage(error, "Identity provider update was rejected; the serving provider chain remains unchanged."));
    } finally { finishMutation(controller); savingRef.current = false; setSaving(false); }
  }

  function updateProviderText(index: number, key: string, value: string) {
    const provider = providerRows[index];
    if (!provider) return;
    markDirty("providers");
    const fieldKey = `${provider.id}:${key}`;
    setProviderTexts((current) => ({ ...current, [fieldKey]: value }));
    const setError = (message: string): void => {
      setProviderErrors((current) => ({ ...current, [fieldKey]: message }));
    };
    let parsed: unknown = value;
    if (key === "group_claims" || key === "algorithms") {
      parsed = value.split(",").map((item) => item.trim()).filter(Boolean);
      if (key === "algorithms" && !(parsed as string[]).length) {
        setError("Enter at least one signing algorithm.");
        return;
      }
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
        const numeric = Number(value);
        if (!Number.isFinite(numeric) || !Number.isInteger(numeric) || numeric < 0) {
          setError("Enter a non-negative integer.");
          return;
        }
        const bounds: Record<string, [number, number]> = {
          leeway_seconds: [0, 300],
          jwks_refresh_interval_seconds: [1, 86_400],
          max_jwks_keys: [1, 4_096],
        };
        const [minimum, maximum] = bounds[key];
        if (numeric < minimum || numeric > maximum) {
          setError(`Enter a value between ${minimum} and ${maximum}.`);
          return;
        }
        parsed = numeric;
      }
    } else if (key === "issuer" || key === "jwks_url") {
      const trimmed = value.trim();
      if (!trimmed && key === "issuer") {
        setError("Issuer URL is required.");
        return;
      }
      if (trimmed) {
        try {
          const url = new URL(trimmed);
          if (!(["http:", "https:"] as string[]).includes(url.protocol) || url.username || url.password || url.search || url.hash) {
            setError("Use an HTTP(S) URL without credentials, query data, or a fragment.");
            return;
          }
        } catch {
          setError("Enter a valid HTTP(S) URL.");
          return;
        }
      }
      parsed = trimmed;
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
    markDirty("providers");
    setProviderRows((current) => current.map((provider, row) => row === index ? { ...provider, enabled } : provider));
  }

  function addProvider() {
    markDirty("providers");
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
    markDirty("providers");
    setProviderRows((current) => current.filter((_, row) => row !== index));
    const provider = providerRows[index];
    if (!provider) return;
    setProviderErrors((current) => Object.fromEntries(Object.entries(current).filter(([key]) => !key.startsWith(`${provider.id}:`))));
    setProviderTexts((current) => Object.fromEntries(Object.entries(current).filter(([key]) => !key.startsWith(`${provider.id}:`))));
  }

  function moveProvider(index: number, direction: -1 | 1) {
    markDirty("providers");
    setProviderRows((current) => {
      const target = index + direction;
      if (target < 0 || target >= current.length) return current;
      const next = [...current];
      [next[index], next[target]] = [next[target], next[index]];
      return next.map((provider, ordinal) => ({ ...provider, ordinal: ordinal + 1 }));
    });
  }

  function updatePathRule(index: number, value: string) {
    markDirty("runtime");
    setPathRuleRoots((current) => current.map((root, row) => row === index ? value : root));
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
        <Button type="button" variant="default" className="secondary" onClick={reload}>Refresh</Button>
      </div>
      <div className="form-card">
        <h3>Runtime limits</h3>
        <p className="help">{runtime ? "Serving values are loaded from the control plane. Saving creates a staged configuration." : "No runtime settings are configured yet. Enter values to create the first staged configuration."}</p>
        <div className="form-grid three">
          <TextInput label="Ticket TTL (seconds)" type="number" min={1} value={form.ticket_ttl_seconds || ""} placeholder="900" onChange={(event) => { markDirty("runtime"); setForm({ ...form, ticket_ttl_seconds: Number(event.currentTarget.value) }); }} />
          <TextInput label="Max tickets" type="number" min={1} value={form.max_tickets || ""} placeholder="64" onChange={(event) => { markDirty("runtime"); setForm({ ...form, max_tickets: Number(event.currentTarget.value) }); }} />
          <TextInput label="Ticket exchanges" type="number" min={1} value={form.max_ticket_exchanges || ""} placeholder="2" onChange={(event) => { markDirty("runtime"); setForm({ ...form, max_ticket_exchanges: Number(event.currentTarget.value) }); }} />
        </div>
        <fieldset className="runtime-path-rules">
          <legend>Storage path roots</legend>
          <p className="help">Every metadata and data location must stay under one of these roots. Leave the list empty only for an explicitly local development profile.</p>
          <div className="path-rule-list">{pathRuleRoots.map((root, index) => <div className="path-rule-row" key={`${index}-${root}`}>
            <TextInput aria-label={`Storage path root ${index + 1}`} value={root} onChange={(event) => updatePathRule(index, event.currentTarget.value)} placeholder="s3://warehouse/curated" />
            <Button className="danger" variant="default" size="sm" type="button" onClick={() => { markDirty("runtime"); setPathRuleRoots((current) => current.filter((_, row) => row !== index)); }}>Remove</Button>
          </div>)}</div>
          <Button className="secondary" variant="default" type="button" onClick={() => { markDirty("runtime"); setPathRuleRoots((current) => [...current, ""]); }}>Add storage root</Button>
        </fieldset>
        <Button type="button" className="primary" disabled={saving} onClick={() => void save()}>{saving ? "Saving…" : "Save runtime settings"}</Button>
        {message && <p className="notice" role="status">{message}</p>}
      </div>
      <div className="form-card">
        <h3>Configuration state</h3>
        {active ? <p><span className="status-dot ready" /> Serving generation <code>{active.id.slice(0, 12)}</code> · {active.asset_count} assets · {active.catalog_count} catalogs</p> : <p><span className="status-dot unavailable" /> No generation is serving yet.</p>}
        <p className="help">{stagedCount ? `${stagedCount} staged generation${stagedCount === 1 ? " is" : "s are"} waiting for explicit administrator activation.` : "Save settings creates draft configuration; create and activate a snapshot from Connections when ready."}</p>
      </div>
      <div className="form-card">
        <div className="form-card-head"><h3>Authentication providers</h3><Button className="secondary" variant="default" type="button" onClick={addProvider}>Add OIDC provider</Button></div>
        <p className="help">The ordered provider chain is evaluated top to bottom. Secrets and static JWKS material are never editable in this browser.</p>
        {providerRows.length ? <div className="provider-editor-list">{providerRows.map((provider, index) => {
          const providerField = (key: string, label: string, options: { type?: string; placeholder?: string; help?: string } = {}) => {
            const fieldKey = `${provider.id}:${key}`;
            const error = providerErrors[fieldKey];
            return <div className="provider-field" key={key}>
              <TextInput
                label={label}
                type={options.type ?? "text"}
                value={providerTexts[fieldKey] ?? providerText(provider, key)}
                placeholder={options.placeholder}
                aria-invalid={error ? "true" : undefined}
                aria-describedby={error ? `${fieldKey}-error` : undefined}
                onChange={(event) => updateProviderText(index, key, event.currentTarget.value)}
              />
              {options.help && !error && <small>{options.help}</small>}
              {error && <span className="auth-error" id={`${fieldKey}-error`} role="alert">{error}</span>}
            </div>;
          };
          return <fieldset className="provider-editor" key={provider.id}>
            <legend><span>{provider.module.split(".").at(-1) ?? provider.module}</span><small>Order {provider.ordinal}</small><span className="provider-order-actions"><Button className="secondary" variant="default" size="sm" type="button" disabled={index === 0} onClick={() => moveProvider(index, -1)}>Move up</Button><Button className="secondary" variant="default" size="sm" type="button" disabled={index === providerRows.length - 1} onClick={() => moveProvider(index, 1)}>Move down</Button><Button className="danger" variant="default" size="sm" type="button" onClick={() => removeProvider(index)}>Remove</Button></span></legend>
            <Checkbox className="provider-enabled" checked={provider.enabled} onChange={(event) => updateProviderEnabled(index, event.currentTarget.checked)} label="Enabled" />
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
        <Button type="button" className="primary" disabled={saving || Object.keys(providerErrors).length > 0} onClick={() => void saveProviders()}>{saving ? "Saving…" : "Save identity providers"}</Button>
      </div>
    </section>
  );
}
