import { useEffect, useState } from "react";
import type { QueryClient } from "@tanstack/react-query";
import type { AuthProvider, RuntimeSettings, WorkspacePublication } from "../api";
import { controlPlane } from "../api";
import { recoveryMessage } from "../recovery";

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
};

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
  const [message, setMessage] = useState("");

  useEffect(() => setForm(runtime ?? emptyRuntime), [runtime]);
  useEffect(() => setProviderRows(providers), [providers]);

  async function save() {
    if (form.ticket_ttl_seconds < 1 || form.max_tickets < 1 || form.max_ticket_exchanges < 1) {
      setMessage("Enter positive values for all runtime limits before saving.");
      return;
    }
    try {
      await controlPlane.saveRuntimeSettings(form);
      void queryClient.invalidateQueries({ queryKey: ["management", sessionScope, "settings"] });
      setMessage("Runtime settings saved as draft configuration. Publish to make worker behavior change.");
      onReload();
    } catch (error) {
      setMessage(recoveryMessage(error, "Settings update was rejected; the previous values remain active."));
    }
  }

  async function saveProviders() {
    try {
      await controlPlane.saveAuthProviders(
        providerRows.map((provider, index) => ({
          ordinal: index + 1,
          module: provider.module,
          args: provider.args,
          enabled: provider.enabled,
        })),
        providerRows[0]?.revision ?? providerRevision,
      );
      void queryClient.invalidateQueries({ queryKey: ["management", sessionScope, "settings"] });
      setMessage("Identity provider settings saved as draft configuration. Publish a snapshot to activate them.");
      onReload();
    } catch (error) {
      setMessage(recoveryMessage(error, "Identity provider update was rejected; the serving provider chain remains unchanged."));
    }
  }

  function updateProvider(index: number, key: string, value: string) {
    setProviderRows((current) =>
      current.map((provider, row) => {
        if (row !== index) return provider;
        const args = { ...provider.args };
        args[key] = key === "group_claims" || key === "attribute_claims"
          ? value.split(",").map((item) => item.trim()).filter(Boolean)
          : value;
        return { ...provider, args };
      }),
    );
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
        {providerRows.length ? providerRows.map((provider, index) => <div className="provider-editor" key={provider.id}><div className="management-head"><div><strong>OIDC provider {index + 1}</strong><small>{provider.module}</small></div><label className="checkbox-label"><input type="checkbox" checked={provider.enabled} onChange={(event) => setProviderRows((current) => current.map((item, row) => row === index ? { ...item, enabled: event.target.checked } : item))} /> Enabled</label></div><div className="form-grid"><label>Issuer<input value={String(provider.args.issuer ?? "")} onChange={(event) => updateProvider(index, "issuer", event.target.value)} /></label><label>Audience<input value={String(provider.args.audience ?? "")} onChange={(event) => updateProvider(index, "audience", event.target.value)} /></label><label>JWKS URL<input value={String(provider.args.jwks_url ?? "")} onChange={(event) => updateProvider(index, "jwks_url", event.target.value)} /></label><label>Group claims<input value={Array.isArray(provider.args.group_claims) ? provider.args.group_claims.join(", ") : String(provider.args.group_claims ?? "groups")} onChange={(event) => updateProvider(index, "group_claims", event.target.value)} /></label></div><p className="help">Secret references remain redacted and are preserved by the server. Provider changes stay staged until an administrator activates a reviewed snapshot.</p></div>) : <div className="empty-result"><strong>No provider configured</strong><p>Production startup must fail closed until an approved identity provider is enabled.</p></div>}
        <button className="primary" onClick={() => void saveProviders()}>Save identity providers</button>
      </div>
    </section>
  );
}
