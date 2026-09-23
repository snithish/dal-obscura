import { useEffect, useState } from "react";
import { Button, NativeSelect, TextInput } from "@mantine/core";
import type { QueryClient } from "@tanstack/react-query";
import type { AssetAccess, AssetGrant, AuditEvent, AuthProvider, Catalog, PluginDescriptor, PluginPair, PluginState, RuntimeSettings, Session, WorkspaceObservations, WorkspaceSummary } from "../api";
import type { UiPage } from "../navigation";
import { Icon } from "./Icon";
import { ConnectionsView } from "./ConnectionsView";
import { SettingsView } from "./SettingsView";

export type AuditFilters = { actor?: string; action?: string; resourceType?: string; outcome?: string; correlationId?: string; createdAfter?: string; createdBefore?: string };
export type ManagementData = { events?: AuditEvent[]; eventsNextCursor?: string | null; catalogs?: Catalog[]; runtime?: RuntimeSettings | null; providers?: AuthProvider[]; providerRevision?: number; summary?: WorkspaceSummary; observations?: WorkspaceObservations; grants?: AssetGrant[]; access?: AssetAccess; plugins?: PluginDescriptor[]; pluginStates?: PluginState[]; pluginPairs?: PluginPair[] };

function hasLoadedManagementData(page: Exclude<UiPage, "assets">, data: ManagementData): boolean {
  if (page === "activity") return "events" in data || "summary" in data || "observations" in data;
  if (page === "connections") return "catalogs" in data || "plugins" in data || "pluginStates" in data;
  return "runtime" in data || "providers" in data || "providerRevision" in data;
}

export function ManagementView({ page, data, loading, error, onReload, onLoadMore, auditLoading, filters, onFiltersChange, session, queryClient, sessionScope, onDirtyChange }: { page: Exclude<UiPage, "assets">; data: ManagementData; loading: boolean; error: string; onReload: () => void; onLoadMore?: () => void; auditLoading?: boolean; filters: AuditFilters; onFiltersChange: (filters: AuditFilters) => void; session: Session | null; queryClient: QueryClient; sessionScope: string; onDirtyChange?: (dirty: boolean) => void }) {
  const loaded = hasLoadedManagementData(page, data);
  if (loading && !loaded) return <section className="coming-soon"><span className="eyebrow">{page.toUpperCase()}</span><h2>Loading {page}</h2><p>Checking the current workspace state and your capabilities.</p></section>;
  if (error && !loaded) return <section className="coming-soon" role="alert"><span className="eyebrow">{page.toUpperCase()}</span><h2>Management view unavailable</h2><p>{error}</p><Button type="button" variant="default" className="secondary" onClick={onReload} leftSection={<Icon name="refresh-cw" size={16} />}>Retry</Button></section>;
  const refreshError = error ? <div className="validation-summary management-refresh-error" role="alert"><strong>Workspace refresh failed</strong><p>{error}</p><Button type="button" variant="default" size="sm" className="secondary compact" onClick={onReload} leftSection={<Icon name="refresh-cw" size={15} />}>Retry refresh</Button></div> : null;
  if (page === "activity") return <>{refreshError}<ActivityView events={data.events ?? []} nextCursor={data.eventsNextCursor} onLoadMore={onLoadMore} loading={auditLoading ?? false} filters={filters} onFiltersChange={onFiltersChange} summary={data.summary} observations={data.observations} /></>;
  if (page === "connections") return <>{refreshError}<ConnectionsView catalogs={data.catalogs ?? []} plugins={data.plugins ?? []} pluginStates={data.pluginStates ?? []} pluginPairs={data.pluginPairs ?? []} canManagePlugins={Boolean(session?.platform_admin)} onReload={onReload} onDirtyChange={onDirtyChange} queryClient={queryClient} sessionScope={sessionScope} /></>;
  return <>{refreshError}<SettingsView runtime={data.runtime} providers={data.providers ?? []} providerRevision={data.providerRevision} onReload={onReload} queryClient={queryClient} sessionScope={sessionScope} onDirtyChange={onDirtyChange} /></>;
}

function ActivityView({ events, nextCursor, onLoadMore, loading, filters, onFiltersChange, summary, observations }: { events: AuditEvent[]; nextCursor?: string | null; onLoadMore?: () => void; loading: boolean; filters: AuditFilters; onFiltersChange: (filters: AuditFilters) => void; summary?: WorkspaceSummary; observations?: WorkspaceObservations }) {
  const [draftFilters, setDraftFilters] = useState<AuditFilters>(filters);
  useEffect(() => setDraftFilters(filters), [filters]);
  function setFilter(key: keyof AuditFilters, value: string) {
    setDraftFilters((current) => ({ ...current, [key]: value || undefined }));
  }
  function applyFilters() { onFiltersChange({ ...draftFilters }); }
  function clearFilters() { setDraftFilters({}); onFiltersChange({}); }
  return <section className="management-view">
    <span className="eyebrow">ACTIVITY</span>
    <h2>Workspace status</h2>
    <p className="muted">Live counts and redacted audit observations from the control plane. Data is limited to the assets your session can see.</p>
    <div className="form-card audit-filters">
      <h3>Filter activity</h3>
      <div className="form-grid three">
        <TextInput label="Actor" value={draftFilters.actor ?? ""} onChange={(event) => setFilter("actor", event.currentTarget.value)} placeholder="platform:admin" />
        <TextInput label="Action" value={draftFilters.action ?? ""} onChange={(event) => setFilter("action", event.currentTarget.value)} placeholder="asset.policy.replace" />
        <TextInput label="Request ID" value={draftFilters.correlationId ?? ""} onChange={(event) => setFilter("correlationId", event.currentTarget.value)} placeholder="Correlation ID" />
        <NativeSelect label="Resource type" value={draftFilters.resourceType ?? ""} onChange={(event) => setFilter("resourceType", event.currentTarget.value)} data={[{ value: "", label: "All resources" }, { value: "asset", label: "Asset" }, { value: "catalog", label: "Catalog" }, { value: "plugin", label: "Plugin" }, { value: "workspace", label: "Workspace" }]} />
        <NativeSelect label="Outcome" value={draftFilters.outcome ?? ""} onChange={(event) => setFilter("outcome", event.currentTarget.value)} data={[{ value: "", label: "All outcomes" }, { value: "success", label: "Success" }, { value: "failure", label: "Failure" }]} />
        <TextInput label="Created after" type="datetime-local" value={draftFilters.createdAfter?.slice(0, 16) ?? ""} onChange={(event) => setFilter("createdAfter", event.currentTarget.value ? new Date(event.currentTarget.value).toISOString() : "")} />
        <TextInput label="Created before" type="datetime-local" value={draftFilters.createdBefore?.slice(0, 16) ?? ""} onChange={(event) => setFilter("createdBefore", event.currentTarget.value ? new Date(event.currentTarget.value).toISOString() : "")} />
      </div>
      <div className="editor-actions"><Button type="button" variant="default" className="secondary" onClick={clearFilters} leftSection={<Icon name="x" size={16} />}>Clear</Button><Button type="button" className="primary" onClick={applyFilters} leftSection={<Icon name="filter" size={16} />}>Apply filters</Button></div>
    </div>
    {summary ? <div className="metric-grid">{[["Assets", summary.asset_count], ["Catalogs", summary.catalog_count], ["Missing policy", summary.missing_policy_count], ["Enabled auth", summary.enabled_auth_provider_count]].map(([label, value]) => <div className="metric-card" key={String(label)}><strong>{String(value)}</strong><span>{label}</span></div>)}</div> : <div className="empty-result"><strong>Status unavailable</strong><p>Reconnect with a session that can read workspace observations.</p></div>}
    {observations && <div className="form-card observation-card"><h3>Runtime observation</h3><p><span className={"status-dot " + (observations.available ? "ready" : "unavailable")} /> {observations.available ? "Control plane connected" : "Workspace unavailable"}</p>{observations.generation ? <p><strong>Live configuration:</strong> <code>{observations.generation.config_revision.slice(0, 12)}</code></p> : <p>No live workspace configuration is available.</p>}<p className="help">Data-plane health: <strong>{observations.data_plane.status}</strong> ({observations.data_plane.reason}). Observed {new Date(observations.observed_at).toLocaleString()} from {observations.source}.</p></div>}
    <h3 className="activity-title">Recent governed actions</h3>
    {events.length ? <><ul className="activity-list">{events.map((event) => <li key={event.id}><span className={"status-dot " + (event.outcome === "success" ? "ready" : "unavailable")} /><div><strong>{event.action}</strong><small>{event.actor} · {event.resource_type} {event.resource_id.slice(0, 8)}</small></div><time>{new Date(event.created_at).toLocaleString()}</time></li>)}</ul>{nextCursor && onLoadMore && <Button type="button" variant="default" size="sm" className="secondary load-more" disabled={loading} onClick={onLoadMore} leftSection={<Icon name="chevron-down" size={15} />}>{loading ? "Loading…" : "Load more activity"}</Button>}</> : <div className="empty-result"><strong>{Object.keys(filters).length ? "No matching activity" : "No activity yet"}</strong><p>{Object.keys(filters).length ? "Try clearing a filter or widening the time range." : "Asset policy, access, and configuration edits appear here after the first governed change."}</p></div>}
  </section>;
}
