import { useEffect, useState } from "react";
import { Button, NativeSelect, TextInput } from "@mantine/core";
import type { AuditEvent, WorkspaceObservations, WorkspaceSummary } from "../../data/api";
import { Icon } from "../../shared/Icon";
import { QueryView } from "../../shared/QueryView";
import type { ActivityData, AuditFilters } from "./useActivityQueries";

export function ActivityView({
  data,
  loading,
  error,
  onReload,
  onLoadMore,
  auditLoading,
  filters,
  onFiltersChange,
}: {
  data: Partial<ActivityData>;
  loading: boolean;
  error: string;
  onReload: () => void;
  onLoadMore?: () => void;
  auditLoading?: boolean;
  filters: AuditFilters;
  onFiltersChange: (filters: AuditFilters) => void;
}) {
  return (
    <QueryView
      page="activity"
      loaded={"events" in data || "summary" in data || "observations" in data}
      loading={loading}
      error={error}
      onReload={onReload}
    >
      <ActivityContent
        events={data.events ?? []}
        nextCursor={data.eventsNextCursor}
        onLoadMore={onLoadMore}
        loading={auditLoading ?? false}
        filters={filters}
        onFiltersChange={onFiltersChange}
        summary={data.summary}
        observations={data.observations}
      />
    </QueryView>
  );
}

function localDateTimeValue(value?: string): string {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  const pad = (part: number) => String(part).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

function ActivityContent({
  events,
  nextCursor,
  onLoadMore,
  loading,
  filters,
  onFiltersChange,
  summary,
  observations,
}: {
  events: AuditEvent[];
  nextCursor?: string | null;
  onLoadMore?: () => void;
  loading: boolean;
  filters: AuditFilters;
  onFiltersChange: (filters: AuditFilters) => void;
  summary?: WorkspaceSummary;
  observations?: WorkspaceObservations;
}) {
  const [draftFilters, setDraftFilters] = useState<AuditFilters>(filters);
  useEffect(() => setDraftFilters(filters), [filters]);
  function setFilter(key: keyof AuditFilters, value: string) {
    setDraftFilters((current) => ({ ...current, [key]: value || undefined }));
  }
  function applyFilters() {
    onFiltersChange({ ...draftFilters });
  }
  function clearFilters() {
    setDraftFilters({});
    onFiltersChange({});
  }
  return (
    <section className="management-view">
      <span className="eyebrow">ACTIVITY</span>
      <h2>Workspace status</h2>
      <p className="muted">
        Live counts and redacted audit observations from the control plane. Data is limited to the
        assets your session can see.
      </p>
      <div className="form-card audit-filters">
        <h3>Filter activity</h3>
        <div className="form-grid three">
          <TextInput
            label="Actor"
            value={draftFilters.actor ?? ""}
            onChange={(event) => setFilter("actor", event.currentTarget.value)}
            placeholder="platform:admin"
          />
          <TextInput
            label="Action"
            value={draftFilters.action ?? ""}
            onChange={(event) => setFilter("action", event.currentTarget.value)}
            placeholder="asset.policy.replace"
          />
          <TextInput
            label="Request ID"
            value={draftFilters.correlationId ?? ""}
            onChange={(event) => setFilter("correlationId", event.currentTarget.value)}
            placeholder="Correlation ID"
          />
          <NativeSelect
            label="Resource type"
            value={draftFilters.resourceType ?? ""}
            onChange={(event) => setFilter("resourceType", event.currentTarget.value)}
            data={[
              { value: "", label: "All resources" },
              { value: "asset", label: "Asset" },
              { value: "catalog", label: "Catalog" },
              { value: "plugin", label: "Plugin" },
              { value: "workspace", label: "Workspace" },
            ]}
          />
          <NativeSelect
            label="Outcome"
            value={draftFilters.outcome ?? ""}
            onChange={(event) => setFilter("outcome", event.currentTarget.value)}
            data={[
              { value: "", label: "All outcomes" },
              { value: "success", label: "Success" },
              { value: "failure", label: "Failure" },
            ]}
          />
          <TextInput
            label="Created after"
            type="datetime-local"
            value={localDateTimeValue(draftFilters.createdAfter)}
            onChange={(event) =>
              setFilter(
                "createdAfter",
                event.currentTarget.value ? new Date(event.currentTarget.value).toISOString() : "",
              )
            }
          />
          <TextInput
            label="Created before"
            type="datetime-local"
            value={localDateTimeValue(draftFilters.createdBefore)}
            onChange={(event) =>
              setFilter(
                "createdBefore",
                event.currentTarget.value ? new Date(event.currentTarget.value).toISOString() : "",
              )
            }
          />
        </div>
        <div className="editor-actions">
          <Button
            type="button"
            variant="default"
            className="secondary"
            onClick={clearFilters}
            leftSection={<Icon name="x" size={16} />}
          >
            Clear
          </Button>
          <Button
            type="button"
            className="primary"
            onClick={applyFilters}
            leftSection={<Icon name="filter" size={16} />}
          >
            Apply filters
          </Button>
        </div>
      </div>
      {summary ? (
        <div className="metric-grid">
          {[
            ["Assets", summary.asset_count],
            ["Catalogs", summary.catalog_count],
            ["Missing policy", summary.missing_policy_count],
            ["Enabled auth", summary.enabled_auth_provider_count],
          ].map(([label, value]) => (
            <div className="metric-card" key={String(label)}>
              <strong>{String(value)}</strong>
              <span>{label}</span>
            </div>
          ))}
        </div>
      ) : (
        <div className="empty-result">
          <strong>Status unavailable</strong>
          <p>Reconnect with a session that can read workspace observations.</p>
        </div>
      )}
      {observations && (
        <div className="form-card observation-card">
          <h3>Runtime observation</h3>
          <p>
            <span className={"status-dot " + (observations.available ? "ready" : "unavailable")} />{" "}
            {observations.available ? "Control plane connected" : "Workspace unavailable"}
          </p>
          <p className="help">
            Data-plane health: <strong>{observations.data_plane.status}</strong> (
            {observations.data_plane.reason}). Observed{" "}
            {new Date(observations.observed_at).toLocaleString()} from {observations.source}.
          </p>
        </div>
      )}
      <h3 className="activity-title">Recent governed actions</h3>
      {events.length ? (
        <>
          <ul className="activity-list">
            {events.map((event) => (
              <li key={event.id}>
                <span
                  className={
                    "status-dot " + (event.outcome === "success" ? "ready" : "unavailable")
                  }
                />
                <div>
                  <strong>{event.action}</strong>
                  <small>
                    {event.actor} · {event.resource_type} {event.resource_id.slice(0, 8)}
                  </small>
                </div>
                <time>{new Date(event.created_at).toLocaleString()}</time>
              </li>
            ))}
          </ul>
          {nextCursor && onLoadMore && (
            <Button
              type="button"
              variant="default"
              size="sm"
              className="secondary load-more"
              disabled={loading}
              onClick={onLoadMore}
              leftSection={<Icon name="chevron-down" size={15} />}
            >
              {loading ? "Loading…" : "Load more activity"}
            </Button>
          )}
        </>
      ) : (
        <div className="empty-result">
          <strong>
            {Object.keys(filters).length ? "No matching activity" : "No activity yet"}
          </strong>
          <p>
            {Object.keys(filters).length
              ? "Try clearing a filter or widening the time range."
              : "Asset policy, access, and configuration edits appear here after the first governed change."}
          </p>
        </div>
      )}
    </section>
  );
}
