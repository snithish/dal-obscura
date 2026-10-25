import { useEffect, useState } from "react";
import { queryOptions, useQuery } from "@tanstack/react-query";
import { controlPlane } from "../../data/api";
import { sessionQueryScope } from "../../data/query_scope";
import { useSessionScope } from "../identity/SessionBoundary";

function inventoryChanges(scope: string) {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const since = today.toISOString();
  return queryOptions({
    queryKey: ["inventory-changes", scope, since],
    queryFn: ({ signal }) =>
      controlPlane.listAuditEventsPage({
        limit: 50,
        resourceType: "asset",
        outcome: "success",
        createdAfter: since,
        signal,
      }),
  });
}

export function InventoryInsights() {
  const { session, generation, queryClient } = useSessionScope();
  const scope = sessionQueryScope(session, generation);
  const summary = useQuery(
    {
      queryKey: ["inventory-summary", scope],
      queryFn: ({ signal }) => controlPlane.getSummary(signal),
      enabled: Boolean(session),
    },
    queryClient,
  );
  const changes = useQuery({ ...inventoryChanges(scope), enabled: Boolean(session) }, queryClient);
  const changed = new Set(changes.data?.items.map((event) => event.resource_id)).size;
  return (
    <section className="inventory-summary" aria-label="Inventory summary">
      {summary.data ? (
        <>
          <div>
            <span>Governed</span>
            <strong>{summary.data.asset_count.toLocaleString()}</strong>
            <small>Control-plane summary</small>
          </div>
          <div>
            <span>Default NULL masks</span>
            <strong>{summary.data.missing_policy_count.toLocaleString()}</strong>
            <small>Default NULL masks</small>
          </div>
        </>
      ) : (
        <p className="help">
          {summary.isError ? "Inventory totals unavailable." : "Loading inventory totals…"}
        </p>
      )}
      {changes.data && (
        <div>
          <span>Changed today</span>
          <strong>
            {changed.toLocaleString()}
            {changes.data.next_cursor ? "+" : ""}
          </strong>
          <small>
            {changes.data.next_cursor
              ? "At least this many; recent activity only"
              : "Assets in today's authorized activity"}
          </small>
        </div>
      )}
    </section>
  );
}

/** Fetch history on demand rather than issuing one eager request per inventory row. */
export function AssetLastChange({ assetId, revision }: { assetId: string; revision: number }) {
  const [open, setOpen] = useState(false);
  const { session, generation, queryClient } = useSessionScope();
  const scope = sessionQueryScope(session, generation);
  const recent = useQuery({ ...inventoryChanges(scope), enabled: false }, queryClient);
  const recorded = recent.data?.items.find(
    (event) => event.resource_id === assetId && event.action === "asset.policy.replace",
  );
  const history = useQuery(
    {
      queryKey: ["asset", sessionQueryScope(session, generation), assetId, "last-change"],
      enabled: open && !recorded && Boolean(session),
      queryFn: ({ signal }) =>
        controlPlane.listAuditEventsPage({
          limit: 1,
          assetId,
          action: "asset.policy.replace",
          outcome: "success",
          signal,
        }),
    },
    queryClient,
  );
  const fetched = history.data?.items[0];
  const event =
    recorded && (!fetched || Date.parse(recorded.created_at) > Date.parse(fetched.created_at))
      ? recorded
      : fetched;
  useEffect(() => {
    if (event) setOpen(true);
  }, [event?.id]);
  const revoked = event?.details.revoked_token_count;
  return (
    <details
      open={open}
      className="asset-last-change"
      onToggle={(event) => setOpen(event.currentTarget.open)}
    >
      <summary>
        <span className="revision-tag">rev {revision}</span>
        <span className="help">{event ? "Recorded policy change" : "View last policy change"}</span>
      </summary>
      {(open || event) &&
        (event ? (
          <div>
            <code>{event.actor}</code>
            <time dateTime={event.created_at}>{new Date(event.created_at).toLocaleString()}</time>
            {typeof revoked === "number" && <span>{revoked} tokens revoked</span>}
          </div>
        ) : (
          <p className="help">
            {history.isFetching
              ? "Loading change history…"
              : history.isError
                ? "Change history unavailable."
                : "No recorded policy change available."}
          </p>
        ))}
    </details>
  );
}
