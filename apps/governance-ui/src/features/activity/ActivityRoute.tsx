import { useState } from "react";
import { useSessionScope } from "../identity/SessionBoundary";
import { sessionQueryScope } from "../../data/query_scope";
import { ActivityView } from "./ActivityView";
import { useActivityQueries, type AuditFilters } from "./useActivityQueries";

export function ActivityRoute() {
  const { session, generation, queryClient, isCurrent } = useSessionScope();
  const scope = sessionQueryScope(session, generation);
  const filterKey = ["activity-filters", scope];
  // Retain this login's applied filters across navigation; teardown clears them.
  const [filters, setFilters] = useState(() => {
    queryClient.setQueryDefaults(["activity-filters"], { gcTime: Infinity });
    return queryClient.getQueryData<AuditFilters>(filterKey) ?? {};
  });
  const query = useActivityQueries(queryClient, scope, session, filters);
  function changeFilters(next: AuditFilters) {
    if (!isCurrent(generation)) return;
    queryClient.setQueryData(filterKey, next);
    setFilters(next);
  }
  return <ActivityView data={query.data ?? {}} loading={query.loading} error={query.error}
    onReload={() => void query.refresh()} onLoadMore={query.loadMore} auditLoading={query.auditLoading}
    filters={filters} onFiltersChange={changeFilters} />;
}
