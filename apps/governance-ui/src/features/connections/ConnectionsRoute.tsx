import { useSessionScope } from "../identity/SessionBoundary";
import { sessionQueryScope } from "../../data/query_scope";
import { QueryView } from "../../shared/QueryView";
import { ConnectionsView } from "./ConnectionsView";
import { useConnectionsQueries } from "./useConnectionsQueries";

export function ConnectionsRoute({ onDirtyChange }: { onDirtyChange: (dirty: boolean) => void }) {
  const { session, generation, queryClient } = useSessionScope();
  const scope = sessionQueryScope(session, generation);
  const query = useConnectionsQueries(queryClient, scope, session);
  return (
    <QueryView
      page="connections"
      loaded={Boolean(query.data)}
      loading={query.loading}
      error={query.error}
      onReload={() => void query.refresh()}
    >
      <ConnectionsView
        catalogs={query.data?.catalogs ?? []}
        plugins={query.data?.plugins ?? []}
        pluginStates={query.data?.pluginStates ?? []}
        pluginPairs={query.data?.pluginPairs ?? []}
        onReload={() => void query.refresh()}
        onDirtyChange={onDirtyChange}
        queryClient={queryClient}
        sessionScope={scope}
      />
    </QueryView>
  );
}
