import { useSessionScope } from "../identity/SessionBoundary";
import { sessionQueryScope } from "../../data/query_scope";
import { QueryView } from "../../shared/QueryView";
import { SettingsView } from "./SettingsView";
import { useSettingsQueries } from "./useSettingsQueries";

export function SettingsRoute({ onDirtyChange }: { onDirtyChange: (dirty: boolean) => void }) {
  const { session, generation, queryClient } = useSessionScope();
  const scope = sessionQueryScope(session, generation);
  const query = useSettingsQueries(queryClient, scope, session);
  return <QueryView page="settings" loaded={Boolean(query.data)} loading={query.loading}
    error={query.error} onReload={() => void query.refresh()}>
    <SettingsView runtime={query.data?.runtime} providers={query.data?.providers ?? []}
      providerRevision={query.data?.providerRevision} onReload={() => void query.refresh()}
      onDirtyChange={onDirtyChange} queryClient={queryClient} sessionScope={scope} />
  </QueryView>;
}
