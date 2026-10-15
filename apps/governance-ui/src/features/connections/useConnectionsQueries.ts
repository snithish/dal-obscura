import { useQuery, type QueryClient } from "@tanstack/react-query";
import { controlPlane, type Session, type Catalog, type PluginDescriptor, type PluginState, type PluginPair } from "../../data/api";
import { recoveryMessage } from "../../data/recovery";

export type ConnectionsData = { catalogs: Catalog[]; plugins: PluginDescriptor[]; pluginStates: PluginState[]; pluginPairs: PluginPair[] };
async function loadConnections(signal: AbortSignal): Promise<ConnectionsData> {
  const [plugins, catalogs] = await Promise.all([controlPlane.listPlugins(signal), controlPlane.listCatalogs(signal)]);
  return { catalogs, plugins: plugins.plugins, pluginStates: plugins.states, pluginPairs: plugins.pairs };
}

export function useConnectionsQueries(client: QueryClient, scope: string, session: Session | null) {
  const allowed = Boolean(session?.capabilities.includes("workspace:admin"));
  const query = useQuery({ queryKey: ["management", scope, "connections", null], enabled: allowed,
    queryFn: ({ signal }) => loadConnections(signal) }, client);
  return { data: allowed ? query.data : undefined, loading: query.isFetching,
    error: query.error ? recoveryMessage(query.error,
      "This management view could not be loaded. The server may be unavailable or the session may have expired.") : "", refresh: () => query.refetch() };
}
