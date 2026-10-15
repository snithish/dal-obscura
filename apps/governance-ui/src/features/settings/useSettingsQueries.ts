import { useQuery, type QueryClient } from "@tanstack/react-query";
import {
  controlPlane,
  type Session,
  type RuntimeSettings,
  type AuthProvider,
} from "../../data/api";
import { recoveryMessage } from "../../data/recovery";

export type SettingsData = {
  runtime: RuntimeSettings | null;
  providers: AuthProvider[];
  providerRevision: number;
};
async function loadSettings(signal: AbortSignal): Promise<SettingsData> {
  const [runtime, providers, revision] = await Promise.all([
    controlPlane.getRuntimeSettings(signal),
    controlPlane.getAuthProviders(signal),
    controlPlane.getAuthProviderRevision(signal),
  ]);
  return { runtime, providers, providerRevision: revision.revision };
}

export function useSettingsQueries(client: QueryClient, scope: string, session: Session | null) {
  const allowed = Boolean(session?.capabilities.includes("workspace:admin"));
  const query = useQuery(
    {
      queryKey: ["management", scope, "settings", null],
      enabled: allowed,
      queryFn: ({ signal }) => loadSettings(signal),
    },
    client,
  );
  return {
    data: allowed ? query.data : undefined,
    loading: query.isFetching,
    error: query.error
      ? recoveryMessage(
          query.error,
          "This management view could not be loaded. The server may be unavailable or the session may have expired.",
        )
      : "",
    refresh: () => query.refetch(),
  };
}
