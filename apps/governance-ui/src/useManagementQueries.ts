import { useMemo } from "react";
import { useInfiniteQuery, type QueryClient } from "@tanstack/react-query";
import { controlPlane, type Session } from "./api";
import type {
  AuditFilters,
  ManagementData,
} from "./components/ManagementViews";
import type { UiPage } from "./navigation";
import { recoveryMessage } from "./recovery";

async function loadPage(
  page: UiPage,
  filters: AuditFilters,
  cursor: string | null,
  signal: AbortSignal,
): Promise<ManagementData> {
  if (page === "activity") {
    const audit = controlPlane.listAuditEventsPage({
      limit: 50,
      cursor: cursor ?? undefined,
      ...filters,
      signal,
    });
    if (cursor) {
      const result = await audit;
      return { events: result.items, eventsNextCursor: result.next_cursor };
    }
    const [result, summary, observations] = await Promise.all([
      audit,
      controlPlane.getSummary(signal),
      controlPlane.getObservations(signal),
    ]);
    return {
      events: result.items,
      eventsNextCursor: result.next_cursor,
      summary,
      observations,
    };
  }
  if (page === "connections") {
    const [plugins, catalogs] = await Promise.all([
      controlPlane.listPlugins(signal),
      controlPlane.listCatalogs(signal),
    ]);
    return {
      catalogs,
      plugins: plugins.plugins,
      pluginStates: plugins.states,
      pluginPairs: plugins.pairs,
    };
  }
  if (page === "settings") {
    const [runtime, providers, revision] = await Promise.all([
      controlPlane.getRuntimeSettings(signal),
      controlPlane.getAuthProviders(signal),
      controlPlane.getAuthProviderRevision(signal),
    ]);
    return { runtime, providers, providerRevision: revision.revision };
  }
  return {};
}

export function useManagementQueries(
  client: QueryClient,
  scope: string,
  page: UiPage,
  session: Session | null,
  filters: AuditFilters,
) {
  const allowed = Boolean(
    session &&
      page !== "assets" &&
      (page === "activity" || session.capabilities.includes("workspace:admin")),
  );
  const query = useInfiniteQuery(
    {
      queryKey: [
        "management",
        scope,
        page,
        page === "activity" ? filters : null,
      ],
      enabled: allowed,
      initialPageParam: null as string | null,
      queryFn: ({ signal, pageParam }) =>
        loadPage(page, filters, pageParam, signal),
      getNextPageParam: (last) => last.eventsNextCursor,
    },
    client,
  );
  const data = useMemo<ManagementData>(() => {
    const pages = query.data?.pages;
    if (!allowed || !pages?.length) return {};
    return page === "activity"
      ? {
          ...pages[0],
          events: pages.flatMap((item) => item.events ?? []),
          eventsNextCursor: pages.at(-1)?.eventsNextCursor,
        }
      : pages[0];
  }, [allowed, page, query.data]);
  return {
    data,
    loading: query.isFetching,
    auditLoading: query.isFetchingNextPage,
    error: query.error
      ? recoveryMessage(
          query.error,
          "This management view could not be loaded. The server may be unavailable or the session may have expired.",
        )
      : "",
    refresh: () => query.refetch(),
    loadMore: () => {
      if (query.hasNextPage && !query.isFetching) void query.fetchNextPage();
    },
  };
}
