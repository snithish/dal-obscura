import { useMemo } from "react";
import { useInfiniteQuery, type InfiniteData, type QueryClient } from "@tanstack/react-query";
import {
  controlPlane,
  type Session,
  type AuditEvent,
  type WorkspaceObservations,
  type WorkspaceSummary,
} from "../../data/api";
import { recoveryMessage } from "../../data/recovery";

export type AuditFilters = {
  actor?: string;
  action?: string;
  resourceType?: string;
  outcome?: string;
  correlationId?: string;
  createdAfter?: string;
  createdBefore?: string;
};
export type ActivityData = {
  events: AuditEvent[];
  eventsNextCursor: string | null;
  summary?: WorkspaceSummary;
  observations?: WorkspaceObservations;
};

async function loadActivity(
  filters: AuditFilters,
  cursor: string | null,
  signal: AbortSignal,
): Promise<ActivityData> {
  const audit = controlPlane.listAuditEventsPage({
    limit: 50,
    cursor: cursor ?? undefined,
    ...filters,
    signal,
  });
  if (cursor) {
    const result = await audit;
    return { events: result.items, eventsNextCursor: result.next_cursor ?? null };
  }
  const [result, summary, observations] = await Promise.all([
    audit,
    controlPlane.getSummary(signal),
    controlPlane.getObservations(signal),
  ]);
  return {
    events: result.items,
    eventsNextCursor: result.next_cursor ?? null,
    summary,
    observations,
  };
}

export function useActivityQueries(
  client: QueryClient,
  scope: string,
  session: Session | null,
  filters: AuditFilters,
) {
  const query = useInfiniteQuery<
    ActivityData,
    Error,
    InfiniteData<ActivityData, string | null>,
    readonly unknown[],
    string | null
  >(
    {
      queryKey: ["management", scope, "activity", filters],
      enabled: Boolean(session),
      placeholderData: (
        previous: InfiniteData<ActivityData, string | null> | undefined,
        previousQuery,
      ) => (previousQuery?.queryKey[1] === scope ? previous : undefined),
      initialPageParam: null as string | null,
      queryFn: ({ signal, pageParam }) => loadActivity(filters, pageParam, signal),
      getNextPageParam: (last) => last.eventsNextCursor,
    },
    client,
  );
  const data = useMemo<ActivityData | undefined>(() => {
    const pages = query.data?.pages;
    if (!session || !pages?.length) return undefined;
    return {
      ...pages[0],
      events: pages.flatMap((page) => page.events),
      eventsNextCursor: pages.at(-1)?.eventsNextCursor ?? null,
    };
  }, [session, query.data]);
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
