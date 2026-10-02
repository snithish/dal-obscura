import { useEffect, useMemo, useRef, useState } from "react";
import {
  infiniteQueryOptions,
  useInfiniteQuery,
  type InfiniteData,
  type QueryClient,
} from "@tanstack/react-query";
import { controlPlane, type AssetPage } from "./api";
import { assetInventoryQueryKey } from "./query_scope";
import { recoveryMessage } from "./recovery";

export function assetInventoryOptions(scope: string, search = "") {
  return infiniteQueryOptions({
    queryKey: assetInventoryQueryKey(scope, search, null),
    initialPageParam: null as string | null,
    queryFn: ({ signal, pageParam }) =>
      controlPlane.listAssetPage({
        limit: 50,
        search: search || undefined,
        cursor: pageParam ?? undefined,
        signal,
      }),
    getNextPageParam: (page) => page.next_cursor,
  });
}

/** Query cache owns pagination and cancellation; local state owns search input. */
export function useAssetInventory(
  client: QueryClient,
  scope: string,
  enabled: boolean,
) {
  const [input, setInput] = useState({ scope, value: "", term: "" });
  const search = input.scope === scope ? input.value : "";
  const term = input.scope === scope ? input.term : "";
  const query = useInfiniteQuery(
    {
      ...assetInventoryOptions(scope, term),
      enabled,
      placeholderData: (previous, previousQuery) =>
        previousQuery?.queryKey[1] === scope ? previous : undefined,
    },
    client,
  );
  const lastSuccess = useRef<{
    scope: string;
    data: InfiniteData<AssetPage>;
  } | null>(null);
  if (lastSuccess.current?.scope !== scope) lastSuccess.current = null;
  if (query.data && !query.isPlaceholderData)
    lastSuccess.current = { scope, data: query.data };
  // Keep useful results after a failed search, always within this login scope.
  const data = query.data ?? lastSuccess.current?.data;
  const assets = useMemo(
    () => data?.pages.flatMap((page) => page.items) ?? [],
    [data],
  );

  useEffect(() => {
    const timer = window.setTimeout(
      () =>
        setInput((current) => {
          if (current.scope !== scope || current.term === search.trim())
            return current;
          return { ...current, term: search.trim() };
        }),
      250,
    );
    return () => window.clearTimeout(timer);
  }, [scope, search]);

  return {
    assets,
    search,
    loading: query.isFetching,
    hasMore: query.hasNextPage && !query.isPlaceholderData,
    error: query.error
      ? recoveryMessage(
          query.error,
          "Could not update the asset list. Showing the previously loaded results.",
        )
      : "",
    searchAssets(value: string) {
      setInput((current) => ({
        scope,
        value,
        term: current.scope === scope ? current.term : "",
      }));
    },
    refresh: () => query.refetch(),
    loadMore: () => {
      if (query.hasNextPage && !query.isFetching) void query.fetchNextPage();
    },
    reset() {
      lastSuccess.current = null;
      setInput({ scope, value: "", term: "" });
    },
    markPolicySaved(assetId: string, revision: number, configured: boolean) {
      client.setQueriesData<InfiniteData<AssetPage>>(
        { queryKey: ["asset-inventory", scope] },
        (current) =>
          current && {
            ...current,
            pages: current.pages.map((page) => ({
              ...page,
              items: page.items.map((asset) =>
                asset.id === assetId
                  ? {
                      ...asset,
                      policy_revision: revision,
                      policy_status: configured ? "configured" : "missing",
                    }
                  : asset,
              ),
            })),
          },
      );
    },
  };
}
