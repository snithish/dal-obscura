import { queryOptions } from "@tanstack/react-query";
import { controlPlane } from "../../data/api";

export function assetQueries(scope: string, assetId: string) {
  const key = ["asset", scope, assetId] as const;
  return {
    detail: queryOptions({
      queryKey: [...key, "detail"],
      queryFn: ({ signal }) => controlPlane.getAsset(assetId, signal),
    }),
    schema: queryOptions({
      queryKey: [...key, "schema"],
      queryFn: ({ signal }) => controlPlane.getSchema(assetId, signal),
    }),
    grants: queryOptions({
      queryKey: [...key, "grants"],
      queryFn: ({ signal }) => controlPlane.listGrants(assetId, signal),
    }),
    access: queryOptions({
      queryKey: [...key, "access"],
      queryFn: ({ signal }) => controlPlane.getAssetAccess(assetId, signal),
    }),
  };
}
