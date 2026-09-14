import type { Session } from "./api";

/** Stable private-cache scope. Empty/anonymous scope is never shared with a user. */
export function sessionQueryScope(session: Pick<Session, "issuer" | "principal"> | null): string {
  return session ? `${session.issuer ?? ""}|${session.principal}` : "anonymous";
}

/** Inventory keys include actor scope, search and cursor so pages cannot cross sessions. */
export function assetInventoryQueryKey(
  sessionScope: string,
  search: string,
  cursor: string | null,
): readonly [string, string, string, string | null] {
  return ["asset-inventory", sessionScope, search, cursor];
}
