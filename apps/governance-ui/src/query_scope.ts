import type { Session } from "./api";

/** Stable private-cache scope. Empty/anonymous scope is never shared with a user. */
export function sessionQueryScope(
  session: Pick<Session, "issuer" | "principal"> | null,
  generation: number,
): string {
  return session ? JSON.stringify([session.issuer ?? null, session.principal, generation]) : "anonymous";
}

/** Inventory keys include actor scope, search and cursor so pages cannot cross sessions. */
export function assetInventoryQueryKey(
  sessionScope: string,
  search: string,
  cursor: string | null,
): readonly [string, string, string, string | null] {
  return ["asset-inventory", sessionScope, search, cursor];
}
