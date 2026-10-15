import type { Session } from "./api";

/** Stable private-cache scope. Empty/anonymous scope is never shared with a user. */
export function sessionQueryScope(
  session: Pick<Session, "issuer" | "principal"> | null,
  generation: number,
): string {
  return session
    ? JSON.stringify([session.issuer ?? null, session.principal, generation])
    : "anonymous";
}

/** One infinite query owns all pages for an actor scope and search. */
export function assetInventoryQueryKey(
  sessionScope: string,
  search: string,
): readonly [string, string, string] {
  return ["asset-inventory", sessionScope, search];
}
