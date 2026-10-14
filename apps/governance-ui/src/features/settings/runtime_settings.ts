export type SerializedPathRule = { root: string };

/** Normalize UI roots into the exact runtime-settings payload or reject blanks. */
export function serializePathRules(roots: readonly string[]): SerializedPathRule[] | undefined {
  const normalized = roots.map((root) => root.trim());
  if (normalized.some((root) => !root)) return undefined;
  return normalized.map((root) => ({ root }));
}
