import type { PolicyRule } from "./api";

export type PolicyDiff = { added: number; removed: number; changed: number };

function canonicalRule(rule: PolicyRule): string {
  const masks = Object.fromEntries(Object.entries(rule.masks).sort(([left], [right]) => left.localeCompare(right)));
  const when = rule.when
    ? Object.fromEntries(Object.entries(rule.when).sort(([left], [right]) => left.localeCompare(right)).map(([key, value]) => [key, Array.isArray(value) ? [...value].sort() : value]))
    : null;
  return JSON.stringify({
    ordinal: rule.ordinal,
    effect: rule.effect,
    principals: [...rule.principals].sort(),
    columns: [...rule.columns].sort(),
    masks,
    row_filter: rule.row_filter,
    when,
  });
}

export function policyDiff(currentRules: PolicyRule[], publishedRules: PolicyRule[]): PolicyDiff {
  const current = new Map(currentRules.map((rule) => [rule.ordinal, canonicalRule(rule)]));
  const published = new Map(publishedRules.map((rule) => [rule.ordinal, canonicalRule(rule)]));
  let added = 0;
  let removed = 0;
  let changed = 0;
  for (const [ordinal, fingerprint] of current) {
    if (!published.has(ordinal)) added += 1;
    else if (published.get(ordinal) !== fingerprint) changed += 1;
  }
  for (const ordinal of published.keys()) if (!current.has(ordinal)) removed += 1;
  return { added, removed, changed };
}
