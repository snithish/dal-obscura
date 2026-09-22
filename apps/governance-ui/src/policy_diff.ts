import type { PolicyRule } from "./api";

export type PolicyChange = { ordinal: number; field: string; before: string; after: string };
export type PolicyDiff = { added: number; removed: number; changed: number; changes: PolicyChange[] };

function fields(rule: PolicyRule): Record<string, unknown> {
  return {
    Effect: rule.effect,
    Principals: [...new Set(rule.principals)].sort(),
    Fields: [...new Set(rule.columns)].sort(),
    Masks: Object.fromEntries(Object.entries(rule.masks).sort(([a], [b]) => a.localeCompare(b)).map(([path, mask]) => [path, { type: mask.type, value: mask.value }])),
    Rows: rule.row_filter,
    Conditions: Object.fromEntries(Object.entries(rule.when ?? {}).sort(([a], [b]) => a.localeCompare(b)).map(([key, value]) => [key, Array.isArray(value) ? [...new Set(value)].sort() : value])),
  };
}

function describe(value: unknown, field: string): string {
  if (value === undefined) return "—";
  if (value === null) return field === "Rows" ? "No restriction" : "None";
  if (Array.isArray(value)) return value.length ? value.map((item) => JSON.stringify(item)).join(", ") : "None";
  if (typeof value === "object") {
    const entries = Object.entries(value);
    return entries.length ? entries.map(([key, item]) => `${key}: ${JSON.stringify(item)}`).join("; ") : "None";
  }
  return String(value);
}

export function policyDiff(currentRules: PolicyRule[], publishedRules: PolicyRule[]): PolicyDiff {
  const current = new Map(currentRules.map((rule) => [rule.ordinal, fields(rule)]));
  const published = new Map(publishedRules.map((rule) => [rule.ordinal, fields(rule)]));
  const result: PolicyDiff = { added: 0, removed: 0, changed: 0, changes: [] };
  for (const ordinal of [...new Set([...current.keys(), ...published.keys()])].sort((a, b) => a - b)) {
    const before = published.get(ordinal);
    const after = current.get(ordinal);
    if (JSON.stringify(before) === JSON.stringify(after)) continue;
    if (!before) result.added++;
    else if (!after) result.removed++;
    else result.changed++;
    for (const field of Object.keys(after ?? before!)) {
      if (JSON.stringify(before?.[field]) !== JSON.stringify(after?.[field])) {
        result.changes.push({ ordinal, field, before: describe(before?.[field], field), after: describe(after?.[field], field) });
      }
    }
  }
  return result;
}
