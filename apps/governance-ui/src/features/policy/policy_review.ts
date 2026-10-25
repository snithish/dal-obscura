import type { PolicyRule } from "../../data/api";

const fields = {
  name: "Name",
  description: "Description",
  effect: "Effect",
  principals: "Audience",
  when: "Identity conditions",
  columns: "Allowed columns",
  masks: "Column masks",
  row_filter: "Row filter",
} as const;

// These arrays describe sets. Literal mask replacement values keep their order and whitespace.
function canonical(value: unknown, key = ""): string {
  if (Array.isArray(value)) {
    const items = value.map((item) => canonical(item));
    return JSON.stringify(
      ["principals", "columns", "exempt_principals", "when"].includes(key) ? items.sort() : items,
    );
  }
  if (value && typeof value === "object")
    return JSON.stringify(
      Object.entries(value)
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([name, item]) => [name, canonical(item, key === "when" ? "when" : name)]),
    );
  return JSON.stringify(value ?? null);
}

export type PolicyChange = {
  ordinal: number;
  kind: "added" | "removed" | "updated";
  fields: string[];
  before?: PolicyRule;
  after?: PolicyRule;
};

/** Compare authored settings. This does not evaluate effective access. */
export function policyChanges(saved: PolicyRule[], rules: PolicyRule[]): PolicyChange[] {
  const before = new Map(saved.map((rule) => [rule.ordinal, rule]));
  const after = new Map(rules.map((rule) => [rule.ordinal, rule]));
  return [...new Set([...before.keys(), ...after.keys()])].flatMap((ordinal) => {
    const oldRule = before.get(ordinal),
      newRule = after.get(ordinal);
    const changed =
      oldRule && newRule
        ? Object.entries(fields)
            .filter(
              ([key]) =>
                canonical(oldRule[key as keyof typeof fields], key) !==
                canonical(newRule[key as keyof typeof fields], key),
            )
            .map(([, label]) => label)
        : [];
    if (oldRule && newRule && !changed.length) return [];
    return [
      {
        ordinal,
        kind: !oldRule ? "added" : !newRule ? "removed" : "updated",
        fields: changed,
        before: oldRule,
        after: newRule,
      },
    ];
  });
}
