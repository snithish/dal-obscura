import { maskType, type PolicyRule } from "./ruleForm";

export type PreviewContext = {
  claims: Record<string, string>;
  groups: string[];
  principal: string;
};

export type PolicyPreview = {
  decision: "allow" | "deny";
  masks: Array<{ column: string; type: string }>;
  matchedOrdinal: number | null;
  reason: string;
  rowFilter: string | null;
  visibleColumns: string[];
};

export function previewPolicy(
  rules: PolicyRule[],
  context: PreviewContext,
  schemaColumns: string[],
): PolicyPreview {
  const orderedRules = [...rules].sort((left, right) => left.ordinal - right.ordinal);
  const allowed = new Set<string>();
  const denied = new Set<string>();
  const masks = new Map<string, { column: string; type: string }>();
  const rowFilters: string[] = [];
  const matchedOrdinals: number[] = [];
  let firstDenyOrdinal: number | null = null;

  for (const rule of orderedRules) {
    if (!ruleMatches(rule, context)) {
      continue;
    }

    matchedOrdinals.push(rule.ordinal);
    const matchingColumns = expandColumns(rule.columns, schemaColumns);

    if (rule.effect === "deny") {
      firstDenyOrdinal ??= rule.ordinal;
      for (const column of matchingColumns) {
        denied.add(column);
        masks.delete(column);
      }
      continue;
    }

    for (const column of matchingColumns) {
      if (!denied.has(column)) {
        allowed.add(column);
      }
    }
    for (const [column, value] of Object.entries(rule.masks)) {
      if (denied.has(column)) {
        continue;
      }
      const candidate = { column, type: maskType(value) };
      masks.set(column, chooseMask(masks.get(column), candidate));
    }
    if (rule.row_filter) {
      rowFilters.push(rule.row_filter);
    }
  }

  if (matchedOrdinals.length === 0) {
    return {
      decision: "deny",
      masks: [],
      matchedOrdinal: null,
      reason: "No rule matched.",
      rowFilter: null,
      visibleColumns: [],
    };
  }

  const visibleColumns = expandColumns(["*"], schemaColumns).filter(
    (column) => allowed.has(column) && !denied.has(column),
  );
  if (visibleColumns.length === 0) {
    const denyOrdinal =
      matchedOrdinals.length === 1 && firstDenyOrdinal !== null
        ? firstDenyOrdinal
        : matchedOrdinals[0];
    return {
      decision: "deny",
      masks: [],
      matchedOrdinal: denyOrdinal,
      reason:
        matchedOrdinals.length === 1 && firstDenyOrdinal !== null
          ? `Rule ${firstDenyOrdinal} denied access.`
          : "No columns visible.",
      rowFilter: null,
      visibleColumns: [],
    };
  }

  const visible = new Set(visibleColumns);

  return {
    decision: "allow",
    masks: Array.from(masks.values()).filter((mask) => visible.has(mask.column)),
    matchedOrdinal: matchedOrdinals[0],
    reason: formatMatchedReason(matchedOrdinals),
    rowFilter:
      rowFilters.length > 0 ? rowFilters.map((filter) => `(${filter})`).join(" AND ") : null,
    visibleColumns,
  };
}

function ruleMatches(rule: PolicyRule, context: PreviewContext): boolean {
  return principalsMatch(rule.principals, context) && conditionsMatch(rule.when, context.claims);
}

function principalsMatch(principals: string[], context: PreviewContext): boolean {
  if (principals.includes("*") || principals.includes(context.principal)) {
    return true;
  }
  return context.groups.some((group) => principals.includes(`group:${group}`));
}

function conditionsMatch(
  conditions: Record<string, unknown>,
  claims: Record<string, string>,
): boolean {
  return Object.entries(conditions).every(([key, expected]) => {
    const actual = claims[key];
    if (Array.isArray(expected)) {
      return expected.map(String).includes(actual);
    }
    return String(expected) === actual;
  });
}

function expandColumns(columns: string[], schemaColumns: string[]): string[] {
  if (columns.includes("*")) {
    return schemaColumns.length > 0 ? schemaColumns : ["*"];
  }
  return columns;
}

function chooseMask(
  existing: { column: string; type: string } | undefined,
  candidate: { column: string; type: string },
): { column: string; type: string } {
  if (!existing) {
    return candidate;
  }
  return maskPrecedence(candidate.type) > maskPrecedence(existing.type) ? candidate : existing;
}

function maskPrecedence(mask: string): number {
  switch (mask.toLowerCase()) {
    case "null":
      return 4;
    case "redact":
      return 3;
    case "hash":
      return 2;
    case "default":
      return 1;
    default:
      return 0;
  }
}

function formatMatchedReason(ordinals: number[]): string {
  if (ordinals.length === 1) {
    return `Rule ${ordinals[0]} matched.`;
  }
  return `Rules ${ordinals.join(", ")} matched.`;
}
