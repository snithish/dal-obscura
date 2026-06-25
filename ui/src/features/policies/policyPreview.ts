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
  const matchedRule = orderedRules.find((rule) => ruleMatches(rule, context));

  if (!matchedRule) {
    return {
      decision: "deny",
      masks: [],
      matchedOrdinal: null,
      reason: "No rule matched.",
      rowFilter: null,
      visibleColumns: [],
    };
  }

  if (matchedRule.effect === "deny") {
    return {
      decision: "deny",
      masks: [],
      matchedOrdinal: matchedRule.ordinal,
      reason: `Rule ${matchedRule.ordinal} denied access.`,
      rowFilter: null,
      visibleColumns: [],
    };
  }

  return {
    decision: "allow",
    masks: Object.entries(matchedRule.masks).map(([column, value]) => ({
      column,
      type: maskType(value),
    })),
    matchedOrdinal: matchedRule.ordinal,
    reason: `Rule ${matchedRule.ordinal} matched.`,
    rowFilter: matchedRule.row_filter,
    visibleColumns: expandColumns(matchedRule.columns, schemaColumns),
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
