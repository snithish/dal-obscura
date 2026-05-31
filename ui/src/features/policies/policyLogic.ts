export type PolicyRule = {
  ordinal: number;
  effect: "allow" | "deny";
  principals: string[];
  when: Record<string, unknown>;
  columns: string[];
  masks: Record<string, unknown>;
  row_filter: string | null;
};

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

export type PolicyResponsibility = {
  label: string;
  message: string;
};

export type MaskRow = {
  column: string;
  type: string;
};

export type ColumnSelection = {
  column: string;
  selected: boolean;
};

export type ConditionRow = {
  key: string;
  value: string;
  valueKind: "text" | "list";
};

export type PolicyRuleForm = {
  columnsText: string;
  columnSelections: ColumnSelection[];
  conditions: ConditionRow[];
  effect: "allow" | "deny";
  masks: MaskRow[];
  ordinal: number;
  principalsText: string;
  rowFilter: string;
};

export const defaultRule: PolicyRuleForm = {
  columnsText: "*",
  columnSelections: [{ column: "*", selected: true }],
  conditions: [],
  effect: "allow",
  masks: [],
  ordinal: 1,
  principalsText: "group:data-stewards",
  rowFilter: "",
};

export function ruleToForm(rule: PolicyRule): PolicyRuleForm {
  return {
    columnsText: rule.columns.join(", "),
    columnSelections: rule.columns.map((column) => ({ column, selected: true })),
    conditions: Object.entries(rule.when).map(([key, value]) => ({
      key,
      value: Array.isArray(value) ? value.join(", ") : String(value),
      valueKind: Array.isArray(value) ? "list" : "text",
    })),
    effect: rule.effect,
    masks: Object.entries(rule.masks).map(([column, value]) => ({
      column,
      type: maskType(value),
    })),
    ordinal: rule.ordinal,
    principalsText: rule.principals.join(", "),
    rowFilter: rule.row_filter ?? "",
  };
}

export function formToRule(rule: PolicyRuleForm): PolicyRule {
  return {
    columns: selectedColumns(rule),
    effect: rule.effect,
    masks: Object.fromEntries(
      rule.masks
        .filter((mask) => mask.column.trim())
        .map((mask) => [mask.column.trim(), { type: mask.type }]),
    ),
    ordinal: rule.ordinal,
    principals: splitList(rule.principalsText),
    row_filter: rule.rowFilter.trim() || null,
    when: Object.fromEntries(
      rule.conditions
        .filter((condition) => condition.key.trim())
        .map((condition) => [
          condition.key.trim(),
          condition.valueKind === "list" ? splitList(condition.value) : condition.value.trim(),
        ]),
    ),
  };
}

export function mergeColumnSelections(
  current: ColumnSelection[],
  schemaColumns: string[],
): ColumnSelection[] {
  const selected = new Set(current.filter((item) => item.selected).map((item) => item.column));
  if (schemaColumns.length === 0) {
    return current;
  }
  return schemaColumns.map((column) => ({
    column,
    selected: selected.has("*") || selected.has(column),
  }));
}

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

export function policyEditorResponsibility(owners: string[]): PolicyResponsibility {
  if (owners.length === 0) {
    return {
      label: "Needs owner",
      message:
        "Platform admins can seed this policy, then assign owners before handing off ongoing changes.",
    };
  }
  return {
    label: "Owner managed",
    message: `Policy changes are restricted to platform admins and assigned owners: ${owners.join(", ")}.`,
  };
}

function selectedColumns(rule: PolicyRuleForm): string[] {
  const selected = rule.columnSelections
    .filter((item) => item.selected)
    .map((item) => item.column);
  return selected.length > 0 ? selected : splitList(rule.columnsText);
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

function conditionsMatch(conditions: Record<string, unknown>, claims: Record<string, string>): boolean {
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

function splitList(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function maskType(value: unknown): string {
  if (
    value &&
    typeof value === "object" &&
    "type" in value &&
    typeof value.type === "string"
  ) {
    return value.type;
  }
  return "redact";
}
