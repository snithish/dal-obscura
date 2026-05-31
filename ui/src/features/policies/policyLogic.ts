export type PolicyRule = {
  ordinal: number;
  effect: "allow" | "deny";
  principals: string[];
  when: Record<string, unknown>;
  columns: string[];
  masks: Record<string, unknown>;
  row_filter: string | null;
};

export type MaskRow = {
  column: string;
  type: string;
};

export type ConditionRow = {
  key: string;
  value: string;
  valueKind: "text" | "list";
};

export type PolicyRuleForm = {
  columnsText: string;
  conditions: ConditionRow[];
  effect: "allow" | "deny";
  masks: MaskRow[];
  ordinal: number;
  principalsText: string;
  rowFilter: string;
};

export const defaultRule: PolicyRuleForm = {
  columnsText: "*",
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
    columns: splitList(rule.columnsText),
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
