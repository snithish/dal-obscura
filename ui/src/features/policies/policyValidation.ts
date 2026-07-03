import { selectedColumns, splitList, type PolicyRuleForm } from "./ruleForm";

export type PolicyResponsibility = {
  label: string;
  message: string;
};

export type RuleDescription = {
  details: string[];
  title: string;
};

export type SessionActor = {
  groups: string[];
  platform_admin: boolean;
  principal: string;
};

export function describeRuleForm(
  rule: PolicyRuleForm,
  schemaColumns: string[],
): RuleDescription {
  const principals = splitList(rule.principalsText);
  const visibleColumns = selectedColumns(rule);
  const conditionCount = rule.conditions.filter((condition) => condition.key.trim()).length;
  const maskCount = rule.masks.filter((mask) => mask.column.trim()).length;
  return {
    title: `Allow ${principals.join(", ") || "unscoped principals"}`,
    details: [
      `${visibleColumns.includes("*") ? schemaColumns.length || "*" : visibleColumns.length} visible columns`,
      `${conditionCount} match ${conditionCount === 1 ? "condition" : "conditions"}`,
      rule.rowFilter.trim() ? `row filter: ${rule.rowFilter.trim()}` : "no row filter",
      `${maskCount} ${maskCount === 1 ? "mask" : "masks"}`,
    ],
  };
}

export function validateRuleForms(rules: PolicyRuleForm[]): string[] {
  const messages: string[] = [];
  rules.forEach((rule, index) => {
    const label = `Rule ${index + 1}`;
    if (splitList(rule.principalsText).length === 0) {
      messages.push(`${label} needs at least one principal.`);
    }
    if (selectedColumns(rule).length === 0) {
      messages.push(`${label} grant rules need at least one visible column.`);
    }
  });
  return messages;
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

export function canEditPolicy(actor: SessionActor | null, owners: string[]): boolean {
  if (!actor) {
    return false;
  }
  if (actor.platform_admin) {
    return true;
  }
  const ownerTokens = new Set(owners);
  if (ownerTokens.has(actor.principal)) {
    return true;
  }
  return actor.groups.some((group) => ownerTokens.has(`group:${group}`));
}
