import { describe, expect, test } from "vitest";

import { formToRule, ruleToForm, type PolicyRule } from "./policyLogic";

describe("policy rule form mapping", () => {
  test("round-trips condition arrays without flattening them to strings", () => {
    const rule: PolicyRule = {
      columns: ["id", "email"],
      effect: "allow",
      masks: { email: { type: "email" } },
      ordinal: 1,
      principals: ["group:finance"],
      row_filter: "region = 'us'",
      when: { groups: ["finance", "analytics"], purpose: "support" },
    };

    expect(formToRule(ruleToForm(rule))).toEqual(rule);
  });

  test("maps selected column rows to policy columns", () => {
    const form = ruleToForm({
      columns: ["id", "email"],
      effect: "allow",
      masks: {},
      ordinal: 1,
      principals: ["group:finance"],
      row_filter: null,
      when: {},
    });

    expect(form.columnSelections).toEqual([
      { column: "id", selected: true },
      { column: "email", selected: true },
    ]);
    expect(formToRule(form).columns).toEqual(["id", "email"]);
  });
});
