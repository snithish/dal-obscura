import { describe, expect, test } from "vitest";

import {
  formToRule,
  previewPolicy,
  ruleToForm,
  type PolicyRule,
} from "./policyLogic";

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

describe("policy preview", () => {
  test("returns the first matching allow rule with visible columns and masks", () => {
    const preview = previewPolicy(
      [
        {
          columns: ["id", "email"],
          effect: "allow",
          masks: { email: { type: "email" } },
          ordinal: 1,
          principals: ["group:finance"],
          row_filter: "region = 'us'",
          when: { purpose: "support" },
        },
      ],
      {
        claims: { purpose: "support" },
        principal: "user:alice@example.com",
        groups: ["finance"],
      },
      ["id", "email", "region"],
    );

    expect(preview).toEqual({
      decision: "allow",
      masks: [{ column: "email", type: "email" }],
      matchedOrdinal: 1,
      reason: "Rule 1 matched.",
      rowFilter: "region = 'us'",
      visibleColumns: ["id", "email"],
    });
  });

  test("returns deny when the first matching rule denies access", () => {
    const preview = previewPolicy(
      [
        {
          columns: ["*"],
          effect: "deny",
          masks: {},
          ordinal: 1,
          principals: ["user:blocked@example.com"],
          row_filter: null,
          when: {},
        },
      ],
      {
        claims: {},
        principal: "user:blocked@example.com",
        groups: [],
      },
      ["id", "email"],
    );

    expect(preview).toEqual({
      decision: "deny",
      masks: [],
      matchedOrdinal: 1,
      reason: "Rule 1 denied access.",
      rowFilter: null,
      visibleColumns: [],
    });
  });
});
