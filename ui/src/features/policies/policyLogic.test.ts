import { describe, expect, test } from "vitest";

import {
  canEditPolicy,
  defaultRule,
  describeRuleForm,
  formToRule,
  policyEditorResponsibility,
  previewPolicy,
  ruleToForm,
  validateRuleForms,
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
      rowFilter: "(region = 'us')",
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

  test("unions matching allow rules and combines row filters", () => {
    const preview = previewPolicy(
      [
        {
          columns: ["id"],
          effect: "allow",
          masks: {},
          ordinal: 1,
          principals: ["group:finance"],
          row_filter: "region = 'us'",
          when: {},
        },
        {
          columns: ["email"],
          effect: "allow",
          masks: { email: { type: "email" } },
          ordinal: 2,
          principals: ["group:finance"],
          row_filter: "active = true",
          when: {},
        },
      ],
      {
        claims: {},
        principal: "user:alice@example.com",
        groups: ["finance"],
      },
      ["id", "email", "region"],
    );

    expect(preview).toEqual({
      decision: "allow",
      masks: [{ column: "email", type: "email" }],
      matchedOrdinal: 1,
      reason: "Rules 1, 2 matched.",
      rowFilter: "(region = 'us') AND (active = true)",
      visibleColumns: ["id", "email"],
    });
  });

  test("applies deny precedence across matching rules", () => {
    const preview = previewPolicy(
      [
        {
          columns: ["id", "email"],
          effect: "allow",
          masks: { email: { type: "email" } },
          ordinal: 1,
          principals: ["group:analysts"],
          row_filter: null,
          when: {},
        },
        {
          columns: ["email"],
          effect: "deny",
          masks: {},
          ordinal: 2,
          principals: ["group:analysts"],
          row_filter: null,
          when: { clearance: "low" },
        },
      ],
      {
        claims: { clearance: "low" },
        principal: "user:bob@example.com",
        groups: ["analysts"],
      },
      ["id", "email"],
    );

    expect(preview).toEqual({
      decision: "allow",
      masks: [],
      matchedOrdinal: 1,
      reason: "Rules 1, 2 matched.",
      rowFilter: null,
      visibleColumns: ["id"],
    });
  });

  test("keeps stricter mask when several allow rules mask the same column", () => {
    const preview = previewPolicy(
      [
        {
          columns: ["email"],
          effect: "allow",
          masks: { email: { type: "hash" } },
          ordinal: 1,
          principals: ["group:support"],
          row_filter: null,
          when: {},
        },
        {
          columns: ["email"],
          effect: "allow",
          masks: { email: { type: "redact" } },
          ordinal: 2,
          principals: ["group:support"],
          row_filter: null,
          when: {},
        },
      ],
      {
        claims: {},
        principal: "user:caseworker@example.com",
        groups: ["support"],
      },
      ["email"],
    );

    expect(preview.masks).toEqual([{ column: "email", type: "redact" }]);
  });
});

describe("policy editor responsibility", () => {
  test("explains that policy edits are restricted to assigned owners", () => {
    expect(policyEditorResponsibility(["user:alice@example.com", "group:data-owners"])).toEqual({
      label: "Owner managed",
      message:
        "Policy changes are restricted to platform admins and assigned owners: user:alice@example.com, group:data-owners.",
    });
  });

  test("explains that platform admins must assign owners before handoff", () => {
    expect(policyEditorResponsibility([])).toEqual({
      label: "Needs owner",
      message:
        "Platform admins can seed this policy, then assign owners before handing off ongoing changes.",
    });
  });
});

describe("policy edit permissions", () => {
  test("allows platform admins to edit any asset policy", () => {
    expect(
      canEditPolicy(
        { principal: "platform:admin", groups: [], platform_admin: true },
        ["group:data-owners"],
      ),
    ).toBe(true);
  });

  test("allows direct and group asset owners to edit policy", () => {
    expect(
      canEditPolicy(
        { principal: "user:alice@example.com", groups: [], platform_admin: false },
        ["user:alice@example.com"],
      ),
    ).toBe(true);
    expect(
      canEditPolicy(
        { principal: "user:bob@example.com", groups: ["data-owners"], platform_admin: false },
        ["group:data-owners"],
      ),
    ).toBe(true);
  });

  test("rejects non-owner policy edits", () => {
    expect(
      canEditPolicy(
        { principal: "user:eve@example.com", groups: ["analysts"], platform_admin: false },
        ["group:data-owners"],
      ),
    ).toBe(false);
  });
});

describe("policy rule authoring support", () => {
  test("describes allow rules in a readable summary", () => {
    const form = ruleToForm({
      columns: ["customer_id", "email"],
      effect: "allow",
      masks: { email: { type: "email" } },
      ordinal: 10,
      principals: ["group:us-analysts"],
      row_filter: "region = 'us'",
      when: { purpose: "support" },
    });

    expect(describeRuleForm(form, ["customer_id", "email", "region"])).toEqual({
      title: "Allow group:us-analysts",
      details: [
        "2 visible columns",
        "1 match condition",
        "row filter: region = 'us'",
        "1 mask",
      ],
    });
  });

  test("blocks invalid save payloads before the API call", () => {
    expect(
      validateRuleForms([
        {
          ...defaultRule,
          principalsText: "",
          rowFilter: "region = 'us'",
          masks: [{ column: "email", type: "email" }],
          effect: "deny",
        },
      ]),
    ).toEqual([
      "Rule 1 needs at least one principal.",
      "Rule 1 deny rules cannot include row filters.",
      "Rule 1 deny rules cannot include masks.",
    ]);
  });
});
