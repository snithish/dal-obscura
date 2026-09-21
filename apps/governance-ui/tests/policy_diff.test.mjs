import assert from "node:assert/strict";
import test from "node:test";
import { policyDiff } from "../src/policy_diff.ts";

const baseRule = {
  ordinal: 10,
  effect: "allow",
  principals: ["group:analysts", "user:owner"],
  columns: ["region", "customer.email"],
  masks: { "customer.email": { type: "email" } },
  row_filter: "region = 'EU'",
  when: { tenant: ["analytics", "warehouse"] },
};

test("policy diff normalizes set-like rule fields", () => {
  const reordered = {
    ...baseRule,
    principals: [...baseRule.principals].reverse(),
    columns: [...baseRule.columns].reverse(),
    when: { tenant: ["warehouse", "analytics"] },
  };
  assert.deepEqual(policyDiff([reordered], [baseRule]), { added: 0, removed: 0, changed: 0 });
});

test("policy diff reports additions, removals, and ordinal changes", () => {
  assert.deepEqual(
    policyDiff(
      [{ ...baseRule, row_filter: null }, { ...baseRule, ordinal: 20 }],
      [baseRule, { ...baseRule, ordinal: 30 }],
    ),
    { added: 1, removed: 1, changed: 1 },
  );
});
