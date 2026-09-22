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
  assert.deepEqual(policyDiff([reordered], [baseRule]), { added: 0, removed: 0, changed: 0, changes: [] });
});

test("policy diff reports additions, removals, and ordinal changes", () => {
  const { changes, ...counts } = policyDiff(
      [{ ...baseRule, row_filter: null }, { ...baseRule, ordinal: 20 }],
      [baseRule, { ...baseRule, ordinal: 30 }],
  );
  assert.deepEqual(counts, { added: 1, removed: 1, changed: 1 });
  assert.deepEqual(changes.filter((change) => change.ordinal === 10), [
    { ordinal: 10, field: "Rows", before: "region = 'EU'", after: "No restriction" },
  ]);
});

test("policy diff exposes each changed field in review order", () => {
  const draft = { ...baseRule, effect: "deny", principals: ["group:finance"], columns: ["region"], masks: { region: { type: "redact" } }, when: { tenant: "finance" } };
  const changes = policyDiff([draft], [baseRule]).changes;

  assert.deepEqual(changes.map(({ ordinal, field }) => [ordinal, field]), [
    [10, "Effect"],
    [10, "Principals"],
    [10, "Fields"],
    [10, "Masks"],
    [10, "Conditions"],
  ]);
  assert.equal(changes.find((change) => change.field === "Masks")?.before, "customer.email: {\"type\":\"email\"}");
  assert.equal(changes.find((change) => change.field === "Masks")?.after, "region: {\"type\":\"redact\"}");
});
