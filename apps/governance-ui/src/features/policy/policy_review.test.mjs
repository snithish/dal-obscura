import assert from "node:assert/strict";
import test from "node:test";
import { policyChanges } from "./policy_review.ts";
const rule = {
  ordinal: 10,
  name: "Analysts",
  effect: "allow",
  principals: ["group:analysts"],
  columns: ["email", "region"],
  masks: { email: { type: "email", exempt_principals: ["privacy", "admin"] } },
  row_filter: null,
  when: { region: ["eu", "us"] },
};
test("review ignores object and set order but retains literal replacement values", () => {
  const equivalent = {
    ...rule,
    principals: [...rule.principals],
    columns: [...rule.columns].reverse(),
    masks: { email: { exempt_principals: ["admin", "privacy"], type: "email" } },
    when: { region: ["us", "eu"] },
  };
  assert.deepEqual(policyChanges([rule], [equivalent]), []);
  const updated = {
    ...rule,
    masks: { email: { type: "redact", value: "  keep spaces  " } },
    row_filter: "region = 'EU'",
  };
  const changes = policyChanges([rule], [updated]);
  assert.deepEqual(changes[0].fields, ["Column masks", "Row filter"]);
  assert.equal(changes[0].before.masks.email.type, "email");
  assert.equal(changes[0].after.masks.email.value, "  keep spaces  ");
});
test("review distinguishes added, removed, renamed and bypassed rules without evaluating access", () => {
  const changes = policyChanges(
    [rule, { ...rule, ordinal: 20 }],
    [
      { ...rule, name: "Renamed" },
      { ...rule, ordinal: 30, effect: "allow_all" },
    ],
  );
  assert.deepEqual(
    changes.map((change) => [change.kind, change.fields]),
    [
      ["updated", ["Name"]],
      ["removed", []],
      ["added", []],
    ],
  );
  assert.equal(changes[2].after.effect, "allow_all");
});
