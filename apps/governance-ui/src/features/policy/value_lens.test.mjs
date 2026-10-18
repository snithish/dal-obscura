import assert from "node:assert/strict";
import test from "node:test";
import { simulateMask } from "./value_lens.ts";

test("synthetic mask values preserve NULL and stored values", async () => {
  assert.equal(await simulateMask(null, { type: "redact", value: "hidden" }), null);
  assert.equal(await simulateMask("a@example.com", undefined), "a@example.com");
  assert.equal(await simulateMask("a@example.com", { type: "email" }), "a***@example.com");
  assert.equal(await simulateMask("not-email", { type: "email" }), null);
  assert.equal(await simulateMask("123456", { type: "keep_last", value: 2 }), "****56");
  assert.equal(await simulateMask("short", { type: "keep_last", value: 0 }), "*****");
  assert.equal(await simulateMask("value", { type: "redact", value: "hidden" }), "hidden");
  assert.equal(await simulateMask("value", { type: "null" }), null);
  assert.equal(await simulateMask("value", { type: "default", value: false }), false);
  assert.equal(
    await simulateMask("abc", { type: "hash" }),
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
  );
});

test("coverage expands typed parents and preserves child masks and conflicts", async () => {
  const { createRuleCoverage } = await import("./policy_editor.ts");
  const field = (name, segments) => ({
    value: name,
    label: name,
    type: "string",
    valid: true,
    path: {
      version: 1,
      segments: segments.map((name, index) => ({ kind: "field", name, field_id: index + 1 })),
    },
  });
  const options = [
    field("person", ["person"]),
    field("person.email", ["person", "email"]),
    field("person.phone", ["person", "phone"]),
    field("person.literal.dot", ["person.literal.dot"]),
  ];
  const rule = {
    ordinal: 1,
    effect: "allow",
    principals: ["*"],
    columns: ["person"],
    masks: { "person.phone": { type: "hash" } },
    row_filter: null,
  };
  const covered = createRuleCoverage(rule, options);
  assert.deepEqual(covered.rule.columns, ["person.email", "person.phone"]);
  assert.deepEqual(covered.rule.masks, { "person.phone": { type: "hash" } });
  const conflict = createRuleCoverage(
    { ...rule, masks: { person: { type: "email" }, "person.phone": { type: "hash" } } },
    options,
  );
  assert.deepEqual([...conflict.conflicts], ["person.phone"]);
  assert.deepEqual(conflict.rule.columns, ["person.email"]);
});

test("synthetic string masks count Unicode characters rather than UTF-16 units", async () => {
  assert.equal(await simulateMask("😀123", { type: "keep_last", value: 4 }), "😀123");
  assert.equal(await simulateMask("😀123", { type: "keep_last", value: 3 }), "*123");
  assert.equal(await simulateMask("😀person@example.com", { type: "email" }), "😀***@example.com");
});
