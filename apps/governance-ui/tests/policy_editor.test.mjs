import assert from "node:assert/strict";
import test from "node:test";
import { applyMaskToTargets, authoritativeColumnOptions, groupMasks, mixedMaskState, removeColumnSelections, schemaPathCovers } from "../src/policy_editor.ts";

const node = (human_path, segments, children = []) => ({ field_id: 1, name: human_path, human_path, path: { version: 1, segments }, type: children.length ? "struct" : "string", nullable: true, kind: children.length ? "struct" : "scalar", ...(children.length ? { children } : {}) });
const fields = [
  node("profile", [{ kind: "field", field_id: 1, name: "profile" }], [node("profile.email", [{ kind: "field", field_id: 1, name: "profile" }, { kind: "field", field_id: 2, name: "email" }])]),
  node("a.b", [{ kind: "field", field_id: 3, name: "a.b" }]),
  node("phone", [{ kind: "field", field_id: 4, name: "phone" }]),
];
const rule = () => ({ ordinal: 2, effect: "allow", principals: ["group:readers"], columns: ["email", "phone"], masks: { email: { type: "hash" }, phone: { type: "redact" } }, row_filter: "country = 'US'", when: { region: "us" } });

test("authoritative options retain removed selections and distinguish typed paths from dotted names", () => {
  const options = authoritativeColumnOptions(fields, ["missing"]);
  assert.equal(options.at(-1).valid, false);
  assert.equal(schemaPathCovers("profile", "profile.email", options), true);
  assert.equal(schemaPathCovers("a.b", "profile.email", options), false);
  assert.deepEqual(authoritativeColumnOptions(undefined, ["stale"])[0].value, "stale");
});

test("apply mask to two explicit columns and subset preserves unrelated rule data", () => {
  const original = rule();
  const updated = applyMaskToTargets(original, ["email", "phone"], { type: "hash", value: 4 });
  assert.deepEqual(updated.masks, { email: { type: "hash", value: 4 }, phone: { type: "hash", value: 4 } });
  assert.deepEqual(applyMaskToTargets(original, ["phone"], { type: "null" }).masks, { email: { type: "hash" }, phone: { type: "null" } });
  assert.equal(updated.row_filter, original.row_filter);
  assert.deepEqual(updated.principals, original.principals);
  assert.deepEqual(original.masks.phone, { type: "redact" });
});

test("column exemptions clear only targeted masks while keeping access; all-exempt and No mask clear them", () => {
  const original = rule();
  const updated = applyMaskToTargets(original, ["email", "phone"], { type: "hash" }, ["phone"]);
  assert.deepEqual(updated.columns, original.columns);
  assert.deepEqual(updated.masks, { email: { type: "hash" } });
  assert.deepEqual(applyMaskToTargets(original, ["email", "phone"], { type: "hash" }, ["email", "phone"]).masks, {});
  assert.deepEqual(applyMaskToTargets(original, ["email"], undefined).masks, { phone: { type: "redact" } });
});

test("mixed mask state and grouping include unmasked state", () => {
  const value = rule();
  assert.equal(mixedMaskState(value, ["email", "phone"]), true);
  assert.deepEqual(groupMasks(value, ["email", "phone"]).map((item) => item.columns), [["email"], ["phone"]]);
  assert.equal(mixedMaskState({ ...value, masks: { email: { type: "hash" }, phone: { type: "hash" } } }, ["email", "phone"]), false);
  const exempted = { ...value, masks: { email: { type: "hash", exempt_principals: ["group:a"] }, phone: { type: "hash", exempt_principals: ["group:b"] } } };
  assert.equal(mixedMaskState(exempted, ["email", "phone"]), true);
  assert.equal(groupMasks(exempted, ["email", "phone"]).length, 2);
});

test("removing a granted parent clears exclusively covered masks but preserves another grant", () => {
  const options = authoritativeColumnOptions(fields);
  const input = { ...rule(), columns: ["profile", "profile.email", "phone"], masks: { "profile.email": { type: "hash" }, phone: { type: "redact" } } };
  const output = removeColumnSelections(input, ["profile"], options);
  assert.deepEqual(output.columns, ["profile.email", "phone"]);
  assert.deepEqual(output.masks, input.masks);
  const onlyParent = removeColumnSelections({ ...input, columns: ["profile"], masks: { "profile.email": { type: "hash" } } }, ["profile"], options);
  assert.deepEqual(onlyParent.masks, {});
  assert.deepEqual(input.columns, ["profile", "profile.email", "phone"]);
});

test("mask groups treat exemption lists as sets", () => {
  const rule = { columns: ["a", "b"], masks: { a: { type: "hash", exempt_principals: ["b", "a"] }, b: { type: "hash", exempt_principals: ["a", "b"] } } };
  assert.equal(groupMasks(rule, rule.columns).length, 1);
});

test("nested selection shortcuts expand leaves and exclude entire subtrees", async () => {
  const { selectColumnsShortcut } = await import("../src/policy_editor.ts");
  const option = (value, names) => ({ value, label: value, valid: true, type: "string", path: { version: 1, segments: names.map((name) => ({ kind: "field", name })) } });
  const options = [option("a", ["a"]), option("a.b", ["a", "b"]), option("a.c", ["a", "c"]), option('["a.b"]', ["a.b"])];
  assert.deepEqual(selectColumnsShortcut(options, "all"), ["a.b", "a.c", '["a.b"]']);
  assert.deepEqual(selectColumnsShortcut(options, "except", "a"), ['["a.b"]']);
  assert.deepEqual(selectColumnsShortcut(options, "prefix", "a."), ["a.b", "a.c"]);
});

test("parent selection expands nested collection leaves for individual exemptions", async () => {
  const { expandColumnSelections } = await import("../src/policy_editor.ts");
  const parent = { value: "contacts", label: "contacts", valid: true, type: "list", path: { version: 1, segments: [{ kind: "field", name: "contacts" }] } };
  const options = [parent, ...["city", "postcode"].map((name) => ({ ...parent, value: `contacts.$element.details.address.${name}`, path: { version: 1, segments: [...parent.path.segments, { kind: "list_element" }, { kind: "field", name: "details" }, { kind: "field", name: "address" }, { kind: "field", name }] } }))];
  assert.deepEqual(expandColumnSelections(options, ["contacts"]), options.slice(1).map((option) => option.value));
});
