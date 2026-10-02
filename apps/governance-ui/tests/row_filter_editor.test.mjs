import assert from "node:assert/strict";
import test from "node:test";
import { compileRowFilter, rowFilterType } from "../src/row_filter_editor.ts";

const field = (human_path, type, segments = [{ kind: "field", name: human_path }]) => ({ human_path, type, path: { version: 1, segments } });
const country = field("country", "string");
const active = field("active", "boolean");

for (const [name, conditions, join, sql] of [
  ["quoted nested identifiers and apostrophes", [{ field: field("person.name", "string", [{ kind: "field", name: "person" }, { kind: "field", name: 'na"me' }]), operator: "equals", value: "O'Brien" }], "AND", `("person"."na""me" = 'O''Brien')`],
  ["literal dotted name", [{ field: field("a.b", "string"), operator: "equals", value: "x" }], "AND", `("a.b" = 'x')`],
  ["conjunction", [{ field: country, operator: "equals", value: "US" }, { field: active, operator: "equals", value: "true" }], "AND", `("country" = 'US') AND ("active" = TRUE)`],
  ["disjunction", [{ field: country, operator: "equals", value: "US" }, { field: active, operator: "equals", value: "true" }], "OR", `("country" = 'US') OR ("active" = TRUE)`],
  ["IN values", [{ field: country, operator: "in", value: ["US", "CA"] }], "AND", `("country" IN ('US', 'CA'))`],
  ["NULL", [{ field: country, operator: "is_null" }], "AND", `("country" IS NULL)`],
  ["NOT NULL", [{ field: country, operator: "is_not_null" }], "AND", `("country" IS NOT NULL)`],
]) {
  test(`row filter compiles ${name}`, () => assert.deepEqual(compileRowFilter(conditions, join), { sql }));
}

for (const [name, conditions, error] of [
  ["non-finite number", [{ field: field("age", "int64"), operator: "greater", value: "NaN" }], /finite number/],
  ["empty filter", [], /at least one/],
  ["unfinished scalar", [{ field: country, operator: "equals", value: "" }], /Complete/i],
  ["container", [{ field: field("items", "list"), operator: "equals", value: "x" }], /does not support/],
  ["list traversal", [{ field: field("items", "string", [{ kind: "field", name: "items" }, { kind: "list_element" }]), operator: "is_null" }], /does not support/],
  ["unfinished IN value", [{ field: country, operator: "in", value: ["US", ""] }], /Complete/],
]) {
  test(`row filter rejects ${name}`, () => assert.match(compileRowFilter(conditions).error, error));
}

test("boolean schema fields allow boolean comparisons", () => assert.equal(rowFilterType("boolean"), "boolean"));
