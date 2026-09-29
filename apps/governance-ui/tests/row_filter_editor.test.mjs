import assert from "node:assert/strict";
import test from "node:test";
import { compileRowFilter, rowFilterType } from "../src/row_filter_editor.ts";

const field = (human_path, type, segments = [{ kind: "field", name: human_path }]) => ({ human_path, type, path: { version: 1, segments } });

test("builder escapes nested identifiers, literal dotted names, and string values", () => {
  const nested = field("person.name", "string", [{ kind: "field", name: "person" }, { kind: "field", name: 'na"me' }]);
  assert.deepEqual(compileRowFilter([{ field: nested, operator: "equals", value: "O'Brien" }]), { sql: `("person"."na""me" = 'O''Brien')` });
  assert.deepEqual(compileRowFilter([{ field: field("a.b", "string"), operator: "equals", value: "x" }]), { sql: `("a.b" = 'x')` });
});

test("builder compiles AND, OR, IN, and NULL operators", () => {
  const country = field("country", "string");
  const active = field("active", "boolean");
  assert.equal(compileRowFilter([{ field: country, operator: "equals", value: "US" }, { field: active, operator: "equals", value: "true" }], "AND").sql, `("country" = 'US') AND ("active" = TRUE)`);
  assert.equal(compileRowFilter([{ field: country, operator: "in", value: ["US", "CA"] }], "OR").sql, `("country" IN ('US', 'CA'))`);
  assert.equal(compileRowFilter([{ field: country, operator: "is_null" }]).sql, `("country" IS NULL)`);
  assert.equal(compileRowFilter([{ field: country, operator: "is_not_null" }]).sql, `("country" IS NOT NULL)`);
});

test("builder rejects invalid numbers, incomplete and unsupported conditions", () => {
  assert.match(compileRowFilter([{ field: field("age", "int64"), operator: "greater", value: "NaN" }]).error, /finite number/);
  assert.match(compileRowFilter([]).error, /at least one/);
  assert.match(compileRowFilter([{ field: field("country", "string"), operator: "equals", value: "" }]).error, /Complete/i);
  assert.match(compileRowFilter([{ field: field("items", "list"), operator: "equals", value: "x" }]).error, /does not support/);
  assert.match(compileRowFilter([{ field: field("items", "string", [{ kind: "field", name: "items" }, { kind: "list_element" }]), operator: "is_null" }]).error, /does not support/);
  assert.equal(rowFilterType("boolean"), "boolean");
});

test("IN conditions reject unfinished values rather than widening the filter", () => {
  assert.match(compileRowFilter([{ field: field("country", "string"), operator: "in", value: ["US", ""] }]).error, /Complete/);
});
