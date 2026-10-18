import assert from "node:assert/strict";
import test from "node:test";
import {
  applyMaskToTargets,
  authoritativeColumnOptions,
  groupMasks,
  removeColumnSelections,
  columnOptionCovers,
} from "./policy_editor.ts";

const node = (human_path, segments, children = []) => ({
  field_id: 1,
  name: human_path,
  human_path,
  path: { version: 1, segments },
  type: children.length ? "struct" : "string",
  nullable: true,
  kind: children.length ? "struct" : "scalar",
  ...(children.length ? { children } : {}),
});
const fields = [
  node(
    "profile",
    [{ kind: "field", field_id: 1, name: "profile" }],
    [
      node("profile.email", [
        { kind: "field", field_id: 1, name: "profile" },
        { kind: "field", field_id: 2, name: "email" },
      ]),
    ],
  ),
  node("a.b", [{ kind: "field", field_id: 3, name: "a.b" }]),
  node("phone", [{ kind: "field", field_id: 4, name: "phone" }]),
];
const rule = () => ({
  ordinal: 2,
  effect: "allow",
  principals: ["group:readers"],
  columns: ["email", "phone"],
  masks: { email: { type: "hash" }, phone: { type: "redact" } },
  row_filter: "country = 'US'",
  when: { region: "us" },
});

test("authoritative options retain removed selections and distinguish typed paths from dotted names", () => {
  const options = authoritativeColumnOptions(fields, ["missing"]);
  assert.equal(options.at(-1).valid, false);
  assert.equal(columnOptionCovers(options[0], options[1]), true);
  assert.equal(columnOptionCovers(options[2], options[1]), false);
  assert.deepEqual(authoritativeColumnOptions(undefined, ["stale"])[0].value, "stale");
});

test("apply mask to two explicit columns and subset preserves unrelated rule data", () => {
  const original = rule();
  const updated = applyMaskToTargets(original, ["email", "phone"], { type: "hash", value: 4 });
  assert.deepEqual(updated.masks, {
    email: { type: "hash", value: 4 },
    phone: { type: "hash", value: 4 },
  });
  assert.deepEqual(applyMaskToTargets(original, ["phone"], { type: "null" }).masks, {
    email: { type: "hash" },
    phone: { type: "null" },
  });
  assert.equal(updated.row_filter, original.row_filter);
  assert.deepEqual(updated.principals, original.principals);
  assert.deepEqual(original.masks.phone, { type: "redact" });
});

test("column exemptions clear only targeted masks while keeping access; all-exempt and No mask clear them", () => {
  const original = rule();
  const updated = applyMaskToTargets(original, ["email", "phone"], { type: "hash" }, ["phone"]);
  assert.deepEqual(updated.columns, original.columns);
  assert.deepEqual(updated.masks, { email: { type: "hash" } });
  assert.deepEqual(
    applyMaskToTargets(original, ["email", "phone"], { type: "hash" }, ["email", "phone"]).masks,
    {},
  );
  assert.deepEqual(applyMaskToTargets(original, ["email"], undefined).masks, {
    phone: { type: "redact" },
  });
});

test("mask grouping distinguishes equal masks and exemptions", () => {
  const value = rule();
  assert.deepEqual(
    groupMasks(value, ["email", "phone"]).map((item) => item.columns),
    [["email"], ["phone"]],
  );
  assert.deepEqual(
    groupMasks({ ...value, masks: { email: { type: "hash" }, phone: { type: "hash" } } }, [
      "email",
      "phone",
    ]),
    [{ mask: { type: "hash" }, columns: ["email", "phone"] }],
  );
  const exempted = {
    ...value,
    masks: {
      email: { type: "hash", exempt_principals: ["group:a"] },
      phone: { type: "hash", exempt_principals: ["group:b"] },
    },
  };
  assert.deepEqual(groupMasks(exempted, ["email", "phone"]), [
    { mask: { type: "hash", exempt_principals: ["group:a"] }, columns: ["email"] },
    { mask: { type: "hash", exempt_principals: ["group:b"] }, columns: ["phone"] },
  ]);
});

test("removing a granted parent clears exclusively covered masks but preserves another grant", () => {
  const options = authoritativeColumnOptions(fields);
  const input = {
    ...rule(),
    columns: ["profile", "profile.email", "phone"],
    masks: { "profile.email": { type: "hash" }, phone: { type: "redact" } },
  };
  const output = removeColumnSelections(input, ["profile"], options);
  assert.deepEqual(output.columns, ["profile.email", "phone"]);
  assert.deepEqual(output.masks, input.masks);
  const onlyParent = removeColumnSelections(
    { ...input, columns: ["profile"], masks: { "profile.email": { type: "hash" } } },
    ["profile"],
    options,
  );
  assert.deepEqual(onlyParent.masks, {});
  assert.deepEqual(input.columns, ["profile", "profile.email", "phone"]);
});

test("mask groups treat exemption lists as sets", () => {
  const rule = {
    columns: ["a", "b"],
    masks: {
      a: { type: "hash", exempt_principals: ["b", "a"] },
      b: { type: "hash", exempt_principals: ["a", "b"] },
    },
  };
  const groups = groupMasks(rule, rule.columns);
  assert.deepEqual(
    groups.map(({ columns }) => columns),
    [["a", "b"]],
  );
  assert.deepEqual(
    groups.map(({ mask }) => ({ type: mask.type, exemptions: [...mask.exempt_principals].sort() })),
    [{ type: "hash", exemptions: ["a", "b"] }],
  );
});

test("parent selection expands nested collection leaves for individual exemptions", async () => {
  const { expandColumnSelections } = await import("./policy_editor.ts");
  const parent = {
    value: "contacts",
    label: "contacts",
    valid: true,
    type: "list",
    path: { version: 1, segments: [{ kind: "field", name: "contacts" }] },
  };
  const options = [
    parent,
    ...["city", "postcode"].map((name) => ({
      ...parent,
      value: `contacts.$element.details.address.${name}`,
      path: {
        version: 1,
        segments: [
          ...parent.path.segments,
          { kind: "list_element" },
          { kind: "field", name: "details" },
          { kind: "field", name: "address" },
          { kind: "field", name },
        ],
      },
    })),
  ];
  assert.deepEqual(
    expandColumnSelections(options, ["contacts"]),
    options.slice(1).map((option) => option.value),
  );
});

test("authoritative options traverse a synthetic deep schema without the JavaScript call stack", () => {
  let child;
  for (let index = 19999; index >= 0; index -= 1) {
    child = node(
      `level-${index}`,
      [{ kind: "field", name: `level-${index}` }],
      child ? [child] : [],
    );
  }
  const options = authoritativeColumnOptions([child], ["retired"]);
  assert.equal(options.length, 20001);
  assert.equal(options[0].value, "level-0");
  assert.equal(options[19999].value, "level-19999");
  assert.deepEqual(options.at(-1), {
    value: "retired",
    label: "retired",
    type: "Removed from schema",
    valid: false,
  });
});

test("mask coverage distinguishes literal names, collection segments, and historical field IDs", () => {
  const options = authoritativeColumnOptions([
    node(
      "items",
      [{ kind: "field", name: "items", field_id: 1 }],
      [
        node("items.$element.key", [
          { kind: "field", name: "items", field_id: 1 },
          { kind: "list_element" },
          { kind: "field", name: "key", field_id: 2 },
        ]),
      ],
    ),
    node("literal", [{ kind: "field", name: "items.$element.key", field_id: 3 }]),
    node("reused", [{ kind: "field", name: "items", field_id: 4 }]),
  ]);
  const input = { ...rule(), columns: ["items"], masks: {} };
  assert.deepEqual(
    applyMaskToTargets(
      input,
      ["items.$element.key", "literal", "reused"],
      { type: "hash" },
      [],
      options,
    ).masks,
    { "items.$element.key": { type: "hash" } },
  );
  const withMasks = {
    ...input,
    columns: ["items.$element.key", "literal"],
    masks: { items: { type: "hash" }, literal: { type: "redact" }, orphan: { type: "null" } },
  };
  assert.deepEqual(removeColumnSelections(withMasks, ["items.$element.key"], options).masks, {
    literal: { type: "redact" },
    orphan: { type: "null" },
  });
});
