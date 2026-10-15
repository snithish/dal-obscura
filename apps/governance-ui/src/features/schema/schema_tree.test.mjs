import assert from "node:assert/strict";
import test from "node:test";
import { flattenSchemaTree } from "./schema_tree.ts";

const node = (human_path, children = []) => ({
  field_id: 1,
  name: human_path.split(".").at(-1),
  path: { version: 1, segments: [{ kind: "field", name: human_path }] },
  human_path,
  type: children.length ? "struct" : "string",
  nullable: true,
  kind: children.length ? "struct" : "scalar",
  ...(children.length ? { children } : {}),
});

test("flattenSchemaTree only includes expanded descendants", () => {
  const tree = [
    node("profile", [
      node("profile.email"),
      node("profile.address", [node("profile.address.city")]),
    ]),
  ];
  assert.deepEqual(flattenSchemaTree(tree, new Set()), [
    { node: tree[0], depth: 0, index: 0, posinset: 1, setsize: 1 },
  ]);
  const expanded = flattenSchemaTree(tree, new Set(["profile", "profile.address"]));
  assert.deepEqual(
    expanded.map(({ node: value, depth, index }) => [value.human_path, depth, index]),
    [
      ["profile", 0, 0],
      ["profile.email", 1, 1],
      ["profile.address", 1, 2],
      ["profile.address.city", 2, 3],
    ],
  );
  assert.deepEqual(
    expanded.map(({ node: value, posinset, setsize }) => [value.human_path, posinset, setsize]),
    [
      ["profile", 1, 1],
      ["profile.email", 1, 2],
      ["profile.address", 2, 2],
      ["profile.address.city", 1, 1],
    ],
  );
});

test("forceExpanded exposes all descendants for search", () => {
  const tree = [node("root", [node("root.child", [node("root.child.leaf")])])];
  assert.deepEqual(
    flattenSchemaTree(tree, new Set(), true).map(({ node: value, depth, index }) => [
      value.human_path,
      depth,
      index,
    ]),
    [
      ["root", 0, 0],
      ["root.child", 1, 1],
      ["root.child.leaf", 2, 2],
    ],
  );
});

test("flattening a synthetic deep chain does not use the JavaScript call stack", () => {
  let root = node("leaf");
  for (let depth = 19_999; depth >= 0; depth--) root = node(`level_${depth}`, [root]);
  const rows = flattenSchemaTree([root], new Set(), true);
  assert.equal(rows.length, 20_001);
  assert.equal(rows.at(-1).depth, 20_000);
  assert.equal(rows.at(-1).node.human_path, "leaf");
});
