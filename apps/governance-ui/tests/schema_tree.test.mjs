import assert from "node:assert/strict";
import test from "node:test";
import { flattenSchemaTree } from "../src/schema_tree.ts";

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
  const tree = [node("profile", [node("profile.email"), node("profile.address", [node("profile.address.city")])])];
  assert.deepEqual(flattenSchemaTree(tree, new Set()), [
    { node: tree[0], depth: 0, index: 0, posinset: 1, setsize: 1 },
  ]);
  const expanded = flattenSchemaTree(tree, new Set(["profile", "profile.address"]));
  assert.deepEqual(expanded.map(({ node: value, depth, index }) => [value.human_path, depth, index]), [
    ["profile", 0, 0],
    ["profile.email", 1, 1],
    ["profile.address", 1, 2],
    ["profile.address.city", 2, 3],
  ]);
});

test("forceExpanded exposes all descendants for search", () => {
  const tree = [node("root", [node("root.child", [node("root.child.leaf")])])];
  assert.equal(flattenSchemaTree(tree, new Set(), true).length, 3);
});
