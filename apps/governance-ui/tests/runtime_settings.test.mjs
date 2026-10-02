import assert from "node:assert/strict";
import test from "node:test";
import { serializePathRules } from "../src/runtime_settings.ts";

for (const [name, roots, expected] of [
  ["trimmed roots", [" s3://warehouse/curated ", "file:///tmp/data"], [{ root: "s3://warehouse/curated" }, { root: "file:///tmp/data" }]],
  ["empty rules", [], []],
  ["blank root", ["  "], undefined],
]) {
  test(`runtime path rules handle ${name}`, () => assert.deepEqual(serializePathRules(roots), expected));
}
