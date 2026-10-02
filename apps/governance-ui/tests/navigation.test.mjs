import assert from "node:assert/strict";
import test from "node:test";
import { locationFromUrl, pageFromHash } from "../src/navigation.ts";

for (const [hash, expected] of [
  ["#connections", "connections"],
  ["#settings?tab=auth", "settings"],
  ["#private-script", "assets"],
]) {
  test(`navigation resolves ${hash}`, () => assert.equal(pageFromHash(hash), expected));
}

for (const [name, search, expected] of [
  ["retired draft parameters", "?asset=asset-42&draft=draft-7&draft_revision=4&tab=history&version=3", { page: "assets", assetId: "asset-42" }],
  ["unknown tab", "?tab=unknown&version=0", { page: "assets" }],
  ["retired test tab", "?asset=asset-42&tab=tests", { page: "assets", assetId: "asset-42" }],
]) {
  test(`asset location ignores ${name}`, () => assert.deepEqual(locationFromUrl("#assets", search), expected));
}
