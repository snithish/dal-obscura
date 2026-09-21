import assert from "node:assert/strict";
import test from "node:test";
import { assetPath } from "../src/api.ts";

test("asset API paths encode the complete identifier as one route segment", () => {
  assert.equal(assetPath("asset-42"), "/v1/assets/asset-42");
  assert.equal(assetPath("tenant/orders?redirect=https://evil.example"), "/v1/assets/tenant%2Forders%3Fredirect%3Dhttps%3A%2F%2Fevil.example");
  assert.equal(assetPath("../private"), "/v1/assets/..%2Fprivate");
});
