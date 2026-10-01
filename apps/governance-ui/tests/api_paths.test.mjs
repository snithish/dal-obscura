import assert from "node:assert/strict";
import test from "node:test";
import { assetPath, controlPlane } from "../src/api.ts";

test("asset API paths encode the complete identifier as one route segment", () => {
  assert.equal(assetPath("asset-42"), "/v1/assets/asset-42");
  assert.equal(assetPath("catalog/orders?redirect=https://evil.example"), "/v1/assets/catalog%2Forders%3Fredirect%3Dhttps%3A%2F%2Fevil.example");
  assert.equal(assetPath("../private"), "/v1/assets/..%2Fprivate");
});

test("asset policy normalization preserves mask exemption metadata", async () => {
  const originalFetch = globalThis.fetch;
  const originalDocument = globalThis.document;
  globalThis.document = { cookie: "" };
  globalThis.fetch = async () => new Response(JSON.stringify({
    id: "asset-42", revision: 1, policy_revision: 2, catalog: "analytics", name: "orders", backend: "iceberg", table_identifier: "orders",
    owners: [], owner_count: 0, policy_status: "configured", options: {}, schema_fields: [],
    policy_rules: [{ ordinal: 0, effect: "allow", principals: ["group:analysts"], columns: ["email"], masks: { email: { type: "hash", exempt_principals: ["group:privacy-reviewers"] } }, row_filter: null, when: {} }],
  }), { headers: { "content-type": "application/json" } });
  try {
    const asset = await controlPlane.getAsset("asset-42");
    assert.deepEqual(asset.policy_rules[0].masks.email.exempt_principals, ["group:privacy-reviewers"]);
  } finally {
    globalThis.fetch = originalFetch;
    if (originalDocument === undefined) delete globalThis.document; else globalThis.document = originalDocument;
  }
});
