import assert from "node:assert/strict";
import test from "node:test";
import { preserveSecretReference } from "../src/connection_options.ts";

test("preserveSecretReference round-trips only a scoped reference", () => {
  assert.deepEqual(
    preserveSecretReference({ secret: "catalog/token", scope: "catalog:analytics" }),
    { secret: "catalog/token", scope: "catalog:analytics" },
  );
  assert.equal(preserveSecretReference({ secret: "[redacted]", scope: "catalog:analytics", extra: true }), undefined);
  assert.equal(preserveSecretReference("token-value"), undefined);
});
