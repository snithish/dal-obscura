import assert from "node:assert/strict";
import test from "node:test";
import { recoveryMessage } from "../src/recovery.ts";

// Explicit expectations: do not derive expected messages from the production map.
for (const [name, failure, expected] of [
  ["forbidden", { status: 403 }, "Your account is authenticated but is not allowed to perform this action."],
  ["missing resource", { status: 404 }, "The requested resource is no longer available. Refresh the workspace and choose it again."],
  ["conflict with request ID", { status: 409, requestId: "req-17" }, "This resource changed on the server. Refresh it before retrying. Request ID: req-17"],
  ["validation", { status: 422 }, "The server rejected the submitted values. Check the validation details and retry."],
  ["missing revision", { status: 428 }, "A current revision is required. Reload this resource before saving."],
  ["rate limit", { status: 429 }, "Too many requests were made. Wait a moment and retry."],
  ["unavailable", { status: 503 }, "The control plane is temporarily unavailable. Your current local state remains unchanged."],
  ["unknown status", { status: 418 }, "fallback"],
  ["HTML challenge", { status: 401, code: "auth_challenge" }, "The browser or edge session expired. Sign in again to continue."],
]) {
  test(`recovery explains ${name}`, () => assert.equal(recoveryMessage(failure, "fallback"), expected));
}

test("recovery identifies the rejected validation field", () => {
  assert.equal(
    recoveryMessage({ status: 422, fieldErrors: [{ field: "options.uri", message: "invalid", type: "value_error" }] }, "fallback"),
    "The server rejected the submitted values. Check the validation details and retry. options.uri: invalid",
  );
});
