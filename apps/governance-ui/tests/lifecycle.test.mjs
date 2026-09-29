import { test } from "node:test";
import assert from "node:assert/strict";
import { isCurrentEpoch, nextEpoch } from "../src/lifecycle.ts";
import { locationFromUrl, pageFromHash } from "../src/navigation.ts";
import { recoveryMessage } from "../src/recovery.ts";
import { serializePathRules } from "../src/runtime_settings.ts";

test("stale async work cannot apply after an epoch advances", () => {
  const initial = 0;
  const requestEpoch = nextEpoch(initial);
  const newerRequestEpoch = nextEpoch(requestEpoch);

  assert.equal(isCurrentEpoch(requestEpoch, newerRequestEpoch), false);
  assert.equal(isCurrentEpoch(newerRequestEpoch, newerRequestEpoch), true);
});

test("epoch advancement is monotonic", () => {
  assert.equal(nextEpoch(4), 5);
});

test("hash navigation accepts governed pages and defaults unknown values safely", () => {
  assert.equal(pageFromHash("#connections"), "connections");
  assert.equal(pageFromHash("#settings?tab=auth"), "settings");
  assert.equal(pageFromHash("#private-script"), "assets");
});

test("asset locations accept current tabs and discard retired draft parameters", () => {
  assert.deepEqual(
    locationFromUrl("#assets", "?asset=asset-42&draft=draft-7&draft_revision=4&tab=history&version=3"),
    { page: "assets", assetId: "asset-42" },
  );
  assert.deepEqual(locationFromUrl("#assets", "?tab=unknown&version=0"), { page: "assets" });
});

test("recovery messages distinguish actionable HTTP failures and retain request IDs", () => {
  assert.equal(
    recoveryMessage({ status: 409, requestId: "req-17" }, "fallback"),
    "This resource changed on the server. Refresh it before retrying. Request ID: req-17",
  );
  assert.equal(
    recoveryMessage({ status: 503 }, "fallback"),
    "The control plane is temporarily unavailable. Your current local state remains unchanged.",
  );
  assert.equal(recoveryMessage({ status: 418 }, "fallback"), "fallback");
});

test("recovery messages explain an HTML authentication challenge", () => {
  assert.equal(
    recoveryMessage({ status: 401, code: "auth_challenge" }, "fallback"),
    "The browser or edge session expired. Sign in again to continue.",
  );
});

test("recovery mapper covers every governed HTTP recovery status", () => {
  const messages = [403, 404, 409, 422, 429, 503].map((status) => recoveryMessage({ status }, "fallback"));
  assert.equal(new Set(messages).size, 6);
  assert.ok(messages.every((message) => message !== "fallback"));
});

test("recovery mapper names safe validation fields", () => {
  assert.equal(
    recoveryMessage({ status: 422, fieldErrors: [{ field: "options.uri", message: "invalid", type: "value_error" }] }, "fallback"),
    "The server rejected the submitted values. Check the validation details and retry. options.uri: invalid",
  );
});

test("runtime path rules trim roots and reject blank rows", () => {
  assert.deepEqual(serializePathRules([" s3://warehouse/curated ", "file:///tmp/data"]), [
    { root: "s3://warehouse/curated" },
    { root: "file:///tmp/data" },
  ]);
  assert.deepEqual(serializePathRules([]), []);
  assert.equal(serializePathRules(["  "]), undefined);
});

test("removed Tests tab is not restored through old URLs", () => {
  assert.deepEqual(locationFromUrl("#assets", "?asset=asset-42&tab=tests"), { page: "assets", assetId: "asset-42" });
});
