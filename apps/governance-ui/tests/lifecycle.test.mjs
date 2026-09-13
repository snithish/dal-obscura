import { test } from "node:test";
import assert from "node:assert/strict";
import { isCurrentEpoch, nextEpoch } from "../src/lifecycle.ts";
import { locationFromUrl, pageFromHash } from "../src/navigation.ts";
import { recoveryMessage } from "../src/recovery.ts";

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

test("typed asset locations preserve review draft and tab deep links", () => {
  assert.deepEqual(
    locationFromUrl("#assets", "?asset=asset-42&draft=draft-7&tab=history&version=3"),
    { page: "assets", assetId: "asset-42", draftId: "draft-7", tab: "history", version: 3 },
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

test("recovery mapper covers every governed HTTP recovery status", () => {
  const messages = [403, 404, 409, 422, 429, 503].map((status) => recoveryMessage({ status }, "fallback"));
  assert.equal(new Set(messages).size, 6);
  assert.ok(messages.every((message) => message !== "fallback"));
});
