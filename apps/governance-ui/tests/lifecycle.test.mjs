import { test } from "node:test";
import assert from "node:assert/strict";
import { isCurrentEpoch, nextEpoch } from "../src/lifecycle.ts";
import { pageFromHash } from "../src/navigation.ts";

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
