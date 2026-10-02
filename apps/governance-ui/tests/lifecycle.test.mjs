import {test} from "node:test";
import assert from "node:assert/strict";
import {isCurrentEpoch, nextEpoch} from "../src/lifecycle.ts";

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
