import assert from "node:assert/strict";
import test from "node:test";
import { simulateMask } from "./value_lens.ts";

test("synthetic mask values preserve NULL and stored values", async () => {
  assert.equal(await simulateMask(null, { type: "redact", value: "hidden" }), null);
  assert.equal(await simulateMask("a@example.com", undefined), "a@example.com");
  assert.equal(await simulateMask("a@example.com", { type: "email" }), "a***@example.com");
  assert.equal(await simulateMask("not-email", { type: "email" }), null);
  assert.equal(await simulateMask("123456", { type: "keep_last", value: 2 }), "****56");
  assert.equal(await simulateMask("short", { type: "keep_last", value: 0 }), "*****");
  assert.equal(await simulateMask("value", { type: "redact", value: "hidden" }), "hidden");
  assert.equal(await simulateMask("value", { type: "null" }), null);
  assert.equal(await simulateMask("value", { type: "default", value: false }), false);
  assert.equal(
    await simulateMask("abc", { type: "hash" }),
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
  );
});

test("synthetic string masks count Unicode characters rather than UTF-16 units", async () => {
  assert.equal(await simulateMask("😀123", { type: "keep_last", value: 4 }), "😀123");
  assert.equal(await simulateMask("😀123", { type: "keep_last", value: 3 }), "*123");
  assert.equal(await simulateMask("😀person@example.com", { type: "email" }), "😀***@example.com");
});
