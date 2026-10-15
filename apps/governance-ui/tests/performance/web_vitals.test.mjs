import assert from "node:assert/strict";
import test from "node:test";
import { assertWebVitals } from "../../scripts/web-vitals-budget.mjs";

test("web-vitals gate accepts measurements at documented limits", () => {
  assert.doesNotThrow(() => assertWebVitals({ lcpMs: 2500, cls: 0.1 }));
});

test("web-vitals gate rejects a reported slow largest paint", () => {
  assert.throws(
    () => assertWebVitals({ lcpMs: 3000, cls: 0 }),
    /Largest Contentful Paint.*exceeds/,
  );
});

for (const lcpMs of [null, undefined, NaN, Infinity, "100", -1]) {
  test(`web-vitals gate rejects invalid LCP ${lcpMs}`, () => {
    assert.throws(() => assertWebVitals({ lcpMs, cls: 0 }), /Largest Contentful Paint/);
  });
}

for (const cls of [NaN, Infinity, -0.01, 0.2]) {
  test(`web-vitals gate rejects invalid or excessive CLS ${cls}`, () => {
    assert.throws(() => assertWebVitals({ lcpMs: 100, cls }), /Cumulative Layout Shift/);
  });
}
