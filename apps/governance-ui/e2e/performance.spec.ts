import { expect, test } from "@playwright/test";
import { authenticatedApi } from "./fixtures";

declare global {
  interface Window {
    __dalObscuraWebVitals: { lcp: number | null; cls: number };
  }
}

type LayoutShift = PerformanceEntry & { hadRecentInput: boolean; value: number };

test("schema reference and column picker virtualize 10k fields and keep search responsive", async ({ page }) => {
  await authenticatedApi(page);
  const fields = Array.from({ length: 10_000 }, (_, index) => ({
    field_id: index + 1,
    name: `field_${index}`,
    human_path: `field_${index}`,
    type: "string",
    nullable: true,
    kind: "scalar",
    path: { version: 1, segments: [{ kind: "field", name: `field_${index}`, field_id: index + 1 }] },
  }));
  await page.route("**/v1/assets/00000000-0000-4000-8000-000000000001/schema", async (route) => route.fulfill({
    json: {
      asset_id: "00000000-0000-4000-8000-000000000001",
      catalog: "demo",
      target: "demo.orders",
      schema_version: 1,
      schema_fingerprint: "large-schema",
      stable_field_ids: true,
      supported_masks: ["null", "redact", "hash", "email", "keep_last", "default"],
      fields,
    },
  }));
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  await page.getByRole("button", { name: "Show schema", exact: true }).click();
  const sidebar = page.getByRole("complementary", { name: "Asset schema" });
  await expect(sidebar).toBeVisible();
  expect(await sidebar.locator(".schema-reference-row").count()).toBeLessThanOrEqual(12);
  await sidebar.getByRole("searchbox", { name: "Search schema" }).fill("field_9999");
  await expect(sidebar.getByText("field_9999", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "New rule", exact: true }).click();
  await page.getByRole("button", { name: /Allowed columns:/ }).click();
  const picker = page.getByRole("dialog", { name: "Choose allowed columns" });
  expect(await picker.getByRole("option").count()).toBeLessThanOrEqual(12);
  const search = picker.getByRole("combobox", { name: "Search allowed columns" });
  await search.fill("field_9999");
  await expect(picker.getByRole("option", { name: /field_9999/ })).toBeVisible();
  await search.fill("");
  const durations: number[] = [];
  for (let index = 0; index < 20; index += 1) {
    const started = await page.evaluate(() => performance.now());
    await search.press("ArrowDown");
    const finished = await page.evaluate(() => performance.now());
    durations.push(finished - started);
  }
  durations.sort((left, right) => left - right);
  expect(durations[Math.floor(durations.length * 0.95)], "95th percentile picker key latency").toBeLessThan(200);
});

test("authenticated policy workspace meets local web-vitals thresholds", async ({ page }) => {
  await authenticatedApi(page);
  await page.addInitScript(() => {
    window.__dalObscuraWebVitals = { lcp: null, cls: 0 };
    new PerformanceObserver((list) => {
      const last = list.getEntries().at(-1);
      if (last) window.__dalObscuraWebVitals.lcp = last.startTime;
    }).observe({ type: "largest-contentful-paint", buffered: true });
    new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        const shift = entry as LayoutShift;
        if (!shift.hadRecentInput) window.__dalObscuraWebVitals.cls += shift.value;
      }
    }).observe({ type: "layout-shift", buffered: true });
  });
  await page.goto("/#assets", { waitUntil: "networkidle" });
  await page.evaluate(() => document.fonts.ready);
  const metrics = await page.evaluate(() => window.__dalObscuraWebVitals);
  expect(metrics.lcp).not.toBeNull();
  expect(metrics.lcp, "local authenticated LCP").toBeLessThanOrEqual(2500);
  expect(metrics.cls, "local authenticated CLS").toBeLessThanOrEqual(0.1);
});
