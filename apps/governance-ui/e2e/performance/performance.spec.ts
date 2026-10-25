import { expect, test } from "@playwright/test";
import { assertWebVitals, observeWebVitals } from "../../scripts/web-vitals-budget";
import { authenticatedApi } from "../support/fixtures";

test("schema reference and column picker virtualize 10k fields and keep search responsive", async ({
  page,
}) => {
  await authenticatedApi(page);
  const fields = Array.from({ length: 10_000 }, (_, index) => ({
    field_id: index + 1,
    name: `field_${index}`,
    human_path: `field_${index}`,
    type: "string",
    nullable: true,
    kind: "scalar",
    path: {
      version: 1,
      segments: [{ kind: "field", name: `field_${index}`, field_id: index + 1 }],
    },
  }));
  await page.route("**/v1/assets/00000000-0000-4000-8000-000000000001/schema", async (route) =>
    route.fulfill({
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
    }),
  );
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
  // Keep pointer hover from changing the active row while search replaces the list.
  await page.mouse.move(0, 0);
  await search.fill("");
  const firstOption = picker.getByRole("option", { name: /^field_0\b/ });
  await expect(firstOption).toHaveAttribute("aria-setsize", "10000");
  await expect(firstOption).toHaveClass(/active/);
  const durations: number[] = [];
  for (let index = 0; index < 20; index += 1) {
    const started = await page.evaluate(() => performance.now());
    await search.press("ArrowDown");
    await expect(
      picker.getByRole("option", { name: new RegExp(`^field_${index + 1}\\b`) }),
    ).toHaveClass(/active/);
    const finished = await page.evaluate(() => performance.now());
    durations.push(finished - started);
  }
  durations.sort((left, right) => left - right);
  const testInfo = test.info();
  await testInfo.attach("picker-key-latency", {
    body: JSON.stringify({
      fieldCount: 10000,
      samplesMs: durations,
      p95Ms: durations[Math.floor(durations.length * 0.95)],
    }),
    contentType: "application/json",
  });
  expect(
    durations[Math.floor(durations.length * 0.95)],
    "95th percentile picker key latency",
  ).toBeLessThan(200);
});

test("authenticated policy workspace meets local web-vitals thresholds", async ({ page }) => {
  await authenticatedApi(page);
  await page.addInitScript(observeWebVitals);
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets", {
    waitUntil: "networkidle",
  });
  await expect(page.getByRole("heading", { name: "orders", exact: true })).toBeVisible();
  await page.evaluate(() => document.fonts.ready);
  const metrics = await page.evaluate(() => window.__dalObscuraWebVitals);
  const testInfo = test.info();
  await testInfo.attach("workspace-web-vitals", {
    body: JSON.stringify({ lcpMs: metrics.lcp, cls: metrics.cls }),
    contentType: "application/json",
  });
  assertWebVitals({ lcpMs: metrics.lcp, cls: metrics.cls });
});
