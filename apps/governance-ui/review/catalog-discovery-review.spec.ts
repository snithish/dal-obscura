import { mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { expect, test } from "@playwright/test";
import { authenticatedApi } from "../e2e/fixtures";

const phase = process.env.DAL_OBSCURA_REVIEW_PHASE ?? "after";
if (phase !== "before" && phase !== "after") throw new Error("Review phase must be before or after");
const output = resolve(process.env.DAL_OBSCURA_REVIEW_OUTPUT ?? "../../docs/ui-review/catalog-discovery");

for (const width of [1440, 390]) {
  test(`catalog discovery ${phase} at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 1000 });
    await authenticatedApi(page, { admin: true, configuredConnections: true });
    await page.route("**/v1/catalogs/analytics/tables*", async (route) => {
      const url = new URL(route.request().url());
      const limit = Number(url.searchParams.get("limit") ?? "150");
      const start = Number(url.searchParams.get("cursor") ?? "0");
      const end = Math.min(start + limit, 150);
      await route.fulfill({ json: {
        catalog: "analytics", revision: 1, next_cursor: end < 150 ? String(end) : null,
        tables: Array.from({ length: end - start }, (_, offset) => ({
          name: `finance.transactions_${String(start + offset).padStart(4, "0")}`,
          backend: "synthetic.catalog", table_identifier: `finance.transactions_${start + offset}`,
          target: `finance.transactions_${start + offset}`, governed: (start + offset) % 3 === 0,
        })),
      } });
    });
    await page.goto("/#connections");
    await page.getByRole("button", { name: "Discover tables" }).click();
    const results = phase === "after"
      ? page.getByRole("region", { name: "Discovered tables in analytics", exact: true })
      : page.getByRole("region", { name: "Discovered catalog tables table", exact: true });
    await expect(results).toBeVisible();
    await results.evaluate((element) => element.scrollIntoView({ block: "start" }));
    await page.evaluate(() => window.scrollBy(0, -160));
    await page.evaluate(() => document.fonts.ready);
    mkdirSync(output, { recursive: true });
    await page.screenshot({ path: resolve(output, `${phase}-${width}.png`) });
  });
}
