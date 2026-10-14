import { expect, test } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
import { authenticatedApi } from "../support/fixtures";

test("catalog discovery pages replace rows and preserve the current page on failure", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredConnections: true });
  let failNextPage = true;
  const requested: Array<string | null> = [];
  await page.route("**/v1/catalogs/analytics/tables*", async (route) => {
    const url = new URL(route.request().url());
    expect(url.searchParams.get("limit")).toBe("50");
    const cursor = url.searchParams.get("cursor");
    requested.push(cursor);
    if (cursor === "next-page" && failNextPage) {
      failNextPage = false;
      return route.fulfill({ status: 503, json: { detail: "Provider unavailable" } });
    }
    const start = cursor ? 50 : 0;
    await route.fulfill({ json: {
      catalog: "analytics", revision: 4, next_cursor: cursor ? null : "next-page",
      tables: Array.from({ length: 50 }, (_, offset) => ({
        backend: "synthetic.catalog", name: `table-${start + offset}`,
        table_identifier: `default.table-${start + offset}`, target: `table-${start + offset}`, governed: false,
      })),
    } });
  });
  await page.goto("/#connections");
  await page.getByRole("button", { name: "Discover tables" }).click();
  const inventory = page.getByRole("region", { name: "Discovered tables in analytics", exact: true });
  await expect(inventory.getByRole("status")).toHaveText("Page 1 · 50 tables");
  await expect(inventory.getByRole("row")).toHaveCount(51);
  await expect(inventory.getByRole("button", { name: "Previous page" })).toBeDisabled();
  await inventory.getByRole("button", { name: "Next page" }).click();
  await expect(inventory.getByRole("alert")).toContainText("The control plane is temporarily unavailable");
  await expect(inventory.getByRole("cell", { name: "table-0", exact: true })).toBeVisible();
  await inventory.getByRole("button", { name: "Retry page" }).click();
  await expect(inventory.getByRole("status")).toHaveText("Page 2 · 50 tables");
  await expect(inventory.getByRole("row")).toHaveCount(51);
  await expect(inventory.getByRole("cell", { name: "table-0", exact: true })).toHaveCount(0);
  await expect(inventory.getByRole("cell", { name: "table-50", exact: true })).toBeVisible();
  await expect(inventory.getByRole("button", { name: "Next page" })).toBeDisabled();
  await inventory.getByRole("button", { name: "Previous page" }).click();
  await expect(inventory.getByRole("status")).toHaveText("Page 1 · 50 tables");
  expect(requested).toEqual([null, "next-page", "next-page", null]);
});

test("an empty catalog has an explicit discovery result and refresh action", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredConnections: true });
  await page.route("**/v1/catalogs/analytics/tables*", async (route) => route.fulfill({
    json: { catalog: "analytics", revision: 4, tables: [], next_cursor: null },
  }));
  await page.goto("/#connections");
  await page.getByRole("button", { name: "Discover tables" }).click();
  await expect(page.getByText("No tables on this page.")).toBeVisible();
  await expect(page.getByRole("button", { name: "Refresh table discovery" })).toBeVisible();
});

test("catalog configuration changes clear the previous table page", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredConnections: true });
  let revision = 1;
  await page.route("**/v1/catalogs", (route) => route.fulfill({ json: [{
    id: "analytics-catalog", name: "analytics", plugin_id: "synthetic.catalog.iceberg",
    options: { uri: "https://catalog.example" }, revision, status: "configured",
    discovered_table_count: 0, governed_asset_count: 0,
  }] }));
  await page.goto("/#connections");
  await page.getByRole("button", { name: "Discover tables" }).click();
  await expect(page.getByRole("cell", { name: "orders", exact: true })).toBeVisible();
  revision = 2;
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect(page.getByText("Catalog configuration changed. Discover tables again.")).toBeVisible();
  await expect(page.getByRole("cell", { name: "orders", exact: true })).toHaveCount(0);
});

for (const width of [320, 390]) test(`mobile discovery keeps actions accessible at ${width}px`, async ({ page }) => {
  await page.setViewportSize({ width, height: 1000 });
  await authenticatedApi(page, { admin: true, configuredConnections: true });
  await page.route("**/v1/catalogs/analytics/tables*", async (route) => route.fulfill({ json: {
    catalog: "analytics", revision: 4, next_cursor: null,
    tables: [{ name: "finance.very_long_transaction_history_for_regional_reporting", backend: "synthetic.catalog",
      target: "history", table_identifier: "finance.history", governed: false }],
  } }));
  await page.goto("/#connections");
  await page.getByRole("button", { name: "Discover tables" }).click();
  const inventory = page.getByRole("region", { name: "Discovered tables in analytics", exact: true });
  await expect(inventory).toBeVisible();
  await inventory.scrollIntoViewIfNeeded();
  const govern = inventory.getByRole("button", { name: "Govern table" });
  await expect(govern).toBeInViewport();
  await govern.focus();
  await expect(govern).toBeFocused();
  const accessibility = await new AxeBuilder({ page }).include(".catalog-discovery").withTags(["wcag2a", "wcag2aa", "wcag22aa"]).analyze();
  expect(accessibility.violations).toEqual([]);
});
