import { mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { expect, test } from "@playwright/test";
import { uxApi, uxAssetId } from "../e2e/support/ux-fixture";
import { authenticatedApi } from "../e2e/support/fixtures";

import { reviewPhase as phase } from "./phase";
const output = resolve("../../docs/ui-review/governance-ux-plan");
test("inventory, policy and error feedback visual review", async ({ page }) => {
  mkdirSync(output, { recursive: true });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await uxApi(page);
  await page.route("**/v1/audit/events/page?*", (route) =>
    route.fulfill({ json: { items: [], next_cursor: null } }),
  );
  await page.goto("/#assets");
  await expect(
    page.getByRole("button", { name: "retail.customer_revenue", exact: true }),
  ).toBeVisible();
  if (phase === "after")
    await expect(page.getByRole("region", { name: "Inventory summary" })).toBeVisible();
  await page.evaluate(() => document.fonts.ready);
  await page.screenshot({
    animations: "disabled",
    path: resolve(output, `${phase}-inventory.png`),
    fullPage: true,
  });
  await page.goto(`/?asset=${uxAssetId}#assets`);
  await expect(page.getByLabel("Rule name", { exact: true })).toBeVisible();
  await page.screenshot({
    animations: "disabled",
    path: resolve(output, `${phase}-policy.png`),
    fullPage: true,
  });
  if (phase === "after") {
    await page.getByRole("button", { name: "Open value lens", exact: true }).click();
    await expect(page.getByRole("dialog", { name: "Value lens", exact: true })).toBeVisible();
    await page.screenshot({
      animations: "disabled",
      path: resolve(output, "after-value-lens.png"),
    });
    await page.setViewportSize({ width: 390, height: 844 });
    await page.emulateMedia({ colorScheme: "dark" });
    await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "dark");
    await page.screenshot({
      animations: "disabled",
      path: resolve(output, "after-value-lens-mobile-dark.png"),
    });
    await page.setViewportSize({ width: 1440, height: 1000 });
    await page.emulateMedia({ colorScheme: "light" });
  }
  await authenticatedApi(page, { admin: true });
  await page.goto("/#connections");
  await page.getByRole("button", { name: "Save connection", exact: true }).click();
  await expect(page.getByText("Connection name is required.", { exact: true })).toBeVisible();
  await page.screenshot({
    animations: "disabled",
    path: resolve(output, `${phase}-error.png`),
    fullPage: true,
  });
});
