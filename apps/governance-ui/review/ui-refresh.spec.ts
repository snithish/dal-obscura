import { mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { expect, test } from "@playwright/test";
import { uxApi, uxAssetId } from "../e2e/support/ux-fixture";
import { authenticatedApi } from "../e2e/support/fixtures";
import { reviewPhase } from "./phase";

const output = resolve("../../docs/ui-review/ux-review-2026-10-25");
test("capture governance UI refresh", async ({ page }) => {
  mkdirSync(output, { recursive: true });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await uxApi(page);
  await page.goto("/#assets");
  await expect(
    page.getByRole("button", { name: "retail.customer_revenue", exact: true }),
  ).toBeVisible();
  await page.screenshot({ path: resolve(output, `${reviewPhase}-inventory.png`), fullPage: true });
  await page.goto(`/?asset=${uxAssetId}#assets`);
  await expect(page.getByLabel("Rule name", { exact: true })).toBeVisible();
  await page.screenshot({ path: resolve(output, `${reviewPhase}-policy.png`), fullPage: true });
  if (reviewPhase === "after") {
    await page.getByLabel("Rule name", { exact: true }).fill("Reviewed regional analysts");
    await page.getByRole("button", { name: "Review changes", exact: true }).click();
    const review = page.getByRole("dialog", { name: "Review policy changes", exact: true });
    await expect(review).toHaveCSS("opacity", "1");
    await page.screenshot({ path: resolve(output, "after-save-review.png") });
    await review.getByRole("button", { name: "Keep editing", exact: true }).click();
    await page.getByRole("button", { name: "Discard all changes", exact: true }).click();
  }
  await page.setViewportSize({ width: 390, height: 844 });
  await page.emulateMedia({ colorScheme: "dark" });
  await page.getByRole("button", { name: "Open value lens", exact: true }).click();
  const lens = page.getByRole("dialog", { name: "Value lens", exact: true });
  await expect(lens).toBeVisible();
  await expect(lens).toHaveCSS("opacity", "1");
  if (reviewPhase === "after")
    await lens
      .getByRole("region", { name: "Selected synthetic field value" })
      .scrollIntoViewIfNeeded();
  await page.screenshot({
    path: resolve(output, `${reviewPhase}-preview-mobile.png`),
    fullPage: false,
  });
  await authenticatedApi(page, { admin: true });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.emulateMedia({ colorScheme: "light" });
  await page.goto("/#connections");
  await page.getByRole("button", { name: "Save connection", exact: true }).click();
  await expect(page.getByText("Connection name is required.", { exact: true })).toBeVisible();
  await page.screenshot({
    path: resolve(output, `${reviewPhase}-connection-error.png`),
    fullPage: true,
  });
});
