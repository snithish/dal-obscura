import { expect, test } from "@playwright/test";
import { uxApi, uxAssetId } from "../support/ux-fixture";

test.beforeEach(async ({ page }) => {
  await uxApi(page);
  await page.goto(`/?asset=${uxAssetId}#assets`);
});
test("sign out confirms unsaved policy edits and cancellation preserves them", async ({ page }) => {
  const name = page.getByLabel("Rule name", { exact: true });
  await name.fill("Unsaved rule");
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Discard unsaved changes?" });
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Keep editing" }).click();
  await expect(name).toHaveValue("Unsaved rule");
});
test("removing an applied row filter confirms and cancellation retains SQL", async ({ page }) => {
  await page.getByRole("button", { name: "Add Row Filter", exact: true }).click();
  await page.getByLabel("Filter value 1", { exact: true }).fill("EU");
  await page.getByRole("button", { name: "DuckDB SQL", exact: true }).click();
  const sql = page.getByLabel("DuckDB SQL row filter");
  await sql.fill("country = 'EU'");
  await page.getByRole("button", { name: "Apply row filter", exact: true }).click();
  await page.getByRole("button", { name: "All rows", exact: true }).click();
  const dialog = page.getByRole("dialog", { name: "Discard unsaved changes?" });
  await expect(dialog).toBeVisible();
  await dialog.getByRole("button", { name: "Keep editing" }).click();
  await expect(sql).toHaveValue("country = 'EU'");
});
test("collapsed pending edits remain discoverable beside the disabled save", async ({ page }) => {
  await page.getByRole("button", { name: "Add Row Filter", exact: true }).click();
  await page.locator(".rule-disclosure").first().click();
  const review = page.getByRole("button", {
    name: "Review unapplied edits in 1 rule",
    exact: true,
  });
  await expect(review).toBeVisible();
  await review.click();
  await expect(page.getByRole("button", { name: "Discard row changes" })).toBeFocused();
});
