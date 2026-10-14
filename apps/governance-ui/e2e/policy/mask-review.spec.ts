import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { wideSelectionApi } from "../support/wide-selection";

for (const width of [1440, 390, 320]) {
  test(`wide mask review is bounded, searchable and read-only at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    const writes: string[] = [];
    page.on("request", request => { if (request.method() === "PUT") writes.push(request.url()); });
    await wideSelectionApi(page, 5000, true);
    const card = page.locator(".mask-card").first();
    await expect(card.locator(".policy-paths code")).toHaveCount(5);
    const summary = card.locator("summary");
    await summary.focus();
    await summary.press("Enter");
    const search = card.getByRole("searchbox", { name: "Search masked columns" });
    await search.fill("field_4999");
    await expect(card.getByText("1 of 5000 columns", { exact: true })).toBeVisible();
    await expect(card.locator(".column-path-review code")).toHaveText(["field_4999"]);
    await search.fill("missing-field");
    await expect(card.getByText("No matching columns", { exact: true })).toBeVisible();
    await search.fill("");
    await expect(card.locator(".column-path-review code")).toHaveCount(50);
    await card.getByRole("button", { name: "Next columns", exact: true }).click();
    await expect(card.getByText("51–100 of 5000 matches", { exact: true })).toBeVisible();
    await expect(card.locator(".column-path-review code").first()).toHaveText("field_50");
    await expect(card.locator(".column-path-review code").last()).toHaveText("field_99");
    const accessibility = await new AxeBuilder({ page }).include(".bulk-mask-editor").analyze();
    expect(accessibility.violations).toEqual([]);
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    await summary.focus();
    await summary.press("Enter");
    await expect(summary).toBeFocused();
    await expect(search).toBeHidden();
    await expect(page.getByRole("button", { name: "Discard all changes", exact: true })).toBeDisabled();
    expect(writes).toEqual([]);
  });
}

test("wide original-value review keeps scope and does not change the rule", async ({ page }) => {
  const writes: string[] = [];
  page.on("request", request => { if (request.method() === "PUT") writes.push(request.url()); });
  await wideSelectionApi(page, 5000);
  await page.getByRole("button", { name: "Add Column Masks", exact: true }).click();
  await page.getByRole("button", { name: "Cancel mask changes", exact: true }).click();
  const original = page.locator(".unmasked-columns");
  await original.locator(":scope > summary").click();
  await expect(original.locator(".policy-paths code")).toHaveCount(5);
  await original.locator(".column-path-summary summary").click();
  const search = original.getByRole("searchbox", { name: "Search columns showing original values" });
  await search.fill("field_4999");
  await expect(original.locator(".column-path-review code")).toHaveText(["field_4999"]);
  await expect(page.getByRole("button", { name: "Allowed columns: 5000 selected", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Discard all changes", exact: true })).toBeDisabled();
  expect(writes).toEqual([]);
});
