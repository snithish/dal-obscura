import { expect, test } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
import { wideSelectionApi } from "./support/wide-selection";

for (const width of [1440, 390, 320]) {
  test(`10k selections stay bounded and support keyboard review at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 1100 });
    const writes: string[] = [];
    page.on("request", request => { if (request.method() === "PUT") writes.push(request.url()); });
    await wideSelectionApi(page);
    const summary = page.getByRole("group", { name: "Selected columns", exact: true }).first();
    expect(await summary.locator(".column-chip").count()).toBeLessThanOrEqual(5);
    await summary.getByRole("button", { name: "Review all 10000 selected columns", exact: true }).click();
    const picker = page.getByRole("dialog", { name: "Choose allowed columns" });
    const review = picker.getByRole("complementary", { name: "Selection review" });
    await expect(review).toBeVisible();
    const search = review.getByRole("searchbox", { name: "Search selected columns" });
    await expect(search).toBeFocused();
    expect(await review.getByRole("button", { name: /^Remove / }).count()).toBeLessThanOrEqual(12);
    const durations: number[] = [];
    for (let index = 0; index < 20; index += 1) {
      const started = await page.evaluate(() => performance.now());
      await search.fill(`field_${9980 + index}`);
      await expect(review.getByRole("status")).toHaveText("1 of 10000 selected fields");
      durations.push(await page.evaluate(() => performance.now()) - started);
    }
    durations.sort((left, right) => left - right);
    expect(durations[Math.ceil(durations.length * 0.95) - 1], "p95 local selected-field search latency").toBeLessThan(200);
    await expect(review.getByRole("status")).toHaveText("1 of 10000 selected fields");
    await search.press("Tab");
    const remove = review.getByRole("button", { name: "Remove field_9999", exact: true });
    await expect(remove).toBeFocused();
    await remove.press("Enter");
    await expect(search).toBeFocused();
    await expect(review.getByRole("status")).toHaveText("0 of 9999 selected fields");
    await search.fill("field_9998");
    await expect(review.getByRole("button", { name: "Remove field_9998", exact: true })).toBeVisible();
    expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
    expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    await picker.getByRole("button", { name: "Done", exact: true }).click();
    await expect(page.getByRole("button", { name: "Allowed columns: 9999 selected", exact: true })).toBeFocused();
    await expect(page.getByText("Unsaved changes", { exact: true })).toBeVisible();
    expect(writes).toEqual([]);
  });
}
