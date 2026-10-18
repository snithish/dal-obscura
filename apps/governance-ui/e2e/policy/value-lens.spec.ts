import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { wideSelectionApi } from "../support/wide-selection";
import { uxApi, uxAssetId } from "../support/ux-fixture";

test("value lens is synthetic, neutral for stored values, and resolves subtrees on demand", async ({
  page,
}) => {
  await uxApi(page);
  await page.goto(`/?asset=${uxAssetId}#assets`);
  await expect(page.locator(".value-lens-row")).toHaveCount(0);
  await page.getByRole("button", { name: "Open value lens", exact: true }).click();
  const lens = page.getByRole("dialog", { name: "Value lens", exact: true });
  await expect(lens.getByText(/Client-side simulation/)).toBeVisible();
  await lens.getByRole("button", { name: "customer", exact: true }).click();
  await expect(lens.getByRole("status")).toHaveText("Subtree: customer");
  await lens.getByLabel("Find schema path").fill("customer.email");
  await expect(lens.getByText('"sample@example.invalid"', { exact: true })).toBeVisible();
  await expect(lens.locator(".tally-stored")).toHaveCount(0);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("button", { name: "Open value lens", exact: true })).toBeFocused();
});
test("10k-field value lens mounts at most twelve rows while scrolling and searching", async ({
  page,
}) => {
  await wideSelectionApi(page);
  await page.getByRole("button", { name: "Open value lens", exact: true }).click();
  const lens = page.getByRole("dialog", { name: "Value lens", exact: true });
  expect(await lens.locator(".value-lens-row").count()).toBeLessThanOrEqual(12);
  await lens.getByRole("region", { name: "Synthetic field values" }).evaluate((element) => {
    element.scrollTop = element.scrollHeight;
  });
  await expect(lens.getByText("field_9999", { exact: true })).toBeVisible();
  expect(await lens.locator(".value-lens-row").count()).toBeLessThanOrEqual(12);
  await lens.getByLabel("Find schema path").fill("field_9999");
  await expect(lens.locator(".value-lens-row")).toHaveCount(1);
});

for (const width of [390, 320]) {
  test(`value lens fits and stays accessible at ${width}px in both themes`, async ({ page }) => {
    await page.setViewportSize({ width, height: 844 });
    await uxApi(page);
    await page.goto(`/?asset=${uxAssetId}#assets`);
    await page.getByRole("button", { name: "Open value lens", exact: true }).click();
    const lens = page.getByRole("dialog", { name: "Value lens", exact: true });
    for (const colorScheme of ["light", "dark"] as const) {
      await page.emulateMedia({ colorScheme });
      await expect(lens).toBeVisible();
      await expect(lens).toHaveCSS("opacity", "1");
      await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", colorScheme);
      expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(
        true,
      );
      expect((await new AxeBuilder({ page }).analyze()).violations).toEqual([]);
    }
  });
}
