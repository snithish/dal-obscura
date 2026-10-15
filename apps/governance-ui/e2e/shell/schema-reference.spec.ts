import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import {
  openDeepReference,
  referenceName,
  referencePath,
  referenceType,
  schemaReferenceApi,
} from "./schema-reference.fixture";
import { uxAssetId } from "../support/ux-fixture";

test("keyboard search reaches a leaf before inspection without changing rule selection", async ({
  page,
}) => {
  await schemaReferenceApi(page);
  await page.goto(`/?asset=${uxAssetId}#assets`);
  await page.getByRole("button", { name: "Show schema", exact: true }).click();
  const sidebar = page.getByRole("complementary", { name: "Asset schema" });
  await expect(sidebar.locator(".schema-results-count")).toHaveText("25 fields · schema v1");
  const search = sidebar.getByRole("searchbox", { name: "Search schema" });
  await search.fill("literal");
  await expect(sidebar.locator(".schema-results-count")).toHaveText("1 matching fields");
  const inspect = sidebar.getByRole("button", { name: `Inspect ${referencePath}`, exact: true });
  await expect(inspect).not.toHaveAttribute("aria-controls", /.*/);
  const initial = await new AxeBuilder({ page }).include("#asset-schema-sidebar").analyze();
  expect(initial.violations).toEqual([]);
  await search.press("Tab");
  await expect(inspect).toBeFocused();
  await inspect.press("Space");
  await expect(sidebar.getByRole("region", { name: "Field details", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Allowed columns: 3 selected" })).toBeVisible();
});

for (const width of [1440, 390, 320]) {
  test(`deep schema inspection preserves exact identity and keyboard focus at ${width}px`, async ({
    page,
  }) => {
    await page.setViewportSize({ width, height: 1100 });
    await schemaReferenceApi(page);
    let writes = 0;
    page.on("request", (request) => {
      if (request.method() !== "GET" && request.url().includes("/v1/assets/")) writes++;
    });
    const sidebar = await openDeepReference(page);
    const row = sidebar.getByRole("button", { name: `Inspect ${referencePath}`, exact: true });
    await row.focus();
    await row.press("Enter");
    await expect(row).toBeFocused();
    const details = sidebar.getByRole("region", { name: "Field details", exact: true });
    await expect(details.locator("[data-field-detail=path]")).toHaveText(referencePath);
    await expect(details.locator("[data-field-detail=name]")).toHaveText(referenceName);
    await expect(details.locator("[data-field-detail=type]")).toHaveText(referenceType);
    const label = row.locator(".schema-field-name");
    expect(
      await label.evaluate((element) => element.getBoundingClientRect().width),
    ).toBeGreaterThan(100);
    expect(await page.evaluate(() => document.documentElement.scrollWidth)).toBeLessThanOrEqual(
      width,
    );
    await details.focus();
    await details.press("End");
    await expect.poll(() => details.evaluate((element) => element.scrollTop)).toBeGreaterThan(0);
    await details.press("Home");
    await expect(details.locator("[data-field-detail=type]")).toBeInViewport();
    expect(writes).toBe(0);
    const result = await new AxeBuilder({ page }).include("#asset-schema-sidebar").analyze();
    expect(result.violations).toEqual([]);
  });
}
