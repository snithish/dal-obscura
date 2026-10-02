import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { authenticatedApi } from "./fixtures";

test("authenticated mobile navigation respects capabilities and restores focus", async ({ page }) => {
  await authenticatedApi(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/#assets");

  await expect(page.getByRole("heading", { name: "Assets", exact: true })).toBeVisible();
  const openNavigation = page.getByRole("button", { name: "Open navigation menu" });
  await openNavigation.click();
  await expect(page.getByRole("dialog", { name: "Navigation" })).toBeVisible();
  await expect(page.getByRole("dialog", { name: "Navigation" }).getByRole("button", { name: "Settings" })).toBeDisabled();
  await expect(page.getByRole("dialog", { name: "Navigation" }).getByRole("button", { name: "Assets" })).toHaveAttribute("aria-current", "page");

  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog", { name: "Navigation" })).toHaveCount(0);
  await expect(openNavigation).toBeFocused();

  await openNavigation.click();
  await page.getByRole("dialog", { name: "Navigation" }).getByRole("button", { name: "Activity", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Activity", exact: true })).toBeVisible();
  await expect(page.getByRole("dialog", { name: "Navigation" })).toHaveCount(0);
});

test("authenticated policy workspace stays within the page at supported sizes and zoom", async ({ page }) => {
  await authenticatedApi(page);
  for (const viewport of [{ width: 390, height: 844 }, { width: 768, height: 1024 }, { width: 1440, height: 900 }]) {
    await page.setViewportSize(viewport);
    await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
    await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
    const dimensions = await page.evaluate(() => ({
      documentWidth: document.documentElement.scrollWidth,
      viewportWidth: document.documentElement.clientWidth,
    }));
    expect(dimensions.documentWidth, `${viewport.width}px page overflow`).toBeLessThanOrEqual(dimensions.viewportWidth + 1);
  }

  await page.evaluate(() => { document.body.style.zoom = "2"; });
  const zoomedDimensions = await page.evaluate(() => ({
    documentWidth: document.documentElement.scrollWidth,
    viewportWidth: document.documentElement.clientWidth,
  }));
  expect(zoomedDimensions.documentWidth, "200% zoom page overflow").toBeLessThanOrEqual(zoomedDimensions.viewportWidth + 1);
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
});

test("authenticated reader shell has no serious or critical accessibility violations", async ({ page }) => {
  await authenticatedApi(page);
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "Assets", exact: true })).toBeVisible();

  const results = await new AxeBuilder({ page }).analyze();
  const blockingViolations = results.violations.filter(
    (violation) => violation.impact === "serious" || violation.impact === "critical",
  );
  expect(blockingViolations).toEqual([]);
});

test("reader deep links fail closed before admin settings requests", async ({ page }) => {
  const adminRequests: string[] = [];
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (path.startsWith("/v1/settings/")) adminRequests.push(path);
  });
  await authenticatedApi(page);
  await page.goto("/#settings");

  await expect(page.getByRole("heading", { name: "Assets", exact: true })).toBeVisible();
  await expect.poll(() => new URL(page.url()).hash).toBe("#assets");
  expect(adminRequests).toEqual([]);
});

test("reader deep links fail closed before connection management requests", async ({ page }) => {
  const adminRequests: string[] = [];
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (path.startsWith("/v1/settings/") || path.startsWith("/v1/catalogs") || path.startsWith("/v1/plugins")) {
      adminRequests.push(path);
    }
  });
  await authenticatedApi(page);
  await page.goto("/#connections");

  await expect(page.getByRole("heading", { name: "Assets", exact: true })).toBeVisible();
  await expect.poll(() => new URL(page.url()).hash).toBe("#assets");
  expect(adminRequests).toEqual([]);
});
