import { expect, test } from "@playwright/test";
import { authenticatedApi, deferredResponse } from "./fixtures";

test.beforeEach(async ({ page }) => {
  page.on("dialog", (dialog) => { throw new Error(`Unexpected native dialog: ${dialog.message()}`); });
});

test("discard dialog preserves edits, traps focus, and defaults to keep editing", async ({ page }, testInfo) => {
  await authenticatedApi(page, { ruleEditor: true });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await page.getByRole("button", { name: "Add rule", exact: true }).click();
  await page.getByLabel("Rule name").fill("Sensitive data");
  const back = page.getByRole("button", { name: "Back to assets", exact: true });
  await back.click();
  const dialog = page.getByRole("dialog", { name: "Discard unsaved changes?" });
  await expect(dialog.getByRole("button", { name: "Keep editing" })).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(page.getByLabel("Rule name")).toHaveValue("Sensitive data");
  await expect(back).toBeFocused();
  await back.click();
  for (let step = 0; step < 5; step += 1) {
    await page.keyboard.press("Tab");
    expect(await dialog.evaluate((element) => element.contains(document.activeElement))).toBe(true);
  }
  const { default: AxeBuilder } = await import("@axe-core/playwright");
  const result = await new AxeBuilder({ page }).include('[role="dialog"]').analyze();
  expect(result.violations.filter((item) => ["serious", "critical"].includes(item.impact ?? ""))).toEqual([]);
  await page.emulateMedia({ colorScheme: "dark" });
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "dark");
  const darkResult = await new AxeBuilder({ page }).include('[role="dialog"]').analyze();
  expect(darkResult.violations.filter((item) => ["serious", "critical"].includes(item.impact ?? ""))).toEqual([]);
  await page.setViewportSize({ width: 390, height: 844 });
  await expect.poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.screenshot({ path: testInfo.outputPath("discard-mobile.png") });
  await dialog.getByRole("button", { name: "Discard changes" }).click();
  await expect(page.getByLabel("Find governed asset")).toBeVisible();
});

test("revocation requires explicit confirmation and prevents repeated submission", async ({ page }) => {
  await authenticatedApi(page, { admin: true });
  const deferred = deferredResponse();
  let requests = 0;
  await page.route("**/tickets/revoke", async (route) => {
    requests += 1; deferred.markStarted(); await deferred.wait();
    await route.fulfill({ json: { revoked_token_count: 2 } });
  });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001&tab=access#assets");
  const revoke = page.getByRole("button", { name: "Revoke all active tokens", exact: true });
  await revoke.click();
  const dialog = page.getByRole("dialog", { name: "Revoke all active tokens?" });
  await expect(dialog).toContainText("orders");
  await dialog.getByRole("button", { name: "Cancel", exact: true }).click();
  expect(requests).toBe(0);
  await revoke.click();
  await dialog.getByRole("button", { name: "Revoke tokens", exact: true }).click();
  await deferred.started;
  await expect(revoke).toBeDisabled();
  expect(requests).toBe(1);
  deferred.release();
  await expect(page.getByText("Revoked 2 active token(s).")).toBeVisible();
  await expect(revoke).toBeEnabled();
});

test("access principal typing keeps focus and refresh cancellation keeps input", async ({ page }) => {
  await authenticatedApi(page, { admin: true });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001&tab=access#assets");
  await page.getByRole("button", { name: "Add capability" }).click();
  const principal = page.getByLabel("Grant principal 1");
  await principal.pressSequentially("group:data-stewards");
  await expect(principal).toHaveValue("group:data-stewards");
  await expect(principal).toBeFocused();
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await page.getByRole("button", { name: "Keep editing", exact: true }).click();
  await expect(principal).toHaveValue("group:data-stewards");
});

test("storage paths keep typing focus and active navigation preserves settings", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredSettings: true });
  await page.goto("/#settings");
  await page.getByRole("button", { name: "Add storage root" }).click();
  const root = page.getByLabel(/Storage path root/).last();
  await root.pressSequentially("s3://warehouse/private");
  await expect(root).toHaveValue("s3://warehouse/private");
  await expect(root).toBeFocused();
  await page.getByRole("button", { name: "Settings", exact: true }).click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(root).toHaveValue("s3://warehouse/private");
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await page.getByRole("button", { name: "Keep editing", exact: true }).click();
  await expect(root).toHaveValue("s3://warehouse/private");
});
