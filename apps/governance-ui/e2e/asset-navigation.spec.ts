import { expect, test } from "@playwright/test";
import { authenticatedApi } from "./fixtures";

test("inventory and authoring are separate destinations with browser history", async ({ page }) => {
  const detailRequests: string[] = [];
  page.on("request", (request) => { if (/\/v1\/assets\/[^/]+\/schema$/.test(new URL(request.url()).pathname)) detailRequests.push(request.url()); });
  await authenticatedApi(page, { ruleEditor: true });
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { level: 1, name: "Assets", exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Add rule", exact: true })).toHaveCount(0);
  expect(detailRequests).toEqual([]);
  await page.getByLabel("Find governed asset").fill("orders");
  await page.getByRole("button", { name: "orders", exact: true }).click();
  await expect(page).toHaveURL(/asset=.+#assets/);
  await expect(page.getByRole("button", { name: "Add rule", exact: true })).toBeVisible();
  await expect(page.getByLabel("Find governed asset")).toHaveCount(0);
  await page.reload();
  await expect(page.getByRole("button", { name: "Add rule", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Back to assets", exact: true }).click();
  await expect(page.getByLabel("Find governed asset")).toBeVisible();
  await page.goBack();
  await expect(page.getByRole("button", { name: "Add rule", exact: true })).toBeVisible();
  await page.goForward();
  await expect(page.getByLabel("Find governed asset")).toBeVisible();
});

test("failed inventory search keeps results and offers retry", async ({ page }) => {
  await authenticatedApi(page);
  await page.goto("/#assets");
  await expect(page.getByRole("button", { name: "orders", exact: true })).toBeVisible();
  await page.route("**/v1/assets/page**", (route) => route.fulfill({ status: 503, json: { detail: "Directory unavailable" } }));
  await page.getByLabel("Find governed asset").fill("orders");
  await expect(page.getByRole("button", { name: "Retry asset search" })).toBeVisible();
  await expect(page.getByRole("button", { name: "orders", exact: true })).toBeVisible();
  await page.unroute("**/v1/assets/page**");
  await page.getByRole("button", { name: "Retry asset search" }).click();
  await expect(page.getByRole("button", { name: "Retry asset search" })).toHaveCount(0);
});

test("returning to inventory preserves search and guards pending authoring", async ({ page }) => {
  await authenticatedApi(page, { ruleEditor: true, initialPolicyRules: [{ ordinal: 10, effect: "allow", principals: ["*"], columns: ["email"], masks: {}, when: {}, row_filter: null }] });
  await page.goto("/#assets");
  await page.getByLabel("Find governed asset").fill("orders");
  await page.getByRole("button", { name: "orders", exact: true }).click();
  await page.getByRole("button", { name: "Add Column Masks", exact: true }).click();
  await page.getByRole("button", { name: "Back to assets", exact: true }).click();
  await page.getByRole("button", { name: "Keep editing", exact: true }).click();
  await expect(page.getByLabel("Mask", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Back to assets", exact: true }).click();
  await page.getByRole("button", { name: "Discard changes", exact: true }).click();
  await expect(page.getByLabel("Find governed asset")).toHaveValue("orders");
  await page.getByRole("button", { name: "orders", exact: true }).click();
  await expect(page.getByLabel("Mask", { exact: true })).toHaveCount(0);
});

test("browser history restores asset tabs and guards unfinished forms", async ({ page }) => {
  await authenticatedApi(page, { ruleEditor: true, initialPolicyRules: [{ ordinal: 10, effect: "allow", principals: ["*"], columns: ["email"], masks: {}, when: {}, row_filter: null }] });
  await page.goto("/#assets");
  await page.getByRole("button", { name: "orders", exact: true }).click();
  await page.getByRole("tab", { name: "Access", exact: true }).click();
  await expect(page).toHaveURL(/tab=access/);
  await page.goBack();
  await expect(page.getByRole("tab", { name: "Policy", exact: true })).toHaveAttribute("aria-selected", "true");
  await page.getByRole("button", { name: "Add Column Masks", exact: true }).click();
  await page.goBack();
  await page.getByRole("button", { name: "Keep editing", exact: true }).click();
  await expect(page.getByLabel("Mask", { exact: true })).toBeVisible();
  await expect(page).toHaveURL(/asset=/);
  await page.goBack();
  await page.getByRole("button", { name: "Discard changes", exact: true }).click();
  await expect(page.getByLabel("Find governed asset")).toBeVisible();
});

test("unavailable deep link can return to inventory", async ({ page }) => {
  await authenticatedApi(page);
  await page.route("**/v1/assets/missing**", (route) => route.fulfill({ status: 404, json: { detail: "Asset not found" } }));
  await page.goto("/?asset=missing#assets");
  await expect(page.getByRole("heading", { name: "Asset unavailable" })).toBeVisible();
  await expect(page.getByLabel("Find governed asset")).toHaveCount(0);
  await page.getByRole("button", { name: "Back to assets" }).click();
  await expect(page.getByRole("button", { name: "orders", exact: true })).toBeVisible();
});

test("inventory fits desktop and mobile without exposing authoring", async ({ page }, testInfo) => {
  await authenticatedApi(page);
  await page.goto("/#assets");
  await expect(page.getByRole("button", { name: "orders", exact: true })).toBeVisible();
  await page.screenshot({ path: testInfo.outputPath("inventory-desktop.png"), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await expect(page.getByRole("button", { name: "Add rule", exact: true })).toHaveCount(0);
  await page.screenshot({ path: testInfo.outputPath("inventory-mobile.png"), fullPage: true });
});
