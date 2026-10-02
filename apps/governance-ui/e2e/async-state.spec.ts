import { expect, test } from "@playwright/test";
import { authenticatedApi, deferredResponse } from "./fixtures";

test("late catalog discovery cannot replace the current table inventory", async ({ page }) => {
  const deferredDiscovery = deferredResponse();
  await authenticatedApi(page, { admin: true, configuredConnections: true, deferredDiscovery });
  await page.goto("/#connections");
  await expect(page.getByRole("heading", { name: "Catalog connections" })).toBeVisible();
  await page.getByRole("button", { name: "Discover tables" }).click();
  await deferredDiscovery.started;

  await page.getByRole("button", { name: "Assets" }).click();
  await expect(page.getByRole("heading", { name: "Assets", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Connections" }).click();
  await expect(page.getByRole("heading", { name: "Catalog connections" })).toBeVisible();
  await page.getByRole("button", { name: "Discover tables" }).click();
  await expect(page.getByRole("cell", { name: "fresh-orders" })).toBeVisible();

  deferredDiscovery.release();
  await expect(page.getByRole("cell", { name: "fresh-orders" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "stale-orders" })).toHaveCount(0);
});

test("mutation HTML challenges clear private workspace state", async ({ page }) => {
  await authenticatedApi(page);
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");

  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await page.route("**/v1/assets/**/policy", async (route) => route.fulfill({
    status: 200,
    headers: { "content-type": "text/html" },
    body: "<html><title>Sign in</title></html>",
  }));
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
  await expect(page.getByText("The browser or edge session expired. Sign in again to continue.")).toBeVisible();
  await expect(page.getByRole("heading", { name: "orders" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Save policy", exact: true })).toHaveCount(0);
});

test("late management responses cannot replace the current page", async ({ page }) => {
  const deferredAudit = deferredResponse();
  await authenticatedApi(page, { deferredAudit });
  await page.goto("/#activity");
  await deferredAudit.started;

  await page.getByRole("button", { name: "Assets", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Assets", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Activity", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Workspace status" })).toBeVisible();
  await expect(page.getByText("fresh-audit")).toBeVisible();

  deferredAudit.release();
  await expect(page.getByText("fresh-audit")).toBeVisible();
  await expect(page.getByText("stale-audit")).toHaveCount(0);
});

test("late settings responses cannot replace the current administrator page", async ({ page }) => {
  const deferredSettings = deferredResponse();
  await authenticatedApi(page, { admin: true, deferredSettings });
  await page.goto("/#settings");
  await deferredSettings.started;

  await page.getByRole("button", { name: "Assets" }).click();
  await expect(page.getByRole("heading", { name: "Assets", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Settings" }).click();
  await expect(page.getByRole("heading", { name: "Runtime and identity" })).toBeVisible();
  await expect(page.getByLabel("Ticket TTL (seconds)")).toHaveValue("22");

  deferredSettings.release();
  await expect(page.getByLabel("Ticket TTL (seconds)")).toHaveValue("22");
});

test("late connection responses cannot replace the current administrator page", async ({ page }) => {
  const deferredConnections = deferredResponse();
  await authenticatedApi(page, { admin: true, deferredConnections });
  await page.goto("/#connections");
  await deferredConnections.started;

  await page.getByRole("button", { name: "Assets" }).click();
  await expect(page.getByRole("heading", { name: "Assets", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Connections" }).click();
  await expect(page.getByRole("heading", { name: "Catalog connections" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "fresh-catalog" })).toBeVisible();

  deferredConnections.release();
  await expect(page.getByRole("heading", { name: "fresh-catalog" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "stale-catalog" })).toHaveCount(0);
});

test("late asset lookup responses cannot replace the current inventory", async ({ page }) => {
  const deferredInventory = deferredResponse();
  await authenticatedApi(page, { deferredInventory });
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "Assets", exact: true })).toBeVisible();

  const search = page.getByLabel("Find governed asset");
  await search.fill("stale");
  await deferredInventory.started;

  await page.getByRole("button", { name: "Activity", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Activity", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Assets" }).click();
  await expect(page.getByRole("heading", { name: "Assets", exact: true })).toBeVisible();
  await expect(search).toBeEnabled();
  await search.fill("fresh");
  await expect(page.getByRole("region", { name: "Governed assets table" })).toContainText("fresh-orders");

  deferredInventory.release();
  await expect(page.getByRole("region", { name: "Governed assets table" })).toContainText("fresh-orders");
  await expect(page.getByRole("region", { name: "Governed assets table" })).not.toContainText("stale-orders");
});

test("late live policy saves cannot clear a newer local edit", async ({ page }) => {
  const deferredSave = deferredResponse();
  await authenticatedApi(page, { deferredSave });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  await deferredSave.started;
  await page.getByRole("button", { name: "New rule", exact: true }).click();
  await expect(page.getByRole("button", { name: "Save policy" })).toBeVisible();
  await expect(page.getByText("Unsaved changes")).toBeVisible();

  deferredSave.release();
  await expect(page.getByText("Unsaved changes")).toBeVisible();
  await expect(page.getByText("Live policy saved.")).toHaveCount(0);
});

test("late policy evaluations cannot replace a newer unsaved edit", async ({ page }) => {
  const deferredEvaluate = deferredResponse();
  await authenticatedApi(page, { deferredEvaluate });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  await page.getByRole("button", { name: "Test Policy", exact: true }).click();
  await page.getByRole("dialog", { name: "Test policy", exact: true }).getByRole("button", { name: "Test saved policy", exact: true }).click();
  await deferredEvaluate.started;
  await page.getByRole("dialog", { name: "Test policy", exact: true }).getByRole("button", { name: "Close policy test", exact: true }).click();
  await page.getByRole("button", { name: "New rule", exact: true }).click();
  await expect(page.getByText("Unsaved changes")).toBeVisible();

  deferredEvaluate.release();
  await expect(page.getByText("Unsaved changes")).toBeVisible();
  await expect(page.getByText("Server-side evaluation completed: denied.")).toHaveCount(0);
});
