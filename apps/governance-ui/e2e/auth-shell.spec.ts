import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";
import { authenticatedApi, deferredResponse, edgeChallengeApi, signedOutApi, type EdgeChallengeOptions } from "./fixtures";

async function assertEdgeChallenge(page: Page, options: EdgeChallengeOptions) {
  await edgeChallengeApi(page, options);
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
  await expect(page.getByText("The browser or edge session expired. Sign in again to continue.")).toBeVisible();
  await expect(page.getByText("Use demo persona")).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "orders" })).toHaveCount(0);
  await expect(page.getByLabel("Find governed asset")).toHaveCount(0);
}

test.describe("signed-out governance shell", () => {
  test.beforeEach(async ({ page }) => signedOutApi(page));
  test("gates a protected deep link behind normal sign-in", async ({ page }) => {
    await page.goto("/#assets");

    await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
    await expect(page.getByLabel("Local control-plane token")).toBeVisible();
    await expect(page.getByRole("button", { name: "Assets" })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Connections" })).toBeDisabled();
  });

  test("does not restore the retired demo bypass", async ({ page }) => {
    await page.goto("/?demo#assets");

    await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
    await expect(page.getByText("Not signed in")).toBeVisible();
    await expect(page.getByText("Use demo persona")).toHaveCount(0);
  });

  test("HTML edge challenges never become API success", async ({ page }) => {
    await assertEdgeChallenge(page, {});
  });

  test("forbidden HTML edge challenges never become API success", async ({ page }) => {
    await assertEdgeChallenge(page, { status: 403 });
  });

  test("redirected edge challenges never become API success", async ({ page }) => {
    await assertEdgeChallenge(page, { redirect: true });
  });

  test("has no serious or critical accessibility violations when signed out", async ({ page }) => {
    await page.goto("/#assets");

    const results = await new AxeBuilder({ page }).analyze();
    const blockingViolations = results.violations.filter(
      (violation) => violation.impact === "serious" || violation.impact === "critical",
    );
    expect(blockingViolations).toEqual([]);
  });

  test("keeps keyboard focus in the public shell", async ({ page }) => {
    await page.goto("/#assets");

    await page.keyboard.press("Tab");
    await expect(page.getByRole("link", { name: /OBSCURA GOVERNANCE/i })).toBeFocused();
  });

  test("keeps sign-in usable at a narrow viewport", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto("/#assets");

    await expect(page.getByRole("button", { name: "Open navigation menu" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
    await expect(page.getByLabel("Local control-plane token")).toBeVisible();
  });

  test("keeps signed-out actions inside the viewport at supported sizes and zoom", async ({ page }) => {
    for (const viewport of [{ width: 390, height: 844 }, { width: 768, height: 1024 }, { width: 1440, height: 900 }]) {
      await page.setViewportSize(viewport);
      await page.goto("/");
      const login = page.getByRole("heading", { name: "Sign in to your workspace" });
      const token = page.getByLabel("Local control-plane token");
      const button = page.getByRole("button", { name: "Sign in locally" });
      await expect(login).toBeVisible();
      await expect(token).toBeVisible();
      await expect(button).toBeVisible();
      for (const element of [login, token, button]) {
        const box = await element.boundingBox();
        expect(box).not.toBeNull();
        expect(box!.x).toBeGreaterThanOrEqual(0);
        expect(box!.x + box!.width).toBeLessThanOrEqual(viewport.width + 1);
      }
    }

    await page.evaluate(() => { document.body.style.zoom = "2"; });
    const zoomedButton = page.getByRole("button", { name: "Sign in locally" });
    await expect(zoomedButton).toBeVisible();
    const zoomedBox = await zoomedButton.boundingBox();
    expect(zoomedBox).not.toBeNull();
    expect(zoomedBox!.x).toBeGreaterThanOrEqual(0);
    expect(zoomedBox!.x + zoomedBox!.width).toBeLessThanOrEqual(1440 + 1);
  });
});

test("authenticated mobile navigation respects capabilities and restores focus", async ({ page }) => {
  await authenticatedApi(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/#assets");

  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  const openNavigation = page.getByRole("button", { name: "Open navigation menu" });
  await openNavigation.click();
  await expect(page.getByRole("dialog", { name: "Navigation" })).toBeVisible();
  await expect(page.getByRole("dialog", { name: "Navigation" }).getByRole("button", { name: "Settings" })).toBeDisabled();
  await expect(page.getByRole("dialog", { name: "Navigation" }).getByRole("button", { name: "Assets" })).toHaveAttribute("aria-current", "page");

  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog", { name: "Navigation" })).toHaveCount(0);
  await expect(openNavigation).toBeFocused();

  await openNavigation.click();
  await page.getByRole("dialog", { name: "Navigation" }).getByRole("button", { name: "Activity" }).click();
  await expect(page.getByRole("heading", { name: "Activity", exact: true })).toBeVisible();
  await expect(page.getByRole("dialog", { name: "Navigation" })).toHaveCount(0);
});

test("authenticated reader shell has no serious or critical accessibility violations", async ({ page }) => {
  await authenticatedApi(page);
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  const results = await new AxeBuilder({ page }).analyze();
  const blockingViolations = results.violations.filter(
    (violation) => violation.impact === "serious" || violation.impact === "critical",
  );
  expect(blockingViolations).toEqual([]);
});

test("administrator management routes load through the authenticated shell", async ({ page }) => {
  await authenticatedApi(page, { admin: true });
  await page.goto("/#connections");

  await expect(page.getByRole("heading", { name: "Catalog connections" })).toBeVisible();
  await expect(page.getByText("No catalogs configured")).toBeVisible();
  await page.getByRole("button", { name: "Settings" }).click();
  await expect(page.getByRole("heading", { name: "Runtime and identity" })).toBeVisible();
  await expect(page.getByText("No identity providers configured.")).toBeVisible();

  const results = await new AxeBuilder({ page }).analyze();
  const blockingViolations = results.violations.filter(
    (violation) => violation.impact === "serious" || violation.impact === "critical",
  );
  expect(blockingViolations).toEqual([]);
});

test("administrator access management submits owner and grant changes", async ({ page }) => {
  await authenticatedApi(page, { admin: true });
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await page.getByRole("tab", { name: "Access" }).click();
  await expect(page.getByRole("heading", { name: "Owners and delegated capabilities" })).toBeVisible();

  await page.getByLabel("Owner principals").fill("alex@example.invalid, steward@example.invalid");
  await page.getByRole("button", { name: "Save owners" }).click();
  await expect(page.getByText("Owners updated. Existing drafts and publications are unchanged.")).toBeVisible();

  await page.getByRole("button", { name: "Add capability" }).click();
  await page.getByLabel("Grant principal 1").fill("group:data-stewards");
  await page.getByRole("button", { name: "Save capabilities" }).click();
  await expect(page.getByText("Delegated capabilities updated. Changes take effect on the next authorized request.")).toBeVisible();
});

test("administrator settings submits staged runtime changes", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredSettings: true });
  await page.goto("/#settings");
  await expect(page.getByRole("heading", { name: "Runtime and identity" })).toBeVisible();
  await expect(page.getByLabel("Ticket TTL (seconds)")).toHaveValue("11");

  await page.getByLabel("Ticket TTL (seconds)").fill("30");
  await page.getByRole("button", { name: "Save runtime settings" }).click();
  await expect(page.getByText("Runtime settings saved as draft configuration. Publish to make worker behavior change.")).toBeVisible();
});

test("administrator catalogs save, diagnose, and discover through admitted plugins", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredConnections: true });
  await page.goto("/#connections");
  await expect(page.getByRole("heading", { name: "Catalog connections" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "analytics" })).toBeVisible();

  await page.getByRole("button", { name: "Check connection" }).click();
  await expect(page.getByText("Catalog reachable · 1 tables")).toBeVisible();
  await page.getByRole("button", { name: "Discover tables" }).click();
  await expect(page.getByRole("cell", { name: "orders" })).toBeVisible();

  await page.getByLabel("Name").fill("warehouse");
  await page.getByLabel("Catalog URI").fill("https://warehouse.example");
  await page.getByLabel("Password secret reference").fill("catalog/warehouse");
  await page.getByRole("button", { name: "Save connection" }).click();
  await expect(page.getByText("Connection saved. Discovery remains bounded to this configured catalog.")).toBeVisible();
});

test("administrator publication lifecycle creates and activates a workspace snapshot", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredConnections: true, configuredPublications: true });
  await page.goto("/#connections");
  await expect(page.getByRole("heading", { name: "Catalog connections" })).toBeVisible();
  await expect(page.getByText("publication-act", { exact: false })).toBeVisible();

  await page.getByRole("button", { name: "Create snapshot" }).click();
  await expect(page.getByText("Configuration snapshot created. Activate it when ready.")).toBeVisible();
  await expect(page.getByText("publication-sta", { exact: false })).toBeVisible();

  await page.getByRole("button", { name: "Activate" }).click();
  await expect(page.getByText("Configuration snapshot activated for new data-plane requests.")).toBeVisible();
  const activeRow = page.getByRole("row").filter({ hasText: "bbbbbbbbbbbb" });
  await expect(activeRow).toContainText("Active");
  await expect(activeRow).toContainText("Serving");
  const previousRow = page.getByRole("row").filter({ hasText: "aaaaaaaaaaaa" });
  await expect(previousRow).toContainText("Staged");
  await expect(previousRow.getByRole("button", { name: "Activate" })).toBeVisible();
  await expect(page.getByText("Serving", { exact: true })).toHaveCount(1);
});

test("administrator plugin lifecycle applies disable and retire transitions", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredConnections: true, configuredLifecycle: true });
  await page.goto("/#connections");
  await expect(page.getByRole("heading", { name: "Catalog connections" })).toBeVisible();

  const catalogPlugin = page.locator("article.plugin-card").filter({ hasText: "Synthetic Iceberg Catalog" });
  await catalogPlugin.getByLabel("Lifecycle for Synthetic Iceberg Catalog").selectOption("disabled");
  await catalogPlugin.getByRole("button", { name: "Apply" }).click();
  await expect(page.getByText("Synthetic Iceberg Catalog lifecycle is now disabled.")).toBeVisible();
  await expect(catalogPlugin.getByLabel("Lifecycle for Synthetic Iceberg Catalog")).toHaveValue("disabled");

  const formatPlugin = page.locator("article.plugin-card").filter({ hasText: "Synthetic Iceberg" }).filter({ hasNotText: "Catalog" });
  await formatPlugin.getByLabel("Lifecycle for Synthetic Iceberg").selectOption("revoked");
  await formatPlugin.getByRole("button", { name: "Apply" }).click();
  await expect(page.getByText("Synthetic Iceberg lifecycle is now revoked.")).toBeVisible();
  await formatPlugin.getByLabel("Lifecycle for Synthetic Iceberg").selectOption("removed");
  await expect(formatPlugin.getByRole("button", { name: "Apply" })).toBeEnabled();
  page.once("dialog", (dialog) => dialog.accept());
  const removalResponse = page.waitForResponse((response) => response.url().includes("/v1/plugins/table_format/synthetic.table.iceberg/lifecycle") && response.request().method() === "PATCH");
  await formatPlugin.getByRole("button", { name: "Apply" }).click();
  await expect((await removalResponse).status()).toBe(200);
  await expect(page.getByText("Synthetic Iceberg lifecycle is now removed.")).toBeVisible();
});

test("publisher completes the saved draft review and publication journey", async ({ page }) => {
  await authenticatedApi(page, { allowPublish: true });
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  await page.getByRole("button", { name: "Save deny-all draft" }).click();
  await expect(page.getByText("Policy draft saved to the control plane.")).toBeVisible();
  await page.getByRole("button", { name: "Review for publish" }).click();
  await expect(page.getByText("Server review is current for this saved draft revision. You can publish it now.")).toBeVisible();
  await page.getByRole("button", { name: "Publish reviewed deny-all" }).click();
  await expect(page.getByText("Published the saved draft.")).toBeVisible();
});

test("administrator activity filters and paginates the permitted audit records", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredAudit: true });
  await page.goto("/#activity");
  await expect(page.getByRole("heading", { name: "Workspace status" })).toBeVisible();
  await page.getByLabel("Action").fill("policy.draft.save");
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(page.getByText("policy.draft.save")).toBeVisible();
  await expect(page.getByText("policy.publish")).toHaveCount(0);

  await page.getByRole("button", { name: "Load more activity" }).click();
  await expect(page.getByText("policy.publish")).toBeVisible();
});

test("late catalog discovery cannot replace the current table inventory", async ({ page }) => {
  const deferredDiscovery = deferredResponse();
  await authenticatedApi(page, { admin: true, configuredConnections: true, deferredDiscovery });
  await page.goto("/#connections");
  await expect(page.getByRole("heading", { name: "Catalog connections" })).toBeVisible();
  await page.getByRole("button", { name: "Discover tables" }).click();
  await deferredDiscovery.started;

  await page.getByRole("button", { name: "Assets" }).click();
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
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
  await page.goto("/#assets");

  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await page.route("**/v1/assets/**/draft", async (route) => route.fulfill({
    status: 200,
    headers: { "content-type": "text/html" },
    body: "<html><title>Sign in</title></html>",
  }));
  await page.getByRole("button", { name: "Save deny-all draft" }).click();
  await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
  await expect(page.getByText("The browser or edge session expired. Sign in again to continue.")).toBeVisible();
  await expect(page.getByRole("heading", { name: "orders" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Save deny-all draft" })).toHaveCount(0);
});

test("stale review links remain explicit and read-only", async ({ page }) => {
  await authenticatedApi(page);
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001&draft=old-draft&draft_revision=2#assets");

  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await expect(page.getByText(/This review link is stale: it requested draft revision 2/)).toBeVisible();
  await expect(page.getByRole("button", { name: "Save deny-all draft" })).toBeDisabled();
  await expect(page.getByLabel("Find governed asset")).toBeDisabled();
});

test("late management responses cannot replace the current page", async ({ page }) => {
  const deferredAudit = deferredResponse();
  await authenticatedApi(page, { deferredAudit });
  await page.goto("/#activity");
  await deferredAudit.started;

  await page.getByRole("button", { name: "Changes" }).click();
  await expect(page.getByRole("heading", { name: "Published policy history" })).toBeVisible();
  await page.getByRole("button", { name: "Activity" }).click();
  await expect(page.getByRole("heading", { name: "Workspace status" })).toBeVisible();
  await expect(page.getByText("fresh-audit")).toBeVisible();

  deferredAudit.release();
  await expect(page.getByText("fresh-audit")).toBeVisible();
  await expect(page.getByText("stale-audit")).toHaveCount(0);
});

test("late history responses cannot replace the current changes page", async ({ page }) => {
  const deferredHistory = deferredResponse();
  await authenticatedApi(page, { deferredHistory });
  await page.goto("/#changes");
  await deferredHistory.started;

  await page.getByRole("button", { name: "Activity" }).click();
  await expect(page.getByRole("heading", { name: "Workspace status" })).toBeVisible();
  await page.getByRole("button", { name: "Changes" }).click();
  await expect(page.getByRole("heading", { name: "Published policy history" })).toBeVisible();
  await expect(page.getByText("fresh-history")).toBeVisible();

  deferredHistory.release();
  await expect(page.getByText("fresh-history")).toBeVisible();
  await expect(page.getByText("stale-history")).toHaveCount(0);
});

test("late settings responses cannot replace the current administrator page", async ({ page }) => {
  const deferredSettings = deferredResponse();
  await authenticatedApi(page, { admin: true, deferredSettings });
  await page.goto("/#settings");
  await deferredSettings.started;

  await page.getByRole("button", { name: "Assets" }).click();
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
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
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
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
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  const search = page.getByLabel("Find governed asset");
  await search.fill("stale");
  await deferredInventory.started;

  await page.getByRole("button", { name: "Activity" }).click();
  await expect(page.getByRole("heading", { name: "Activity", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Assets" }).click();
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await expect(search).toBeEnabled();
  await search.fill("fresh");
  await expect(page.locator("#asset-select")).toContainText("demo / fresh-orders");

  deferredInventory.release();
  await expect(page.locator("#asset-select")).toContainText("demo / fresh-orders");
  await expect(page.locator("#asset-select")).not.toContainText("demo / stale-orders");
});

test("late policy version lookups cannot replace the selected revision", async ({ page }) => {
  const deferredVersion = deferredResponse();
  await authenticatedApi(page, { deferredVersion });
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await page.getByRole("tab", { name: "History" }).click();
  await expect(page.getByRole("heading", { name: "Published policy revisions" })).toBeVisible();

  await page.getByRole("button", { name: "View details" }).nth(0).click();
  await deferredVersion.started;
  await page.getByRole("button", { name: "View details" }).nth(1).click();
  await expect(page.getByRole("heading", { name: "Version 2 details" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "group:fresh" })).toBeVisible();

  deferredVersion.release();
  await expect(page.getByRole("heading", { name: "Version 2 details" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "group:fresh" })).toBeVisible();
  await expect(page.getByRole("cell", { name: "group:stale" })).toHaveCount(0);
});

test("late draft saves cannot clear a newer local edit", async ({ page }) => {
  const deferredSave = deferredResponse();
  await authenticatedApi(page, { deferredSave });
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  await page.getByRole("button", { name: "Save deny-all draft" }).click();
  await deferredSave.started;
  await page.getByRole("button", { name: "Add first rule" }).click();
  await expect(page.getByRole("button", { name: "Save draft" })).toBeVisible();
  await expect(page.getByText("Unsaved changes")).toBeVisible();

  deferredSave.release();
  await expect(page.getByText("Unsaved changes")).toBeVisible();
  await expect(page.getByText("Policy draft saved to the control plane.")).toHaveCount(0);
});

test("late policy evaluations cannot replace a newer draft preview", async ({ page }) => {
  const deferredEvaluate = deferredResponse();
  await authenticatedApi(page, { deferredEvaluate });
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  await page.getByRole("button", { name: "Run policy test" }).click();
  await deferredEvaluate.started;
  await page.getByRole("button", { name: "Add first rule" }).click();
  await expect(page.getByText("Unsaved changes")).toBeVisible();

  deferredEvaluate.release();
  await expect(page.getByText("Unsaved changes")).toBeVisible();
  await expect(page.getByText("Server-side evaluation completed: denied.")).toHaveCount(0);
});

test("late policy restores cannot replace a newer local edit", async ({ page }) => {
  const deferredRestore = deferredResponse();
  await authenticatedApi(page, { deferredRestore });
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await page.getByRole("tab", { name: "History" }).click();
  await expect(page.getByRole("heading", { name: "Published policy revisions" })).toBeVisible();

  await page.getByRole("button", { name: "Restore to draft" }).nth(0).click();
  await deferredRestore.started;
  await page.getByRole("tab", { name: "Policy" }).click();
  await page.getByRole("button", { name: "Add first rule" }).click();
  await expect(page.getByRole("button", { name: "Save draft" })).toBeVisible();
  await expect(page.getByText("Unsaved changes")).toBeVisible();

  deferredRestore.release();
  await expect(page.getByText("Unsaved changes")).toBeVisible();
  await expect(page.getByText(/restored as draft revision/)).toHaveCount(0);
});

test("late policy reviews cannot authorize a newer draft", async ({ page }) => {
  const deferredReview = deferredResponse();
  await authenticatedApi(page, { deferredReview });
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  await page.getByRole("button", { name: "Review for publish" }).click();
  await deferredReview.started;
  await page.getByRole("button", { name: "Add first rule" }).click();
  await expect(page.getByText("Unsaved changes")).toBeVisible();

  deferredReview.release();
  await expect(page.getByText("Unsaved changes")).toBeVisible();
  await expect(page.getByText("Server review is current for this saved draft revision. You can publish it now.")).toHaveCount(0);
});

test("late publishes cannot commit a newer local draft", async ({ page }) => {
  const deferredPublish = deferredResponse();
  await authenticatedApi(page, { allowPublish: true, deferredPublish });
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  await page.getByRole("button", { name: "Review for publish" }).click();
  await expect(page.getByText("Server review is current for this saved draft revision. You can publish it now.")).toBeVisible();
  await page.getByRole("button", { name: "Publish reviewed deny-all" }).click();
  await deferredPublish.started;
  await page.getByRole("button", { name: "Add first rule" }).click();
  await expect(page.getByRole("button", { name: "Save draft" })).toBeVisible();
  await expect(page.getByText("Unsaved changes")).toBeVisible();

  deferredPublish.release();
  await expect(page.getByText("Unsaved changes")).toBeVisible();
  await expect(page.getByText("Published the saved draft.")).toHaveCount(0);
});

test("reader deep links fail closed before admin settings requests", async ({ page }) => {
  const adminRequests: string[] = [];
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (path.startsWith("/v1/settings/")) adminRequests.push(path);
  });
  await authenticatedApi(page);
  await page.goto("/#settings");

  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await expect.poll(() => new URL(page.url()).hash).toBe("#assets");
  expect(adminRequests).toEqual([]);
});

test("reader deep links fail closed before connection management requests", async ({ page }) => {
  const adminRequests: string[] = [];
  page.on("request", (request) => {
    const path = new URL(request.url()).pathname;
    if (path.startsWith("/v1/settings/") || path.startsWith("/v1/catalogs") || path.startsWith("/v1/plugins") || path.startsWith("/v1/workspace/publications")) {
      adminRequests.push(path);
    }
  });
  await authenticatedApi(page);
  await page.goto("/#connections");

  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await expect.poll(() => new URL(page.url()).hash).toBe("#assets");
  expect(adminRequests).toEqual([]);
});
