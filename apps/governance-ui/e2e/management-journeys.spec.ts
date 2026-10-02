import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { authenticatedApi } from "./fixtures";

test("administrator management screens stay within the page at supported sizes", async ({ page }) => {
  await authenticatedApi(page, {
    admin: true,
    configuredSettings: true,
    configuredConnections: true,
    configuredLifecycle: true,
    configuredAudit: true,
  });
  const screens = [
    ["#activity", "Workspace status"],
    ["#connections", "Catalog connections"],
    ["#settings", "Runtime and identity"],
  ] as const;
  for (const viewport of [{ width: 390, height: 844 }, { width: 768, height: 1024 }, { width: 1440, height: 900 }]) {
    await page.setViewportSize(viewport);
    for (const [hash, heading] of screens) {
      await page.goto(`/${hash}`);
      await expect(page.getByRole("heading", { name: heading })).toBeVisible();
      const dimensions = await page.evaluate(() => ({
        documentWidth: document.documentElement.scrollWidth,
        viewportWidth: document.documentElement.clientWidth,
      }));
      expect(dimensions.documentWidth, `${hash} at ${viewport.width}px page overflow`).toBeLessThanOrEqual(dimensions.viewportWidth + 1);
    }
  }

  await page.setViewportSize({ width: 1440, height: 900 });
  for (const [hash, heading] of screens) {
    await page.goto(`/${hash}`);
    await page.evaluate(() => { document.body.style.zoom = "2"; });
    const zoomedDimensions = await page.evaluate(() => ({
      documentWidth: document.documentElement.scrollWidth,
      viewportWidth: document.documentElement.clientWidth,
    }));
    expect(zoomedDimensions.documentWidth, `${hash} at 200% zoom page overflow`).toBeLessThanOrEqual(zoomedDimensions.viewportWidth + 1);
    await expect(page.getByRole("heading", { name: heading })).toBeVisible();
  }
});

test("administrator management forms keep predictable keyboard order", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredSettings: true, configuredAudit: true });
  await page.goto("/#activity");
  const actor = page.getByPlaceholder("platform:admin");
  const action = page.getByPlaceholder("asset.policy.replace");
  const requestId = page.getByPlaceholder("Correlation ID");
  await actor.focus();
  await expect(actor).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(action).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(requestId).toBeFocused();

  await page.goto("/#settings");
  const ttl = page.getByLabel("Ticket TTL (seconds)");
  const maxTickets = page.getByLabel("Max tickets");
  const exchanges = page.getByLabel("Ticket exchanges");
  await ttl.focus();
  await page.keyboard.press("Tab");
  await expect(maxTickets).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(exchanges).toBeFocused();
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
  await expect(page.getByRole("heading", { name: "Assets", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "orders", exact: true }).click();
  await page.getByRole("tab", { name: "Access" }).click();
  await expect(page.getByRole("heading", { name: "Owners and delegated capabilities" })).toBeVisible();

  await page.getByLabel("Owner principals").fill("alex@example.invalid, steward@example.invalid");
  await page.getByRole("button", { name: "Save owners" }).click();
  await expect(page.getByText("Owners updated.")).toBeVisible();

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
  await expect(page.getByText("Runtime settings saved to live configuration.")).toBeVisible();
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
  const governRequest = page.waitForRequest((request) => request.method() === "PUT" && request.url().endsWith("/v1/assets/analytics/orders"));
  await page.getByRole("button", { name: "Govern table" }).click();
  await expect((await governRequest).postDataJSON()).toMatchObject({ table_identifier: "demo.orders" });

  await page.getByLabel("Name").fill("warehouse");
  await page.getByLabel("Catalog URI").fill("https://warehouse.example");
  await page.getByLabel("Password secret reference").fill("catalog/warehouse");
  await page.getByRole("button", { name: "Save connection" }).click();
  await expect(page.getByText("Connection saved. Discovery remains bounded to this configured catalog.")).toBeVisible();
});

test("administrator chooses an admitted table format before governing discovery", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredConnections: true, multipleFormats: true });
  await page.goto("/#connections");
  await page.getByRole("button", { name: "Discover tables" }).click();
  await expect(page.getByLabel("Selected table format")).toBeVisible();

  await page.getByRole("button", { name: "Govern table" }).click();
  await expect(page.getByText("Select the table format explicitly before governing a discovered table.")).toBeVisible();

  await page.getByLabel("Selected table format").selectOption("synthetic.table.delta");
  const governRequest = page.waitForRequest((request) => request.method() === "PUT" && request.url().endsWith("/v1/assets/analytics/orders"));
  await page.getByRole("button", { name: "Govern table" }).click();
  await expect((await governRequest).postDataJSON()).toMatchObject({ backend: "synthetic.table.delta", table_identifier: "demo.orders" });
});

test("management refresh failure preserves the loaded connections view", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredConnections: true });
  await page.goto("/#connections");
  await expect(page.getByRole("heading", { name: "Catalog connections" })).toBeVisible();
  await page.route("**/v1/catalogs", async (route) => route.fulfill({ status: 503, json: { detail: "catalog backend unavailable" } }));

  await page.getByRole("button", { name: "Refresh" }).click();
  await expect(page.getByRole("heading", { name: "Catalog connections" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "analytics" })).toBeVisible();
  await expect(page.getByRole("alert")).toContainText("control plane is temporarily unavailable");
  await expect(page.getByRole("button", { name: "Retry refresh" })).toBeVisible();
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
  const removalResponse = page.waitForResponse((response) => response.url().includes("/v1/plugins/table_format/synthetic.table.iceberg/lifecycle") && response.request().method() === "PATCH");
  await formatPlugin.getByRole("button", { name: "Apply" }).click();
  await page.getByRole("button", { name: "Remove plugin", exact: true }).click();
  await expect((await removalResponse).status()).toBe(200);
  await expect(page.getByText("Synthetic Iceberg lifecycle is now removed.")).toBeVisible();
});

test("administrator activity filters and paginates the permitted audit records", async ({ page }) => {
  await authenticatedApi(page, { admin: true, configuredAudit: true });
  await page.goto("/#activity");
  await expect(page.getByRole("heading", { name: "Workspace status" })).toBeVisible();
  await page.getByLabel("Action").fill("asset.policy.replace");
  await page.getByRole("button", { name: "Apply filters" }).click();
  await expect(page.getByText("asset.policy.replace")).toBeVisible();
  await expect(page.getByText("asset.tokens.revoke")).toHaveCount(0);

  await page.getByRole("button", { name: "Load more activity" }).click();
  await expect(page.getByText("asset.tokens.revoke")).toBeVisible();
});

test.describe("audit date filters in a non-UTC timezone", () => {
  test.use({ timezoneId: "Europe/Amsterdam" });

  test("keeps the selected local time visible after applying filters", async ({ page }) => {
    await authenticatedApi(page);
    await page.goto("/#activity");
    await expect(page.getByRole("heading", { name: "Workspace status" })).toBeVisible();

    const createdAfter = page.getByLabel("Created after");
    await createdAfter.fill("2026-09-26T10:45");
    await page.getByRole("button", { name: "Apply filters" }).click();

    await expect(createdAfter).toHaveValue("2026-09-26T10:45");
  });
});
