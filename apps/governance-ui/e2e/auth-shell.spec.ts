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

  test("uses the configured SSO entry and hides local bootstrap", async ({ page }) => {
    await signedOutApi(page, { oidc: { authority: "https://idp.example.invalid", client_id: "governance-ui", redirect_uri: "https://ui.example.invalid/auth/callback" } });
    await page.route("**/auth/login", async (route) => route.fulfill({ status: 200, headers: { "content-type": "text/html" }, body: "<title>SSO redirect</title>" }));
    await page.goto("/");

    await expect(page.getByRole("button", { name: "Sign in with SSO" })).toBeVisible();
    await expect(page.getByLabel("Local control-plane token")).toHaveCount(0);
    await page.getByRole("button", { name: "Sign in with SSO" }).click();
    await expect(page).toHaveURL(/\/auth\/login$/);
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
  await page.getByRole("dialog", { name: "Navigation" }).getByRole("button", { name: "Activity", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Activity", exact: true })).toBeVisible();
  await expect(page.getByRole("dialog", { name: "Navigation" })).toHaveCount(0);
});

test("authenticated policy workspace stays within the page at supported sizes and zoom", async ({ page }) => {
  await authenticatedApi(page);
  for (const viewport of [{ width: 390, height: 844 }, { width: 768, height: 1024 }, { width: 1440, height: 900 }]) {
    await page.setViewportSize(viewport);
    await page.goto("/#assets");
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

test("asset owner chooses whether live policy changes revoke tokens", async ({ page }) => {
  const assetId = "00000000-0000-4000-8000-000000000001";
  await authenticatedApi(page, { initialPolicyRules: [{
    ordinal: 10,
    effect: "allow",
    principals: ["group:analysts"],
    when: { region: "eu" },
    columns: ["order_id"],
    masks: { order_id: { type: "hash" } },
    row_filter: "region = 'eu'",
  }] });
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await expect(page.getByText("Live policy revision 1")).toBeVisible();
  await expect(page.getByText("Loaded the current live policy.")).toBeVisible();

  const revokeOnSave = page.getByLabel("Revoke existing tokens after saving");
  await expect(revokeOnSave).toBeVisible();
  await expect(revokeOnSave).not.toBeChecked();
  const preserveRequest = page.waitForRequest((request) => request.method() === "PUT" && request.url().endsWith(`/v1/assets/${assetId}/policy`));
  await page.getByRole("button", { name: "Save policy" }).click();
  expect((await preserveRequest).postDataJSON()).toMatchObject({
    expected_revision: 1,
    revoke_existing_tokens: false,
    rules: [{ effect: "allow", principals: ["group:analysts"], masks: { order_id: { type: "hash" } }, row_filter: "region = 'eu'" }],
  });
  await expect(page.getByText("Live policy saved. Existing tokens remain valid until expiry unless an owner revokes them.")).toBeVisible();

  await revokeOnSave.check();
  const revokeRequest = page.waitForRequest((request) => request.method() === "PUT" && request.url().endsWith(`/v1/assets/${assetId}/policy`));
  await page.getByRole("button", { name: "Save policy" }).click();
  expect((await revokeRequest).postDataJSON()).toMatchObject({
    expected_revision: 2,
    revoke_existing_tokens: true,
    rules: [{ effect: "allow", principals: ["group:analysts"], masks: { order_id: { type: "hash" } }, row_filter: "region = 'eu'" }],
  });
  await expect(page.getByText("Live policy saved; revoked 2 active token(s)." )).toBeVisible();

  await page.getByRole("tab", { name: "Access" }).click();
  page.once("dialog", (dialog) => dialog.accept());
  const allTokensRequest = page.waitForRequest((request) => request.method() === "POST" && request.url().endsWith(`/v1/assets/${assetId}/tickets/revoke`));
  await page.getByRole("button", { name: "Revoke all active tokens" }).click();
  await allTokensRequest;
  await expect(page.getByText("Revoked 2 active token(s)." )).toBeVisible();
});

test("nested schema tree virtualizes 10k fields and keeps keyboard movement responsive", async ({ page }) => {
  await authenticatedApi(page);
  const fields = Array.from({ length: 10_000 }, (_, index) => ({
    field_id: index + 1,
    name: `field_${index}`,
    human_path: `field_${index}`,
    type: "string",
    nullable: true,
    kind: "scalar",
    path: { version: 1, segments: [{ kind: "field", name: `field_${index}`, field_id: index + 1 }] },
  }));
  await page.route("**/v1/assets/00000000-0000-4000-8000-000000000001/schema", async (route) => route.fulfill({
    json: {
      asset_id: "00000000-0000-4000-8000-000000000001",
      catalog: "demo",
      target: "demo.orders",
      schema_version: 1,
      schema_fingerprint: "large-schema",
      stable_field_ids: true,
      supported_masks: ["null", "redact", "hash", "email", "keep_last", "default"],
      fields,
    },
  }));
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  const tree = page.getByRole("tree", { name: "Schema fields" });
  await expect(tree).toBeVisible();
  const mountedRows = tree.getByRole("treeitem");
  expect(await mountedRows.count()).toBeLessThanOrEqual(200);
  await mountedRows.first().focus();
  const durations: number[] = [];
  for (let index = 0; index < 20; index += 1) {
    const started = await page.evaluate(() => performance.now());
    await page.keyboard.press("ArrowDown");
    const finished = await page.evaluate(() => performance.now());
    durations.push(finished - started);
  }
  durations.sort((left, right) => left - right);
  expect(durations[Math.floor(durations.length * 0.95)], "95th percentile tree key latency").toBeLessThan(200);
});

test("authenticated policy workspace meets local web-vitals thresholds", async ({ page }) => {
  await authenticatedApi(page);
  await page.addInitScript(() => {
    window.__dalObscuraWebVitals = { lcp: null, cls: 0 };
    new PerformanceObserver((list) => {
      const last = list.getEntries().at(-1);
      if (last) window.__dalObscuraWebVitals.lcp = last.startTime;
    }).observe({ type: "largest-contentful-paint", buffered: true });
    new PerformanceObserver((list) => {
      for (const entry of list.getEntries()) {
        if (!entry.hadRecentInput) window.__dalObscuraWebVitals.cls += entry.value;
      }
    }).observe({ type: "layout-shift", buffered: true });
  });
  await page.goto("/#assets", { waitUntil: "networkidle" });
  await page.evaluate(() => document.fonts.ready);
  const metrics = await page.evaluate(() => window.__dalObscuraWebVitals);
  expect(metrics.lcp).not.toBeNull();
  expect(metrics.lcp, "local authenticated LCP").toBeLessThanOrEqual(2500);
  expect(metrics.cls, "local authenticated CLS").toBeLessThanOrEqual(0.1);
});

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
  page.once("dialog", (dialog) => dialog.accept());
  const removalResponse = page.waitForResponse((response) => response.url().includes("/v1/plugins/table_format/synthetic.table.iceberg/lifecycle") && response.request().method() === "PATCH");
  await formatPlugin.getByRole("button", { name: "Apply" }).click();
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
  await page.route("**/v1/assets/**/policy", async (route) => route.fulfill({
    status: 200,
    headers: { "content-type": "text/html" },
    body: "<html><title>Sign in</title></html>",
  }));
  await page.getByRole("button", { name: "Save deny-all policy" }).click();
  await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
  await expect(page.getByText("The browser or edge session expired. Sign in again to continue.")).toBeVisible();
  await expect(page.getByRole("heading", { name: "orders" })).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Save deny-all policy" })).toHaveCount(0);
});

test("asset deep links never substitute the first inventory result", async ({ page }) => {
  await authenticatedApi(page);
  await page.goto("/?asset=00000000-0000-4000-8000-000000000099#assets");
  await expect(page.getByRole("heading", { name: "Asset unavailable" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "orders" })).toHaveCount(0);
});

test("authorized deep links load independently of the first inventory page", async ({ page }) => {
  await authenticatedApi(page);
  await page.route("**/v1/assets/page**", (route) => route.fulfill({ json: { items: [], next_cursor: null } }));
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
});

test("late management responses cannot replace the current page", async ({ page }) => {
  const deferredAudit = deferredResponse();
  await authenticatedApi(page, { deferredAudit });
  await page.goto("/#activity");
  await deferredAudit.started;

  await page.getByRole("button", { name: "Assets", exact: true }).click();
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
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

  await page.getByRole("button", { name: "Activity", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Activity", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Assets" }).click();
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
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
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();

  await page.getByRole("button", { name: "Save deny-all policy" }).click();
  await deferredSave.started;
  await page.getByRole("button", { name: "Add first rule" }).click();
  await expect(page.getByRole("button", { name: "Save policy" })).toBeVisible();
  await expect(page.getByText("Unsaved changes")).toBeVisible();

  deferredSave.release();
  await expect(page.getByText("Unsaved changes")).toBeVisible();
  await expect(page.getByText("Live policy saved.")).toHaveCount(0);
});

test("late policy evaluations cannot replace a newer unsaved edit", async ({ page }) => {
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
    if (path.startsWith("/v1/settings/") || path.startsWith("/v1/catalogs") || path.startsWith("/v1/plugins")) {
      adminRequests.push(path);
    }
  });
  await authenticatedApi(page);
  await page.goto("/#connections");

  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await expect.poll(() => new URL(page.url()).hash).toBe("#assets");
  expect(adminRequests).toEqual([]);
});
