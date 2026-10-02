import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { uxApi, uxAssetId } from "./ux-fixture";
import { authenticatedApi } from "./fixtures";

test.beforeEach(async ({ page }) => { await uxApi(page); });
const assetUrl = `/?asset=${uxAssetId}#assets`;

test("bulk selection scopes actions to results and supports multiple exclusions", async ({ page }) => {
  await page.goto(assetUrl);
  await page.getByRole("button", { name: /Allowed columns:/ }).click();
  const picker = page.getByRole("dialog", { name: "Choose allowed columns" });
  await expect(picker).toBeVisible();
  await picker.getByRole("button", { name: "Clear selection", exact: true }).click();
  const search = picker.getByRole("combobox", { name: "Search allowed columns" });
  await search.fill("customer");
  await picker.getByRole("button", { name: /Select search results/ }).click();
  await expect(picker.getByText("4 selected", { exact: true })).toBeVisible();
  await picker.getByRole("button", { name: "Select all except…", exact: true }).click();
  await search.fill("customer.email");
  await picker.getByRole("option", { name: /customer.email/ }).click();
  await search.fill("internal_notes");
  await picker.getByRole("option", { name: /internal_notes/ }).click();
  await picker.getByRole("button", { name: /Apply exclusions/ }).click();
  await picker.getByRole("button", { name: "Done", exact: true }).click();
  const chips = page.getByRole("group", { name: "Selected columns" }).first();
  await expect(chips.getByText("customer.email", { exact: true })).toHaveCount(0);
  await expect(chips.getByText("internal_notes", { exact: true })).toHaveCount(0);
  await expect(chips.getByText("customer.phone", { exact: true })).toBeVisible();
});

test("prefix previews matching columns and keeps existing selections", async ({ page }) => {
  await page.goto(assetUrl);
  await page.getByRole("button", { name: /Allowed columns:/ }).click();
  const picker = page.getByRole("dialog", { name: "Choose allowed columns" });
  await picker.getByRole("button", { name: "Add by prefix", exact: true }).click();
  await picker.getByLabel("Column path prefix").fill("customer.");
  await expect(picker.getByText("4 matching columns", { exact: true })).toBeVisible();
  await picker.getByRole("button", { name: "Add matching columns", exact: true }).click();
  await picker.getByRole("button", { name: "Done", exact: true }).click();
  await expect(page.getByRole("button", { name: "Allowed columns: 6 selected" })).toBeVisible();
});

test("schema reference searches nested fields and collapses without changing rules", async ({ page }) => {
  await page.goto(assetUrl);
  await page.getByRole("button", { name: "Show schema", exact: true }).click();
  const sidebar = page.getByRole("complementary", { name: "Asset schema" });
  await expect(sidebar).toBeVisible();
  await sidebar.getByRole("searchbox", { name: "Search schema" }).fill("email");
  await expect(sidebar.getByText("customer.email", { exact: true })).toBeVisible();
  await expect(sidebar.getByText("revenue", { exact: true })).toHaveCount(0);
  await sidebar.getByRole("button", { name: "Hide schema", exact: true }).click();
  await expect(sidebar).toHaveCount(0);
  await expect(page.getByRole("button", { name: "Allowed columns: 3 selected" })).toBeVisible();
});

test("expanding nested schema fields reveals children without losing scroll or focus", async ({ page }) => {
  const scalar = (name: string, id: number) => ({ field_id: id, name, human_path: name, type: "string", nullable: true, kind: "scalar", path: { version: 1, segments: [{ kind: "field", name, field_id: id }] } });
  const profile = scalar("profile", 101);
  const contact = { ...scalar("contact", 102), human_path: "profile.contact", path: { version: 1, segments: [...profile.path.segments, { kind: "field", name: "contact", field_id: 102 }] } };
  const email = { ...scalar("email", 103), human_path: "profile.contact.email", path: { version: 1, segments: [...contact.path.segments, { kind: "field", name: "email", field_id: 103 }] } };
  const fields = [...Array.from({ length: 20 }, (_, i) => scalar(`before_${i}`, i + 1)), { ...profile, kind: "struct", type: "struct", children: [{ ...contact, kind: "struct", type: "struct", children: [email] }] }, ...Array.from({ length: 30 }, (_, i) => scalar(`after_${i}`, i + 200))];
  await page.route(`**/v1/assets/${uxAssetId}/schema`, (route) => route.fulfill({ json: { asset_id: uxAssetId, schema_version: 1, fields, supported_masks: ["null"] } }));
  await page.goto(assetUrl);
  await page.getByRole("button", { name: "Show schema", exact: true }).click();
  const sidebar = page.getByRole("complementary", { name: "Asset schema" });
  const viewport = sidebar.locator(".schema-reference-list");
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 900 });
    await viewport.scrollIntoViewIfNeeded();
    await viewport.evaluate((element) => { element.scrollTop = 20 * 56; });
    const expand = sidebar.getByRole("button", { name: "Expand profile.contact", exact: true });
    await expect(expand).toBeInViewport();
    const before = await viewport.evaluate((element) => ({ list: element.scrollTop, page: window.scrollY }));
    await expand.click();
    await expect(sidebar.getByText("profile.contact.email", { exact: true })).toBeInViewport();
    const collapse = sidebar.getByRole("button", { name: "Collapse profile.contact", exact: true });
    await expect(collapse).toBeFocused();
    expect(await viewport.evaluate((element) => ({ list: element.scrollTop, page: window.scrollY }))).toEqual(before);
    await collapse.press("Enter");
    await expect(sidebar.getByText("profile.contact.email", { exact: true })).toHaveCount(0);
    await expect(sidebar.getByRole("button", { name: "Expand profile.contact", exact: true })).toBeFocused();
    expect(await viewport.evaluate((element) => ({ list: element.scrollTop, page: window.scrollY }))).toEqual(before);
  }

});

test("sign-out follows the provider logout URL after revoking the local session", async ({ page }) => {
  let revoked = false;
  const providerUrl = "https://issuer.example/realms/demo/protocol/openid-connect/logout?client_id=dal-obscura-ui&post_logout_redirect_uri=http%3A%2F%2F127.0.0.1%3A4173%2F";
  await page.route("**/v1/logout", (route) => { revoked = true; return route.fulfill({ json: { authenticated: false, logout_url: providerUrl } }); });
  await page.route("https://issuer.example/**", (route) => route.fulfill({ contentType: "text/html", body: "<h1>Confirm SSO sign-out</h1>" }));
  await page.goto(assetUrl);
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await expect(page).toHaveURL(providerUrl);
  await expect(page.getByRole("heading", { name: "Confirm SSO sign-out" })).toBeVisible();
  expect(revoked).toBe(true);
});

test("one persistent New rule action stays reachable while editing on desktop and mobile", async ({ page }) => {
  await page.goto(assetUrl);
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 844 });
    await page.locator(".rule-footer").scrollIntoViewIfNeeded();
    const create = page.getByRole("button", { name: "New rule", exact: true });
    await expect(create).toHaveCount(1);
    await expect(create).toBeInViewport();
    await expect(page.locator(".rule-footer").getByRole("button", { name: /Add rule|New rule/ })).toHaveCount(0);
    const box = await create.boundingBox();
    expect(await page.evaluate(({ x, y }) => document.elementFromPoint(x, y)?.closest("button")?.textContent, { x: box!.x + box!.width / 2, y: box!.y + box!.height / 2 })).toContain("New rule");
  }
  await page.getByRole("button", { name: "New rule", exact: true }).click();
  await expect(page.getByLabel("Rule name").last()).toBeFocused();
  await expect(page.getByLabel("Rule name")).toHaveCount(2);
  await expect(page.getByLabel("Rule name").last()).toBeInViewport();
  const nameBox = await page.getByLabel("Rule name").last().boundingBox();
  expect(await page.evaluate(({ x, y }) => document.elementFromPoint(x, y)?.tagName, { x: nameBox!.x + 10, y: nameBox!.y + nameBox!.height / 2 })).toBe("INPUT");
});

test("New rule appends and preserves rule order after saving and reloading", async ({ page }) => {
  const rule = { effect: "allow", principals: ["group:analysts"], columns: ["email"], masks: {}, row_filter: null, when: {} };
  await authenticatedApi(page, { ruleEditor: true, initialPolicyRules: [{ ...rule, ordinal: 10, name: "First" }, { ...rule, ordinal: 20, name: "Second" }] });
  await page.goto(assetUrl);
  await page.getByRole("button", { name: "New rule", exact: true }).click();
  await page.getByLabel("Rule name").last().fill("Third");
  const audience = page.locator(".collapsible-rule").last().getByLabel("Applies to people or groups");
  await audience.fill("group:analysts");
  await audience.press("Enter");
  const request = page.waitForRequest((request) => request.method() === "PUT" && request.url().endsWith("/policy"));
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  const saved = (await request).postDataJSON().rules as Array<{ name: string; ordinal: number }>;
  expect(saved.map((rule) => rule.name)).toEqual(["First", "Second", "Third"]);
  expect(saved.map((rule) => rule.ordinal)).toEqual([...saved.map((rule) => rule.ordinal)].sort((a, b) => a - b));
  await expect(page.getByText(/Live policy saved/)).toBeVisible();
  await page.reload();
  await expect(page.locator(".rule-disclosure strong")).toHaveText(["First", "Second", "Third"]);
});

test("excluding a subtree then restoring one child keeps sibling exclusions", async ({ page }) => {
  await page.goto(assetUrl);
  await page.getByRole("button", { name: /Allowed columns:/ }).click();
  const picker = page.getByRole("dialog", { name: "Choose allowed columns" });
  await picker.getByRole("button", { name: "Select all except…", exact: true }).click();
  const search = picker.getByRole("combobox", { name: "Search allowed columns" });
  await search.fill("customer");
  await picker.getByRole("option", { name: /^customer 4 nested fields/ }).click();
  await picker.getByRole("option", { name: /customer.email/ }).click();
  await picker.getByRole("button", { name: /Apply exclusions/ }).click();
  await picker.getByRole("button", { name: "Done", exact: true }).click();
  const selected = page.getByRole("group", { name: "Selected columns" }).first();
  await expect(selected.getByText("customer.email", { exact: true })).toBeVisible();
  await expect(selected.getByText("customer.phone", { exact: true })).toHaveCount(0);
});

test("inventory aligns links and reveals the complete owner identity", async ({ page }) => {
  await page.goto("/#assets");
  await page.getByRole("button", { name: /group:asset-owners/ }).click();
  await expect(page.getByText("http://localhost:20080/realms/dal-obscura-demo|group:asset-owners", { exact: true })).toBeVisible();
  const table = page.getByRole("table");
  const heading = await table.getByRole("columnheader", { name: "Asset", exact: true }).boundingBox();
  const link = await table.getByRole("button", { name: "retail.customer_revenue", exact: true }).boundingBox();
  expect(Math.abs(link!.x - heading!.x - 16)).toBeLessThanOrEqual(1);
});

test("picker is accessible in both themes and fits mobile", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.goto(assetUrl);
  for (const colorScheme of ["light", "dark"] as const) {
    await page.emulateMedia({ colorScheme });
    await page.getByRole("button", { name: /Allowed columns:/ }).click();
    await expect(page.getByRole("dialog", { name: "Choose allowed columns" })).toHaveCSS("opacity", "1");
    const results = await new AxeBuilder({ page }).analyze();
    expect(results.violations.filter((issue) => ["serious", "critical"].includes(issue.impact ?? "")).map((issue) => ({ id: issue.id, targets: issue.nodes.map((node) => node.target) }))).toEqual([]);
    await page.getByRole("combobox", { name: "Search allowed columns" }).fill("no-such-field");
    await expect(page.getByText("No matching columns", { exact: true })).toBeVisible();
    const emptyResults = await new AxeBuilder({ page }).analyze();
    expect(emptyResults.violations.filter((issue) => ["serious", "critical"].includes(issue.impact ?? "")).map((issue) => issue.id)).toEqual([]);
    await page.getByRole("button", { name: "Reset search", exact: true }).click();
    await page.getByRole("button", { name: "Done", exact: true }).click();
  }
  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: /Allowed columns:/ }).click();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth)).toBe(true);
  await expect(page.getByRole("button", { name: "Done", exact: true })).toBeInViewport();
});
