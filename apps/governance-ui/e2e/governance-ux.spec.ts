import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
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

test("capture review images", async ({ page }) => {
  const phase = process.env.UX_CAPTURE_PHASE;
  test.skip(!phase, "Run with UX_CAPTURE_PHASE=before or after to refresh review images.");
  const directory = process.env.UX_CAPTURE_DIR ?? resolve("../../docs/ui-review/images");
  await mkdir(directory, { recursive: true });
  await page.setViewportSize({ width: 1440, height: 1100 });
  await page.emulateMedia({ colorScheme: "dark", reducedMotion: "reduce" });
  await page.goto("/#assets");
  await expect(page.getByRole("button", { name: "retail.customer_revenue", exact: true })).toBeVisible();
  await page.locator(".asset-inventory").screenshot({ path: `${directory}/${phase}-inventory.png` });
  await page.getByRole("button", { name: "retail.customer_revenue", exact: true }).click();
  await expect(page.getByLabel("Rule name")).toBeVisible();
  await page.getByRole("button", { name: /Allowed columns:/ }).click();
  await page.getByRole("combobox", { name: "Search allowed columns" }).fill("customer");
  await page.screenshot({ path: `${directory}/${phase}-picker.png` });
  await page.getByRole("button", { name: "Done", exact: true }).click();
  if (phase === "after") await page.getByRole("button", { name: "Show schema", exact: true }).click();
  await page.setViewportSize({ width: 1440, height: 1640 });
  await page.evaluate(() => window.scrollTo(0, 0));
  await page.screenshot({ path: `${directory}/${phase}-workspace.png`, fullPage: true });
  if (phase === "after") {
    await page.getByRole("complementary", { name: "Asset schema" }).getByRole("button", { name: "Hide schema", exact: true }).click();
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.locator(".rule-footer").scrollIntoViewIfNeeded();
    await expect(page.getByRole("button", { name: "New rule", exact: true })).toBeInViewport();
    await page.screenshot({ path: `${directory}/after-rule-toolbar.png` });
    await page.setViewportSize({ width: 1440, height: 1100 });
    await page.getByRole("button", { name: /Allowed columns:/ }).click();
    await page.getByRole("button", { name: "Select all except…", exact: true }).click();
    await page.getByRole("combobox", { name: "Search allowed columns" }).fill("internal_notes");
    await page.getByRole("option", { name: /internal_notes/ }).click();
    await page.screenshot({ path: `${directory}/after-exclusions.png` });
    await page.getByRole("button", { name: "Close column picker", exact: true }).click();
    await page.getByRole("button", { name: /Allowed columns:/ }).click();
    await page.getByRole("button", { name: "Add by prefix", exact: true }).click();
    await page.getByLabel("Column path prefix").fill("customer.");
    await page.screenshot({ path: `${directory}/after-prefix.png` });
    await page.getByRole("button", { name: "Close column picker", exact: true }).click();
    await page.setViewportSize({ width: 390, height: 844 });
    await page.screenshot({ path: `${directory}/after-mobile-workspace.png` });
    await page.getByRole("button", { name: /Allowed columns:/ }).click();
    await expect(page.getByRole("dialog", { name: "Choose allowed columns" })).toBeVisible();
    await expect(page.getByRole("dialog", { name: "Choose allowed columns" })).toHaveCSS("opacity", "1");
    await page.screenshot({ path: `${directory}/after-mobile.png` });
  }
});
