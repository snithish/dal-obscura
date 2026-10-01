import { expect, test } from "@playwright/test";
import AxeBuilder from "@axe-core/playwright";
import { assetId, identityApi } from "./identity-fixture";

test.beforeEach(async ({ page }) => { await identityApi(page); });

test("condition key changes clear values and preserve exact comma-containing values", async ({ page }) => {
  await page.goto(`/?asset=${assetId}#assets`);
  await page.getByText("Identity attribute conditions (1)", { exact: true }).click();
  await page.getByRole("combobox", { name: "Attribute 1", exact: true }).click();
  await page.getByRole("option", { name: "Region", exact: true }).click();
  await expect(page.getByRole("button", { name: "Save policy", exact: true })).toBeDisabled();
  await expect(page.locator(".attribute-conditions")).not.toContainText("Finance");
  await page.getByRole("combobox", { name: "Allowed values 1", exact: true }).click();
  await page.getByRole("option", { name: "EU", exact: true }).click();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("button", { name: "Save policy", exact: true })).toBeEnabled();
  const save = page.waitForRequest((request) => request.url().endsWith("/policy") && request.method() === "PUT");
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  expect((await save).postDataJSON().rules[0].when).toEqual({ region: ["EU"] });
});

test("admin preview sends unsaved mappings and exposes missing source claims", async ({ page }) => {
  await page.route("**/attribute-preview", (route) => route.fulfill({ json: { attributes: { department: "Finance" } } }));
  await page.goto("/#settings");
  await page.getByLabel("Source claim path 1", { exact: true }).fill("custom.unit");
  await page.getByText("Preview attribute mapping", { exact: true }).click();
  await page.getByLabel("Sample provider claims (JSON)").fill('{"custom":{"unit":"Finance"}}');
  const request = page.waitForRequest((request) => request.url().endsWith("/attribute-preview"));
  await page.getByRole("button", { name: "Preview mapping", exact: true }).click();
  expect((await request).postDataJSON().provider_args.attribute_claims.department).toBe("custom.unit");
  await expect(page.locator(".attribute-preview-chip")).toContainText("Finance");
  await expect(page.getByText("Missing: region (claim employee.region)", { exact: true })).toBeVisible();
  await page.getByLabel("Source claim path 1", { exact: true }).fill("employee.department");
  await expect(page.locator(".attribute-preview-chip")).toHaveCount(0);
});

test("provider policy test sends raw claims and shows normalized identity evidence", async ({ page }) => {
  await page.route("**/policy-evaluate", (route) => route.fulfill({ json: { status: "completed", decision: "allow", allowed_columns: ["email"], masks: [], row_filter: null, rows: [], input_rows: 0, output_rows: 0, schema: "", policy_revision: 1, evidence: { identity: { principal: "alice", groups: ["analysts"], attributes: { department: "Finance" } }, conditions: [{ rule_ordinal: 1, key: "department", expected: ["Engineering", "Finance"], actual: "Finance", missing: false, matched: true }, { rule_ordinal: 2, key: "region", expected: "EU", actual: null, missing: true, matched: false }] } } }));
  await page.goto(`/?asset=${assetId}#assets`);
  await page.getByRole("button", { name: "Test Policy", exact: true }).click();
  const modal = page.getByRole("dialog", { name: "Test policy", exact: true });
  await modal.getByLabel("Identity input", { exact: true }).click();
  await page.getByRole("option", { name: /Provider 1/ }).click();
  await expect(modal.getByLabel("Principal", { exact: true })).toHaveCount(0);
  await modal.getByLabel("Synthetic persona claims").fill('{"sub":"alice","employee":{"department":"Finance"}}');
  const request = page.waitForRequest((request) => request.url().endsWith("/policy-evaluate"));
  await modal.getByRole("button", { name: "Test saved policy", exact: true }).click();
  const body = (await request).postDataJSON();
  expect(body.provider_ordinal).toBe(1);
  expect(body.claims.employee.department).toBe("Finance");
  await expect(modal.locator(".identity-preview")).toContainText("Finance");
  await expect(modal.locator(".condition-evidence")).toContainText("Missing");
  await modal.getByLabel("Identity input", { exact: true }).click();
  await page.getByRole("option", { name: "Internal attributes (synthetic)", exact: true }).click();
  await expect(modal.locator(".identity-preview")).toHaveCount(0);
});

test("attribute controls remain accessible on narrow screens", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(`/?asset=${assetId}#assets`);
  await page.getByText("Identity attribute conditions (1)", { exact: true }).click();
  const results = await new AxeBuilder({ page }).include(".attribute-conditions").analyze();
  expect(results.violations).toEqual([]);
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
});

test("identity mapping forms remain accessible in both themes", async ({ page }) => {
  await page.goto("/#settings");
  for (const theme of ["Light theme", "Dark theme"]) {
    await page.getByRole("combobox", { name: "Color theme" }).click();
    await page.getByRole("option", { name: theme, exact: true }).click();
    await expect(page.locator(".attribute-mappings")).toBeVisible();
    const results = await new AxeBuilder({ page }).include(".attribute-mappings").analyze();
    expect(results.violations).toEqual([]);
  }
});
