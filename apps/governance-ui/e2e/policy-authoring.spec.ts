import { expect, test } from "@playwright/test";
import { authenticatedApi } from "./fixtures";
const rule = { ordinal: 10, effect: "allow", principals: ["group:analysts"], columns: ["email", "phone", "country"], masks: {}, row_filter: null, when: {} };
test.beforeEach(async ({ page }) => {
  await authenticatedApi(page, { ruleEditor: true, initialPolicyRules: [rule] });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
});
test("mask and row edits have independent save guards and survive reload", async ({ page }) => {
  await page.getByRole("button", { name: "Add Column Masks", exact: true }).click();
  await page.getByLabel("Mask", { exact: true }).selectOption("hash");
  await page.getByRole("button", { name: "Exclude columns", exact: true }).click();
  await page.getByRole("button", { name: "Columns without this mask: 0 selected" }).click();
  await page.getByRole("option", { name: /country/ }).click();
  await page.getByRole("button", { name: "Done", exact: true }).click();
  await page.getByRole("button", { name: "Exempt people or groups", exact: true }).click();
  await page.getByLabel("People or groups that skip this mask").fill("group:privacy-reviewers");
  await page.getByLabel("People or groups that skip this mask").press("Enter");
  await page.getByRole("button", { name: "Add Row Filter", exact: true }).click();
  await page.getByLabel("Filter column 1").selectOption("country");
  await page.getByLabel("Filter value 1").fill("US");
  await page.getByRole("button", { name: "Apply mask", exact: true }).click();
  await expect(page.getByRole("button", { name: "Save policy", exact: true })).toBeDisabled();
  await page.getByRole("button", { name: "Apply row filter", exact: true }).click();
  await expect(page.getByLabel("Filter value 1")).toHaveValue("US");
  await expect(page.getByRole("button", { name: "Save policy", exact: true })).toBeEnabled();
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  await expect(page.getByText(/Live policy saved/)).toBeVisible();
  await page.reload();
  await expect(page.getByText("group:privacy-reviewers", { exact: true })).toBeVisible();
  await expect(page.getByLabel("DuckDB SQL row filter")).toHaveValue('(\"country\" = \'US\')');
});
test("discard restores row state and releases the save guard", async ({ page }) => {
  await page.getByRole("button", { name: "Add Row Filter", exact: true }).click();
  await expect(page.getByRole("button", { name: "Save policy", exact: true })).toBeDisabled();
  await page.getByRole("button", { name: "Discard row changes", exact: true }).click();
  await expect(page.getByText("All rows are allowed by this rule.")).toBeVisible();
  await expect(page.getByRole("button", { name: "Save policy", exact: true })).toBeEnabled();
});
test("editing a mask preserves literal replacement text", async ({ page }) => {
  await page.getByRole("button", { name: "Add Column Masks", exact: true }).click();
  await page.getByLabel("Mask", { exact: true }).selectOption("redact");
  await page.getByLabel("Replacement text").fill("[private]");
  await page.getByRole("button", { name: "Apply mask", exact: true }).click();
  await page.getByRole("button", { name: "Edit Redact mask" }).click();
  await expect(page.getByLabel("Replacement text")).toHaveValue("[private]");
});
test("SQL mode receives the builder expression", async ({ page }) => {
  await page.getByRole("button", { name: "Add Row Filter", exact: true }).click();
  await page.getByLabel("Filter column 1").selectOption("country");
  await page.getByLabel("Filter operator 1").selectOption("in");
  await page.getByLabel("Filter value 1").fill("US, CA");
  await page.getByRole("button", { name: "DuckDB SQL", exact: true }).click();
  await expect(page.getByLabel("DuckDB SQL row filter")).toHaveValue('(\"country\" IN (\'US\', \'CA\'))');
});

test("discard all restores saved policy including applied masks", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.getByRole("button", { name: "Add Column Masks", exact: true }).click();
  await page.getByLabel("Mask", { exact: true }).selectOption("hash");
  await page.getByRole("button", { name: "Apply mask", exact: true }).click();
  await expect(page.getByRole("button", { name: "Edit Hash mask" })).toBeVisible();
  await page.getByRole("button", { name: "Discard all changes" }).click();
  await expect(page.getByRole("button", { name: "Add Column Masks" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Edit Hash mask" })).toHaveCount(0);
  await page.screenshot({ path: testInfo.outputPath("policy-desktop.png"), fullPage: true });
});

test("removing a column while editing a mask leaves it unmasked", async ({ page }) => {
  await page.getByRole("button", { name: "Add Column Masks", exact: true }).click();
  await page.getByLabel("Mask", { exact: true }).selectOption("hash");
  await page.getByRole("button", { name: "Apply mask", exact: true }).click();
  await page.getByRole("button", { name: "Edit Hash mask" }).click();
  await page.getByRole("region", { name: "Column masks" }).getByRole("button", { name: "Remove phone", exact: true }).click();
  await page.getByRole("button", { name: "Apply mask", exact: true }).click();
  await expect(page.getByText("1 column shows original values")).toBeVisible();
  await expect(page.getByRole("button", { name: "Edit Hash mask" })).toBeVisible();
});

test("open authoring controls have no serious accessibility violations", async ({ page }) => {
  const { default: AxeBuilder } = await import("@axe-core/playwright");
  await page.getByRole("button", { name: "Add Column Masks", exact: true }).click();
  await page.getByLabel("Mask", { exact: true }).selectOption("hash");
  await page.getByRole("button", { name: "Exempt people or groups", exact: true }).click();
  await page.getByRole("button", { name: "Add Row Filter", exact: true }).click();
  const result = await new AxeBuilder({ page }).include('.policy-authoring').analyze();
  expect(result.violations.filter((item) => ["serious", "critical"].includes(item.impact ?? ""))).toEqual([]);
});

test("reselecting the active rule does not release pending edits", async ({ page }) => {
  let dialogs = 0;
  page.on("dialog", async (dialog) => { dialogs += 1; await dialog.accept(); });
  await page.getByRole("button", { name: "Add Column Masks", exact: true }).click();
  await page.getByLabel("Mask", { exact: true }).selectOption("hash");
  await page.getByRole("button", { name: /^Rule 1/ }).click();
  await expect(page.getByRole("button", { name: "Save policy", exact: true })).toBeDisabled();
  expect(dialogs).toBe(0);
});

test("named rules stack, collapse without losing edits, and persist metadata", async ({ page }) => {
  await expect(page.getByLabel("Mask", { exact: true })).toHaveCount(0);
  await expect(page.getByLabel("Filter value 1")).toHaveCount(0);
  await page.getByLabel("Rule name", { exact: true }).fill("Analyst access");
  await page.getByLabel("Description", { exact: true }).fill("Reporting access with protected contact details");
  await page.getByRole("button", { name: "New rule", exact: true }).click();
  const cards = page.locator('.collapsible-rule');
  await expect(cards).toHaveCount(2);
  await cards.nth(1).getByLabel("Rule name", { exact: true }).fill("Regional restriction");
  const first = await cards.first().boundingBox(); const second = await cards.nth(1).boundingBox();
  expect(second!.y).toBeGreaterThan(first!.y + first!.height - 1);
  await cards.first().getByRole("button", { name: /^Analyst access/ }).click();
  await expect(cards.first().getByLabel("Rule name", { exact: true })).not.toBeVisible();
  await cards.first().getByRole("button", { name: /^Analyst access/ }).click();
  await expect(cards.first().getByLabel("Description", { exact: true })).toHaveValue("Reporting access with protected contact details");
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  await expect(page.getByText(/Live policy saved/)).toBeVisible();
  await page.reload();
  await expect(cards.first().getByLabel("Rule name", { exact: true })).toHaveValue("Analyst access");
  await expect(cards.nth(1).getByRole("button", { name: /^Regional restriction/ })).toBeVisible();
});

test("both test buttons open the same modal and retain persona inputs", async ({ page }) => {
  await expect(page.getByRole("tab", { name: "Tests", exact: true })).toHaveCount(0);
  await page.getByRole("button", { name: "Test Policy", exact: true }).click();
  const modal = page.getByRole("dialog", { name: "Test policy", exact: true });
  await modal.getByLabel("Principal", { exact: true }).fill("user:alice");
  await modal.getByLabel("Groups", { exact: true }).fill("analysts, privacy");
  await modal.getByLabel("Synthetic persona claims").fill('{"region":"US"}');
  await page.keyboard.press("Escape");
  await expect(modal).not.toBeVisible();
  await page.getByRole("button", { name: "Test saved policy", exact: true }).click();
  await expect(modal.getByLabel("Principal", { exact: true })).toHaveValue("user:alice");
  await expect(modal.getByLabel("Groups", { exact: true })).toHaveValue("analysts, privacy");
  await expect(modal.getByLabel("Synthetic persona claims")).toHaveValue('{"region":"US"}');
  await expect(modal.getByRole("table")).toHaveCount(0);
});

test("allow all is an explicit removable bypass and discard restores restrictions", async ({ page }) => {
  await page.getByRole("button", { name: "Allow all to all users", exact: true }).click();
  await expect(page.getByText("Allow all is enabled", { exact: true })).toBeVisible();
  await expect(page.getByRole("button", { name: "Add Column Masks" })).toBeDisabled();
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  await expect(page.getByText(/Live policy saved/)).toBeVisible();
  await page.reload();
  const bypass = page.locator('.collapsible-rule').filter({ has: page.getByRole("button", { name: /Allow all.*Global bypass/ }) });
  await bypass.getByRole("button", { name: /Allow all.*Global bypass/ }).click();
  await bypass.getByRole("button", { name: "Delete rule", exact: true }).click();
  await expect(page.getByText("Every column starts with a NULL mask", { exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Discard all changes" }).click();
  await expect(page.getByText("Allow all is enabled", { exact: true })).toBeVisible();
});

test("column shortcuts select all, exclude a field, and add a prefix", async ({ page }) => {
  await page.getByRole("button", { name: /Allowed columns:/ }).click();
  await page.getByRole("button", { name: "Select all", exact: true }).click();
  await expect(page.getByRole("button", { name: "Allowed columns: 3 selected" })).toHaveCount(1);
  await page.getByRole("button", { name: "Select all except…", exact: true }).click();
  await page.getByRole("combobox", { name: "Search allowed columns" }).fill("phone");
  await page.getByRole("option", { name: /phone/ }).click();
  await page.getByRole("button", { name: /Apply exclusions/ }).click();
  await expect(page.getByRole("button", { name: "Allowed columns: 2 selected" })).toHaveCount(1);
  await page.getByRole("button", { name: "Add by prefix", exact: true }).click();
  await page.getByLabel("Column path prefix").fill("pho");
  await page.getByRole("button", { name: "Add matching columns", exact: true }).click();
  await page.getByRole("button", { name: "Done", exact: true }).click();
  await expect(page.getByRole("button", { name: "Allowed columns: 3 selected" })).toBeVisible();
});
