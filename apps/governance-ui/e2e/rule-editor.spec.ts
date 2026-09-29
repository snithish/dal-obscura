import { expect, test } from "@playwright/test";
import { authenticatedApi } from "./fixtures";
const rule = { ordinal: 10, effect: "allow", principals: ["group:analysts"], columns: ["email"], masks: {}, row_filter: null, when: {} };

test("column picker preserves keyboard search selection and focus", async ({ page }) => {
  await authenticatedApi(page, { ruleEditor: true, initialPolicyRules: [rule] });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  const trigger = page.getByRole("button", { name: /Allowed columns: \d+ selected/ });
  await trigger.click();
  const search = page.getByRole("combobox", { name: "Search allowed columns" });
  await search.fill("phone"); await search.press("Enter");
  await expect(trigger).toHaveText(/2 selected/);
  await search.press("Escape"); await expect(trigger).toBeFocused();
});

test("read-only access disables all authoring", async ({ page }) => {
  await authenticatedApi(page, { ruleEditor: true, readOnly: true, initialPolicyRules: [rule] });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await expect(page.getByRole("button", { name: /Allowed columns:/ })).toBeDisabled();
  await expect(page.getByRole("button", { name: "Add Column Masks", exact: true })).toBeDisabled();
  await expect(page.getByRole("button", { name: "Add Row Filter", exact: true })).toBeDisabled();
  await expect(page.getByRole("button", { name: "Save policy", exact: true })).toBeDisabled();
});

test("editor fits a narrow viewport", async ({ page }, testInfo) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await authenticatedApi(page, { ruleEditor: true, initialPolicyRules: [rule] });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await page.getByRole("button", { name: "Add Column Masks", exact: true }).click();
  await page.getByLabel("Mask", { exact: true }).selectOption("hash");
  await page.getByRole("button", { name: "Exempt people or groups", exact: true }).click();
  await expect(page.getByRole("button", { name: "Apply mask", exact: true })).toBeVisible();
  expect(await page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth)).toBe(true);
  await page.screenshot({ path: testInfo.outputPath("policy-mobile.png"), fullPage: true });
});

test("revision conflicts preserve column edits", async ({ page }) => {
  await authenticatedApi(page, { ruleEditor: true, conflictSave: true, initialPolicyRules: [rule] });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await page.getByRole("button", { name: /Allowed columns:/ }).click();
  await page.getByRole("combobox", { name: "Search allowed columns" }).fill("phone");
  await page.getByRole("option", { name: /phone/ }).click();
  await page.getByRole("button", { name: "Done", exact: true }).click();
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  await expect(page.getByText(/resource changed on the server/i).first()).toBeVisible();
  await expect(page.getByRole("button", { name: /Allowed columns: 2 selected/ })).toBeVisible();
});
