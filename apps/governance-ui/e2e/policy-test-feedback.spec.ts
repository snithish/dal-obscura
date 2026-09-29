import { expect, test } from "@playwright/test";
import { authenticatedApi, deferredResponse } from "./fixtures";

test("policy testing validates locally and clears stale persona results", async ({ page }) => {
  await authenticatedApi(page);
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await page.getByRole("button", { name: "Test Policy", exact: true }).click();
  const modal = page.getByRole("dialog", { name: "Test policy", exact: true });
  const run = modal.getByRole("button", { name: "Test saved policy", exact: true });
  await modal.getByLabel("Synthetic persona claims").fill("[]");
  await expect(modal.getByText("Claims must be a JSON object.", { exact: true })).toBeVisible();
  await expect(run).toBeDisabled();
  await modal.getByLabel("Synthetic persona claims").fill("{}");
  await modal.getByLabel("Principal", { exact: true }).fill("   ");
  await expect(run).toBeDisabled();
  await modal.getByLabel("Principal", { exact: true }).fill("analyst");
  await run.click();
  await expect(modal.getByText(/^Current test:/)).toBeVisible();
  await modal.getByLabel("Principal", { exact: true }).fill("someone-else");
  await expect(modal.getByText(/^Current test:/)).toHaveCount(0);
});

test("policy test shows running state and actionable server errors inside modal", async ({ page }) => {
  await authenticatedApi(page);
  const deferred = deferredResponse();
  await page.route("**/policy-evaluate", async (route) => {
    deferred.markStarted(); await deferred.wait();
    await route.fulfill({ status: 422, json: { error: { code: "validation_error", message: "Request validation failed", field_errors: [{ field: "claims", message: "Use a string for region.", type: "value_error" }] } } });
  });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await page.getByRole("button", { name: "Test Policy", exact: true }).click();
  const modal = page.getByRole("dialog", { name: "Test policy", exact: true });
  await modal.getByRole("button", { name: "Test saved policy", exact: true }).click();
  await deferred.started;
  await expect(modal.getByRole("button", { name: "Testing…", exact: true })).toBeDisabled();
  deferred.release();
  await expect(modal.getByRole("alert")).toContainText("Use a string for region.");
});

test("a late policy test cannot show results for a different persona", async ({ page }) => {
  const deferredEvaluate = deferredResponse();
  await authenticatedApi(page, { deferredEvaluate });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await page.getByRole("button", { name: "Test Policy", exact: true }).click();
  const modal = page.getByRole("dialog", { name: "Test policy", exact: true });
  await modal.getByRole("button", { name: "Test saved policy", exact: true }).click();
  await deferredEvaluate.started;
  await modal.getByLabel("Principal", { exact: true }).fill("different-reader");
  deferredEvaluate.release();
  await expect(modal.getByRole("button", { name: "Test saved policy", exact: true })).toBeEnabled();
  await expect(modal.getByText(/^Current test:/)).toHaveCount(0);
});

test("policy save errors reveal the affected rule and focus the validation summary", async ({ page }) => {
  await authenticatedApi(page, { ruleEditor: true, initialPolicyRules: [10, 20].map((ordinal) => ({ ordinal, name: "Rule " + ordinal, effect: "allow", principals: ["*"], columns: ["email"], masks: {}, when: {}, row_filter: null })) });
  await page.route("**/policy", (route) => route.fulfill({ status: 422, json: { error: { code: "validation_error", message: "Request validation failed", field_errors: [{ field: "rules.1", message: "Invalid row_filter SQL", type: "value_error" }] } } }));
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  const second = page.getByRole("button", { name: /^Rule 20/ });
  await expect(second).toHaveAttribute("aria-expanded", "false");
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  await expect(page.locator(".validation-summary")).toBeFocused();
  await expect(page.locator(".validation-summary")).toContainText("Rule 2: Invalid row_filter SQL");
  await expect(second).toHaveAttribute("aria-expanded", "true");
});
