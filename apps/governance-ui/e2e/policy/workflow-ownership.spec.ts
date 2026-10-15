import { expect, test } from "@playwright/test";
import { authenticatedApi, deferredResponse } from "../support/fixtures";

const rule = { ordinal: 10, name: "Original", effect: "allow", principals: ["group:analysts"],
  columns: ["order_id"], masks: {}, row_filter: null, when: {} };
const editorUrl = "/?asset=00000000-0000-4000-8000-000000000001#assets";

test("duplicate saves keep one submission and a late success preserves exact newer edits", async ({ page }) => {
  const pending = deferredResponse();
  await authenticatedApi(page, { initialPolicyRules: [rule], deferredSave: pending });
  const submissions: unknown[] = [];
  page.on("request", request => {
    if (request.method() === "PUT" && request.url().endsWith("/policy")) submissions.push(request.postDataJSON());
  });
  await page.goto(editorUrl);
  await page.getByLabel("Rule name", { exact: true }).fill("Submitted name");
  await page.getByRole("button", { name: "Save policy", exact: true }).click({ clickCount: 2 });
  await pending.started;
  await page.getByLabel("Rule name", { exact: true }).fill("Newer local name");
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  expect(submissions).toHaveLength(1);
  expect(submissions[0]).toMatchObject({ expected_revision: 1,
    rules: [{ ...rule, name: "Submitted name" }] });
  pending.release();
  await pending.completed;
  await expect(page.getByText("Live revision 2", { exact: true })).toBeVisible();
  await expect(page.getByText("Unsaved changes", { exact: true })).toBeVisible();
  await expect(page.getByLabel("Rule name", { exact: true })).toHaveValue("Newer local name");
  const next = page.waitForRequest(request => request.method() === "PUT" && request.url().endsWith("/policy"));
  await page.getByRole("button", { name: "Save policy", exact: true }).click();
  expect((await next).postDataJSON()).toMatchObject({ expected_revision: 2,
    rules: [{ ...rule, name: "Newer local name" }] });
});

test("navigation during preview cannot publish the old asset's result on return", async ({ page }) => {
  const pending = deferredResponse();
  await authenticatedApi(page, { initialPolicyRules: [rule], deferredEvaluate: pending });
  await page.goto(editorUrl);
  await page.getByRole("button", { name: "Test Policy", exact: true }).click();
  const modal = page.getByRole("dialog", { name: "Test policy", exact: true });
  await modal.getByLabel("Principal", { exact: true }).fill("previous-route-reader");
  await modal.getByRole("button", { name: "Test saved policy", exact: true }).click();
  await pending.started;
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Assets", exact: true }).click();
  await page.getByRole("button", { name: "orders", exact: true }).click();
  await page.getByRole("button", { name: "Test Policy", exact: true }).click();
  await expect(modal.getByLabel("Principal", { exact: true })).toHaveValue("analyst.alex");
  pending.release();
  await pending.completed;
  await expect(modal.getByRole("button", { name: "Test saved policy", exact: true })).toBeEnabled();
  await expect(modal.getByText(/^Current test:/)).toHaveCount(0);
});
