import { expect, test } from "@playwright/test";
import { uxApi, uxAssetId } from "../support/ux-fixture";

test("save review names the changed rule, preserves revision and requires explicit submission", async ({
  page,
}) => {
  await uxApi(page);
  const writes: unknown[] = [];
  page.on("request", (request) => {
    if (request.method() === "PUT" && request.url().endsWith("/policy"))
      writes.push(request.postDataJSON());
  });
  await page.goto(`/?asset=${uxAssetId}#assets`);
  await page.getByLabel("Rule name", { exact: true }).fill("Reviewed analysts");
  await page.getByRole("button", { name: "Review changes", exact: true }).click();
  const review = page.getByRole("dialog", { name: "Review policy changes", exact: true });
  await expect(review.getByText("Based on saved revision 1", { exact: true })).toBeVisible();
  await expect(
    review.getByRole("heading", { name: "Reviewed analysts", exact: true }),
  ).toBeVisible();
  await expect(review.getByText("Name", { exact: true })).toBeVisible();
  await expect(review.getByText(/Existing tokens retain their captured policy/)).toBeVisible();
  expect(writes).toEqual([]);
  await page.keyboard.press("Escape");
  await expect(page.getByRole("button", { name: "Review changes", exact: true })).toBeFocused();
  await expect(page.getByLabel("Rule name", { exact: true })).toHaveValue("Reviewed analysts");
  await page.getByRole("button", { name: "Review changes", exact: true }).click();
  await review.getByRole("button", { name: "Save policy", exact: true }).click();
  await expect(page.getByText(/Live policy saved/)).toBeVisible();
  expect(writes).toEqual([
    expect.objectContaining({
      expected_revision: 1,
      revoke_existing_tokens: false,
      rules: [expect.objectContaining({ name: "Reviewed analysts" })],
    }),
  ]);
});

test("rule summary keeps audience and row scope beside the columns", async ({ page }) => {
  await uxApi(page);
  await page.goto(`/?asset=${uxAssetId}#assets`);
  const summary = page.getByRole("region", { name: "Draft rule summary" });
  await expect(summary.getByText("group:analysts", { exact: true })).toBeVisible();
  await expect(summary.getByText("All rows in this rule", { exact: true })).toBeVisible();
  await expect(
    summary.getByText(/Other rules and identity attributes are not evaluated/),
  ).toBeVisible();
});
