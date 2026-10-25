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

test("revision conflict compares saved settings before rebasing the retained draft", async ({
  page,
}) => {
  await uxApi(page);
  await page.goto(`/?asset=${uxAssetId}#assets`);
  await page.getByLabel("Rule name", { exact: true }).fill("My retained draft");
  let attempts = 0;
  await page.route(`**/v1/assets/${uxAssetId}/policy`, (route) => {
    attempts++;
    return attempts === 1
      ? route.fulfill({
          status: 409,
          json: { error: { code: "revision_conflict", message: "Changed", current_revision: 2 } },
        })
      : route.fulfill({ json: { policy_revision: 3, revoked_token_count: 0 } });
  });
  await page.getByRole("button", { name: "Review changes", exact: true }).click();
  await page
    .getByRole("dialog", { name: "Review policy changes", exact: true })
    .getByRole("button", { name: "Save policy", exact: true })
    .click();
  await expect(page.getByRole("button", { name: "Review changes", exact: true })).toBeDisabled();
  await page.route(`**/v1/assets/${uxAssetId}`, (route) =>
    route.fulfill({
      json: {
        id: uxAssetId,
        revision: 2,
        policy_revision: 2,
        catalog: "retail_demo",
        name: "retail.customer_revenue",
        backend: "iceberg",
        owners: [],
        schema_fields: [],
        policy_rules: [
          {
            ordinal: 10,
            name: "Server change",
            description: "",
            effect: "allow",
            principals: ["group:analysts"],
            columns: ["country"],
            masks: {},
            row_filter: null,
            when: {},
          },
        ],
      },
    }),
  );
  await page.getByRole("button", { name: "Check saved policy", exact: true }).click();
  const comparison = page.getByRole("dialog", { name: "Compare saved policy", exact: true });
  await expect(comparison.getByText("Based on saved revision 2", { exact: true })).toBeVisible();
  await comparison.getByText("Saved settings", { exact: true }).click();
  await expect(comparison.getByText("Name: Server change", { exact: true })).toBeVisible();
  expect(attempts).toBe(1);
  await comparison.getByRole("button", { name: "Use saved revision", exact: true }).click();
  await expect(page.getByLabel("Rule name", { exact: true })).toHaveValue("My retained draft");
  await expect(page.getByText("Live revision 2", { exact: true })).toBeVisible();
  const next = page.waitForRequest(
    (request) => request.method() === "PUT" && request.url().endsWith("/policy"),
  );
  await page.getByRole("button", { name: "Review changes", exact: true }).click();
  await page
    .getByRole("dialog", { name: "Review policy changes", exact: true })
    .getByRole("button", { name: "Save policy", exact: true })
    .click();
  expect((await next).postDataJSON()).toMatchObject({
    expected_revision: 2,
    rules: [expect.objectContaining({ name: "My retained draft" })],
  });
});

test("unknown save outcomes keep the draft and require checking saved policy before retry", async ({
  page,
}) => {
  await uxApi(page);
  await page.goto(`/?asset=${uxAssetId}#assets`);
  await page.getByLabel("Rule name", { exact: true }).fill("Unconfirmed draft");
  let attempts = 0;
  await page.route(`**/v1/assets/${uxAssetId}/policy`, (route) => {
    attempts++;
    return route.abort("failed");
  });
  await page.getByRole("button", { name: "Review changes", exact: true }).click();
  await page
    .getByRole("dialog", { name: "Review policy changes", exact: true })
    .getByRole("button", { name: "Save policy", exact: true })
    .click();
  await expect(page.getByText(/The save outcome is unknown/)).toBeVisible();
  await expect(page.getByRole("button", { name: "Review changes", exact: true })).toBeDisabled();
  await expect(page.getByLabel("Rule name", { exact: true })).toHaveValue("Unconfirmed draft");
  await page.getByRole("button", { name: "Check saved policy", exact: true }).click();
  await expect(
    page.getByRole("dialog", { name: "Compare saved policy", exact: true }),
  ).toBeVisible();
  expect(attempts).toBe(1);
});
