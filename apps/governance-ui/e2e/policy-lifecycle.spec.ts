import { expect, test } from "@playwright/test";
import { authenticatedApi } from "./fixtures";

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
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await expect(page.getByText("Live revision 1", { exact: true })).toBeVisible();
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
  const allTokensRequest = page.waitForRequest((request) => request.method() === "POST" && request.url().endsWith(`/v1/assets/${assetId}/tickets/revoke`));
  await page.getByRole("button", { name: "Revoke all active tokens" }).click();
  await page.getByRole("button", { name: "Revoke tokens", exact: true }).click();
  await allTokensRequest;
  await expect(page.getByText("Revoked 2 active token(s)." )).toBeVisible();
});
