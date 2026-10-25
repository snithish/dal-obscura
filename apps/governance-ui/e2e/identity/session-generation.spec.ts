import { savePolicy } from "../support/policy";
import { expect, test } from "@playwright/test";
import { authenticatedApi, deferredResponse } from "../support/fixtures";

for (const operation of ["asset load", "policy save"] as const) {
  test(`signing in as another identity fences a deferred ${operation}`, async ({ page }) => {
    const pending = deferredResponse();
    await authenticatedApi(page, operation === "policy save" ? { deferredSave: pending } : {});
    await page.route("**/v1/session/options", (route) =>
      route.fulfill({ json: { bootstrap_enabled: true, oidc: null } }),
    );
    await page.route("**/v1/session/bootstrap", (route) =>
      route.fulfill({ json: { authenticated: true } }),
    );
    await page.route("**/v1/logout", (route) => route.fulfill({ json: { authenticated: false } }));
    const schema = (name: string) => ({
      asset_id: "00000000-0000-4000-8000-000000000001",
      catalog: "demo",
      target: "demo.orders",
      schema_version: 1,
      schema_fingerprint: name,
      stable_field_ids: true,
      supported_masks: ["null"],
      fields: [
        {
          field_id: 1,
          name,
          human_path: name,
          type: "string",
          nullable: false,
          kind: "scalar",
          path: { version: 1, segments: [{ kind: "field", name, field_id: 1 }] },
        },
      ],
    });
    if (operation === "asset load") {
      await page.route("**/v1/assets/*/schema", async (route) => {
        pending.markStarted(page, route.request());
        await pending.wait();
        await route.fulfill({ json: schema("alice_private") });
      });
    }
    await page.goto("/#assets");
    await page.getByRole("button", { name: "orders", exact: true }).click();
    if (operation === "policy save") {
      await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
      await savePolicy(page);
    }
    await pending.started;
    await page.getByRole("button", { name: "Sign out", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "orders" })).toHaveCount(0);
    // A fresh fixture and different actor must not inherit Alice's pending data.
    await authenticatedApi(page);
    if (operation === "asset load") {
      await page.route("**/v1/assets/*/schema", (route) =>
        route.fulfill({ json: schema("bob_public") }),
      );
    }
    await page.route("**/v1/session/bootstrap", (route) =>
      route.fulfill({ json: { authenticated: true } }),
    );
    await page.route("**/v1/session", (route) =>
      route.fulfill({
        json: {
          principal: "bob@example.invalid",
          groups: ["reviewers"],
          platform_admin: false,
          capabilities: ["asset:read", "asset:edit"],
        },
      }),
    );
    await page.getByLabel("Local control-plane token").fill("synthetic-bob-token");
    await page.getByRole("button", { name: "Sign in locally", exact: true }).click();
    await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
    await expect(page.getByText("bob@example.invalid", { exact: true })).toBeVisible();
    pending.release();
    await pending.completed;
    if (operation === "asset load") {
      await page.getByRole("button", { name: "Show schema", exact: true }).click();
      const sidebar = page.getByRole("complementary", { name: "Asset schema" });
      await expect(sidebar.getByText("bob_public", { exact: true })).toBeVisible();
      await expect(sidebar.getByText("alice_private", { exact: true })).toHaveCount(0);
    }
    await expect(page.getByText("Live revision 1", { exact: true })).toBeVisible();
    await expect(page.getByText("Live revision 2", { exact: true })).toHaveCount(0);
    await expect(
      page.getByText(
        "Live policy saved. Existing tokens remain valid until expiry unless an owner revokes them.",
      ),
    ).toHaveCount(0);
    await expect(page.getByText("bob@example.invalid", { exact: true })).toBeVisible();
  });
}

test("failed logout hides private drafts before revocation and supports retry", async ({
  page,
}) => {
  await authenticatedApi(page);
  const pending = deferredResponse();
  let attempts = 0;
  await page.route("**/v1/logout", async (route) => {
    if (++attempts === 1) {
      pending.markStarted(page, route.request());
      await pending.wait();
      return route.fulfill({
        status: 503,
        json: {
          error: {
            code: "request_rejected",
            message: "Synthetic revocation failure",
            request_id: "test-request",
          },
        },
      });
    }
    return route.fulfill({ json: { authenticated: false } });
  });
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await page.getByRole("button", { name: "New rule", exact: true }).click();
  await expect(page.getByText("Unsaved changes")).toBeVisible();
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await expect(page.getByRole("dialog", { name: "Discard unsaved changes?" })).toBeVisible();
  expect(attempts).toBe(0);
  await page.getByRole("button", { name: "Discard changes", exact: true }).click();
  await pending.started;
  await expect(page.getByRole("heading", { name: "orders" })).toHaveCount(0);
  await expect(page.getByText("Unsaved changes")).toHaveCount(0);
  pending.release();
  await pending.completed;
  await expect(
    page.getByText(
      "Sign out could not be confirmed. Private data is hidden; retry sign out before closing this browser.",
    ),
  ).toBeVisible();
  await page.getByRole("button", { name: "Retry sign out", exact: true }).click();
  await expect(
    page.getByText("Signed out. No policy data remains loaded in this browser."),
  ).toBeVisible();
  expect(attempts).toBe(2);
});
