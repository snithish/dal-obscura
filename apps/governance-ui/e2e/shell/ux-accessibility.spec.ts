import { expect, test } from "@playwright/test";
import { identityApi } from "../identity/fixtures";
import { authenticatedApi } from "../support/fixtures";
import { deferredResponse } from "../support/async";

test("provider validation waits for blur and clears while retyping", async ({ page }) => {
  await identityApi(page);
  await page.goto("/#settings");
  const issuer = page.getByLabel("Issuer URL", { exact: true });
  await issuer.fill("");
  await expect(page.getByText("Issuer URL is required.", { exact: true })).toHaveCount(0);
  await issuer.blur();
  await expect(
    page.getByRole("alert").filter({ hasText: "Issuer URL is required." }),
  ).toBeVisible();
  await issuer.fill("https://new.example.invalid");
  await expect(page.getByText("Issuer URL is required.", { exact: true })).toHaveCount(0);
});
test("bootstrap token discourages password autofill", async ({ page }) => {
  await authenticatedApi(page);
  await page.goto("/#assets");
  await expect(page.getByRole("heading", { name: "Governed assets" })).toBeVisible();
  await page.route("**/v1/session/options", (route) =>
    route.fulfill({ json: { bootstrap_enabled: true, oidc: null } }),
  );
  await page.getByRole("button", { name: "Sign out", exact: true }).click();
  await expect(page.getByLabel("Local control-plane token")).toHaveAttribute("autocomplete", "off");
});
test("activity refresh exposes busy state while retaining loaded rows", async ({ page }) => {
  const pending = deferredResponse();
  await authenticatedApi(page, { admin: true, configuredAudit: true });
  await page.goto("/#activity");
  await expect(page.getByRole("heading", { name: "Workspace status" })).toBeVisible();
  await page.getByLabel("Action", { exact: true }).fill("asset.policy.replace");
  await page.getByRole("button", { name: "Apply filters", exact: true }).click();
  const row = page.locator("strong").filter({ hasText: "asset.policy.replace" });
  await expect(row).toBeVisible();
  await page.route("**/v1/audit/events/page?*", async (route) => {
    pending.markStarted(page, route.request());
    await pending.wait();
    await route.fulfill({ json: { items: [], next_cursor: null } });
  });
  await page.getByPlaceholder("platform:admin").fill("stale");
  await page.getByRole("button", { name: "Apply filters", exact: true }).click();
  await pending.started;
  try {
    await expect(page.getByRole("status").filter({ hasText: "Updating activity…" })).toBeVisible();
    await expect(page.locator('[aria-busy="true"]')).toBeVisible();
    await expect(row).toBeVisible();
  } finally {
    pending.release();
  }
  await pending.completed;
  await expect(row).toHaveCount(0);
});

test("submitting a new untouched provider validates its required issuer locally", async ({
  page,
}) => {
  await authenticatedApi(page, { admin: true });
  let writes = 0;
  page.on("request", (request) => {
    if (request.method() === "PUT" && request.url().endsWith("/settings/auth-providers"))
      writes += 1;
  });
  await page.goto("/#settings");
  await page.getByRole("button", { name: "Add OIDC provider", exact: true }).click();
  await page.getByRole("button", { name: "Save identity providers", exact: true }).click();
  await expect(page.getByText("Issuer URL is required.", { exact: true })).toBeVisible();
  expect(writes).toBe(0);
});
