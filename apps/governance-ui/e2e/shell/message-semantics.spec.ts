import { expect, test } from "@playwright/test";
import { authenticatedApi } from "../support/fixtures";

for (const action of ["connection", "runtime", "owners", "providers"] as const) {
  test(`${action} errors are announced inside the owning card`, async ({ page }) => {
    await authenticatedApi(page, { admin: true, configuredSettings: true });
    if (action === "connection") {
      await page.goto("/#connections");
      await page.getByRole("button", { name: "Save connection", exact: true }).click();
      const card = page
        .locator(".form-card")
        .filter({ has: page.getByRole("heading", { name: "Add catalog", exact: true }) });
      await expect(card.getByRole("alert")).toHaveText("Connection name is required.");
    } else if (action === "runtime") {
      await page.goto("/#settings");
      await page.getByLabel("Ticket TTL (seconds)").fill("0");
      await page.getByRole("button", { name: "Save runtime settings", exact: true }).click();
      const card = page
        .locator(".form-card")
        .filter({ has: page.getByRole("heading", { name: "Runtime limits", exact: true }) });
      await expect(card.getByRole("alert")).toContainText("Enter positive values");
    } else if (action === "owners") {
      await page.goto("/?asset=00000000-0000-4000-8000-000000000001&tab=access#assets");
      await page.getByLabel("Owner principals").fill("");
      await page.getByRole("button", { name: "Save owners", exact: true }).click();
      const card = page
        .locator(".form-card")
        .filter({ has: page.getByRole("heading", { name: "Owners", exact: true }) });
      await expect(card.getByRole("alert")).toContainText("At least one owner is required");
    } else {
      await page.route("**/v1/settings/auth-providers", (route) =>
        route.request().method() === "PUT"
          ? route.fulfill({
              status: 403,
              json: {
                error: { code: "request_rejected", message: "Denied", request_id: "test-request" },
              },
            })
          : route.fallback(),
      );
      await page.goto("/#settings");
      await page.getByRole("button", { name: "Save identity providers", exact: true }).click();
      await page.getByRole("button", { name: "Save with no providers", exact: true }).click();
      const card = page.locator(".form-card").filter({
        has: page.getByRole("heading", { name: "Authentication providers", exact: true }),
      });
      await expect(card.getByRole("alert")).toContainText("not allowed");
    }
  });
}
