import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { signedOutApi } from "./fixtures";

test.describe("signed-out governance shell", () => {
  test.beforeEach(async ({ page }) => signedOutApi(page));
  test("gates a protected deep link behind normal sign-in", async ({ page }) => {
    await page.goto("/#assets");

    await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
    await expect(page.getByLabel("Local control-plane token")).toBeVisible();
    await expect(page.getByRole("button", { name: "Assets" })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Connections" })).toBeDisabled();
  });

  test("does not restore the retired demo bypass", async ({ page }) => {
    await page.goto("/?demo#assets");

    await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
    await expect(page.getByText("Not signed in")).toBeVisible();
    await expect(page.getByText("Use demo persona")).toHaveCount(0);
  });

  test("has no serious or critical accessibility violations when signed out", async ({ page }) => {
    await page.goto("/#assets");

    const results = await new AxeBuilder({ page }).analyze();
    const blockingViolations = results.violations.filter(
      (violation) => violation.impact === "serious" || violation.impact === "critical",
    );
    expect(blockingViolations).toEqual([]);
  });

  test("keeps keyboard focus in the public shell", async ({ page }) => {
    await page.goto("/#assets");

    await page.keyboard.press("Tab");
    await expect(page.getByRole("link", { name: /OBSCURA GOVERNANCE/i })).toBeFocused();
  });

  test("keeps sign-in usable at a narrow viewport", async ({ page }) => {
    await page.setViewportSize({ width: 390, height: 844 });
    await page.goto("/#assets");

    await expect(page.getByRole("button", { name: "Open navigation menu" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
    await expect(page.getByLabel("Local control-plane token")).toBeVisible();
  });
});
