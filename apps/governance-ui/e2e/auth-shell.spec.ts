import AxeBuilder from "@axe-core/playwright";
import { expect, test, type Page } from "@playwright/test";
import { edgeChallengeApi, signedOutApi, type EdgeChallengeOptions } from "./fixtures";

async function assertEdgeChallenge(page: Page, options: EdgeChallengeOptions) {
  await edgeChallengeApi(page, options);
  await page.goto("/");
  await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
  await expect(page.getByText("The browser or edge session expired. Sign in again to continue.")).toBeVisible();
  await expect(page.getByText("Use demo persona")).toHaveCount(0);
  await expect(page.getByRole("heading", { name: "orders" })).toHaveCount(0);
  await expect(page.getByLabel("Find governed asset")).toHaveCount(0);
}

test.describe("signed-out governance shell", () => {
  test.beforeEach(async ({ page }) => signedOutApi(page));
  test("gates a protected deep link behind normal sign-in", async ({ page }) => {
    await page.goto("/#assets");

    await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
    await expect(page.getByLabel("Local control-plane token")).toBeVisible();
    await expect(page.getByRole("button", { name: "Assets" })).toBeDisabled();
    await expect(page.getByRole("button", { name: "Connections" })).toBeDisabled();
    await page.keyboard.press("Tab");
    await expect(page.getByRole("link", { name: /OBSCURA GOVERNANCE/i })).toBeFocused();
  });

  test("uses the configured SSO entry and hides local bootstrap", async ({ page }) => {
    await signedOutApi(page, { oidc: { authority: "https://idp.example.invalid", client_id: "governance-ui", redirect_uri: "https://ui.example.invalid/auth/callback" } });
    await page.route("**/auth/login", async (route) => route.fulfill({ status: 200, headers: { "content-type": "text/html" }, body: "<title>SSO redirect</title>" }));
    await page.goto("/");

    await expect(page.getByRole("button", { name: "Sign in with SSO" })).toBeVisible();
    await expect(page.getByLabel("Local control-plane token")).toHaveCount(0);
    await page.getByRole("button", { name: "Sign in with SSO" }).click();
    await expect(page).toHaveURL(/\/auth\/login$/);
  });

  for (const [name, options] of [
    ['HTML success response', {}],
    ['HTML forbidden response', {status: 403}],
    ['redirect to HTML challenge', {redirect: true}],
  ] as const) {
    test(name + ' requires fresh sign-in', async ({page}) => {
      await assertEdgeChallenge(page, options);
    });
  }

  test("has no serious or critical accessibility violations when signed out", async ({ page }) => {
    await page.goto("/#assets");

    const results = await new AxeBuilder({ page }).analyze();
    const blockingViolations = results.violations.filter(
      (violation) => violation.impact === "serious" || violation.impact === "critical",
    );
    expect(blockingViolations).toEqual([]);
  });

  test("keeps signed-out actions inside the viewport at supported sizes and zoom", async ({ page }) => {
    for (const viewport of [{ width: 390, height: 844 }, { width: 768, height: 1024 }, { width: 1440, height: 900 }]) {
      await page.setViewportSize(viewport);
      await page.goto("/");
      const login = page.getByRole("heading", { name: "Sign in to your workspace" });
      const token = page.getByLabel("Local control-plane token");
      const button = page.getByRole("button", { name: "Sign in locally" });
      await expect(login).toBeVisible();
      await expect(token).toBeVisible();
      await expect(button).toBeVisible();
      if (viewport.width === 390) {
        await expect(page.getByRole("button", { name: "Open navigation menu" })).toBeVisible();
      }
      for (const element of [login, token, button]) {
        const box = await element.boundingBox();
        expect(box).not.toBeNull();
        expect(box!.x).toBeGreaterThanOrEqual(0);
        expect(box!.x + box!.width).toBeLessThanOrEqual(viewport.width + 1);
      }
    }

    await page.evaluate(() => { document.body.style.zoom = "2"; });
    const zoomedButton = page.getByRole("button", { name: "Sign in locally" });
    await expect(zoomedButton).toBeVisible();
    const zoomedBox = await zoomedButton.boundingBox();
    expect(zoomedBox).not.toBeNull();
    expect(zoomedBox!.x).toBeGreaterThanOrEqual(0);
    expect(zoomedBox!.x + zoomedBox!.width).toBeLessThanOrEqual(1440 + 1);
  });
});
