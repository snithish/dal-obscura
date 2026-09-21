import AxeBuilder from "@axe-core/playwright";
import { expect, test } from "@playwright/test";
import { authenticatedApi, deferredResponse, signedOutApi } from "./fixtures";

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

test("authenticated mobile navigation respects capabilities and restores focus", async ({ page }) => {
  await authenticatedApi(page);
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/#assets");

  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  const openNavigation = page.getByRole("button", { name: "Open navigation menu" });
  await openNavigation.click();
  await expect(page.getByRole("dialog", { name: "Navigation" })).toBeVisible();
  await expect(page.getByRole("dialog", { name: "Navigation" }).getByRole("button", { name: "Settings" })).toBeDisabled();
  await expect(page.getByRole("dialog", { name: "Navigation" }).getByRole("button", { name: "Assets" })).toHaveAttribute("aria-current", "page");

  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog", { name: "Navigation" })).toHaveCount(0);
  await expect(openNavigation).toBeFocused();

  await openNavigation.click();
  await page.getByRole("dialog", { name: "Navigation" }).getByRole("button", { name: "Activity" }).click();
  await expect(page.getByRole("heading", { name: "Activity", exact: true })).toBeVisible();
  await expect(page.getByRole("dialog", { name: "Navigation" })).toHaveCount(0);
});

test("stale review links remain explicit and read-only", async ({ page }) => {
  await authenticatedApi(page);
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001&draft=old-draft&draft_revision=2#assets");

  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
  await expect(page.getByText(/This review link is stale: it requested draft revision 2/)).toBeVisible();
  await expect(page.getByRole("button", { name: "Save deny-all draft" })).toBeDisabled();
  await expect(page.getByLabel("Find governed asset")).toBeDisabled();
});

test("late management responses cannot replace the current page", async ({ page }) => {
  const deferredAudit = deferredResponse();
  await authenticatedApi(page, { deferredAudit });
  await page.goto("/#activity");
  await deferredAudit.started;

  await page.getByRole("button", { name: "Changes" }).click();
  await expect(page.getByRole("heading", { name: "Published policy history" })).toBeVisible();
  await page.getByRole("button", { name: "Activity" }).click();
  await expect(page.getByRole("heading", { name: "Workspace status" })).toBeVisible();
  await expect(page.getByText("fresh-audit")).toBeVisible();

  deferredAudit.release();
  await expect(page.getByText("fresh-audit")).toBeVisible();
  await expect(page.getByText("stale-audit")).toHaveCount(0);
});
