import { expect, test } from "@playwright/test";
import { signedOutApi } from "./fixtures";

// Component integration only. Real OIDC/Access qualification remains E07.
test.beforeEach(async ({ page }) => signedOutApi(page));

test("built shell has a fresh style nonce and working library controls under CSP", async ({ page }) => {
  const violations: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error" && /content security policy/i.test(message.text())) violations.push(message.text());
  });
  const response = await page.goto("/");
  const firstNonce = await page.locator('meta[name="csp-nonce"]').getAttribute("content");
  expect(firstNonce).toMatch(/^[a-f0-9]{32}$/);
  const policy = response!.headers()["content-security-policy"];
  expect(policy).toContain(`style-src 'self' 'nonce-${firstNonce}'`);
  expect(policy).not.toMatch(/unsafe-inline|unsafe-eval/);
  await expect(page.getByLabel("Local control-plane token")).toBeVisible();
  await page.getByLabel("Local control-plane token").fill("synthetic-only");
  await page.keyboard.press("Control+k");
  await expect(page.getByRole("dialog", { name: "Command palette" })).toBeVisible();
  await expect(page.getByRole("combobox", { name: "Command search" })).toBeFocused();
  await page.keyboard.press("ArrowDown");
  await expect(page.getByRole("combobox", { name: "Command search" })).toHaveAttribute("aria-activedescendant", /.+/);
  await page.keyboard.press("Shift+Tab");
  await expect(page.getByRole("button", { name: "Close command palette" })).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(page.getByRole("combobox", { name: "Command search" })).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog", { name: "Command palette" })).toHaveCount(0);
  await expect(page.getByLabel("Local control-plane token")).toBeFocused();
  expect(violations).toEqual([]);
  await page.reload();
  expect(await page.locator('meta[name="csp-nonce"]').getAttribute("content")).not.toBe(firstNonce);
});

test("System follows OS changes and explicit choice overrides the OS", async ({ page }) => {
  await page.emulateMedia({ colorScheme: "light" });
  await page.goto("/");
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "light");
  await expect(page.locator(".login-panel")).toHaveCSS("background-color", "rgb(255, 255, 255)");
  await page.emulateMedia({ colorScheme: "dark" });
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "dark");
  await expect(page.locator(".login-panel")).toHaveCSS("background-color", "rgb(23, 31, 44)");
  await page.getByRole("combobox", { name: "Color theme" }).click();
  await page.getByRole("option", { name: "Light theme", exact: true }).click();
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "light");
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "light");
});

test("CSP rejects missing and wrong style nonces", async ({ page }) => {
  await page.goto("/");
  const result = await page.evaluate(() => {
    const target = document.createElement("div");
    target.id = "nonce-probe";
    document.body.append(target);
    const before = getComputedStyle(target).color;
    const insert = (nonce?: string) => {
      const style = document.createElement("style");
      if (nonce) style.nonce = nonce;
      style.textContent = "#nonce-probe { color: rgb(1, 2, 3) !important; }";
      document.head.append(style);
      return getComputedStyle(target).color;
    };
    const absent = insert();
    const wrong = insert("00000000000000000000000000000000");
    const accepted = insert(document.querySelector<HTMLMetaElement>('meta[name="csp-nonce"]')!.content);
    return { before, absent, wrong, accepted };
  });
  expect(result.absent).toBe(result.before);
  expect(result.wrong).toBe(result.before);
  expect(result.accepted).toBe("rgb(1, 2, 3)");
});
