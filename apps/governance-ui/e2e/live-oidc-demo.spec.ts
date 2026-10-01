import { expect, test } from "@playwright/test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

test.use({ trace: "off" });
if (process.env.DAL_OBSCURA_DEMO_TLS_SPKI) {
  test.use({ launchOptions: {
    args: [`--ignore-certificate-errors-spki-list=${process.env.DAL_OBSCURA_DEMO_TLS_SPKI}`],
    ...(process.env.DAL_OBSCURA_E2E_EXECUTABLE_PATH
      ? { executablePath: process.env.DAL_OBSCURA_E2E_EXECUTABLE_PATH } : {}),
  } });
}

const enabled = process.env.DAL_OBSCURA_E2E_LIVE_OIDC === "1";

function demoAdminPassword(): string {
  const runtimeEnv = resolve(
    process.cwd(),
    process.env.DAL_OBSCURA_DEMO_ENV_FILE ?? "../../examples/demo/keycloak/.env",
  );
  const values = Object.fromEntries(
    readFileSync(runtimeEnv, "utf8")
      .split(/\r?\n/)
      .filter((line) => line && !line.startsWith("#"))
      .map((line) => {
        const separator = line.indexOf("=");
        return separator < 0 ? [line, ""] : [line.slice(0, separator), line.slice(separator + 1)];
      }),
  );
  const password = values.DEMO_ADMIN_PASSWORD;
  if (!password) throw new Error("Local demo admin password is missing; run ./demo init first.");
  return password;
}

test("live local Keycloak sign-in, governed inventory, and sign-out", async ({ page }) => {
  test.skip(!enabled, "set DAL_OBSCURA_E2E_LIVE_OIDC=1 to run against the local demo");
  const baseURL = process.env.DAL_OBSCURA_E2E_BASE_URL ?? "http://localhost:28821";
  const origin = new URL(baseURL).origin;

  await page.goto("/");
  await page.getByRole("button", { name: "Sign in with SSO" }).click();
  await expect(page).toHaveURL(/localhost:\d+\/realms\/dal-obscura-demo\/protocol\/openid-connect\/auth/);
  await page.getByLabel("Username or email").fill("demo-admin");
  await page.getByRole("textbox", { name: "Password" }).fill(demoAdminPassword());
  await page.getByRole("button", { name: "Sign In" }).click();
  await page.waitForURL((url) => url.origin === origin);

  const sessionResponse = await page.request.get(`${origin}/v1/session`);
  expect(sessionResponse.status()).toBe(200);
  const session = await sessionResponse.json();
  expect(session.principal).toBe("demo-admin");
  expect(session.groups).toContain("platform-admins");
  expect(session.platform_admin).toBe(true);

  if (origin.startsWith("https://")) {
    const cookies = await page.context().cookies(origin);
    const sessionCookie = cookies.find((cookie) => cookie.name === "__Host-dal_obscura_session");
    expect(sessionCookie?.secure).toBe(true);
    expect(sessionCookie?.httpOnly).toBe(true);
    expect(sessionCookie?.sameSite).toBe("Lax");
    const csrfFailure = await page.request.post(`${origin}/v1/logout`);
    expect(csrfFailure.status()).toBe(403);
    expect((await page.request.get(`${origin}/v1/session`)).status()).toBe(200);
  }

  const assetResponse = await page.request.get(`${origin}/v1/assets`);
  expect(assetResponse.status()).toBe(200);
  const assets = await assetResponse.json();
  expect(Array.isArray(assets) && assets.length).toBeGreaterThan(0);
  await expect(page.getByRole("heading", { name: "Assets" })).toBeVisible();
  await page.reload();
  await expect(page.getByRole("heading", { name: "Assets" })).toBeVisible();

  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("button", { name: "Logout", exact: true })).toBeVisible();
  await page.getByRole("button", { name: "Logout", exact: true }).click();
  await page.waitForURL((url) => url.origin === origin);
  await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
  expect((await page.request.get(`${origin}/v1/session`)).status()).toBe(401);
  await page.getByRole("button", { name: "Sign in with SSO" }).click();
  await expect(page.getByRole("textbox", { name: "Username or email" })).toBeVisible();
  await expect(page.getByRole("textbox", { name: "Password" })).toBeVisible();
});
