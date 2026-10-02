import type { Page } from '@playwright/test';
import type { AuthenticatedApiOptions } from './api-types';
export function fixtureIdentity(options: AuthenticatedApiOptions) {
  const identity = {
    principal: "alex@example.invalid",
    groups: ["analysts"],
    platform_admin: Boolean(options.admin),
    capabilities: options.admin ? ["asset:read", "asset:edit", "workspace:admin"] : ["asset:read", "asset:edit"],
  };
  return identity;
}

/** Synthetic API boundary for component tests, never evidence of live SSO. */
export async function signedOutApi(page: Page, options: { oidc?: { authority: string; client_id: string; redirect_uri: string } } = {}) {
  await page.route("**/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/v1/session/options") return route.fulfill({ json: { bootstrap_enabled: !options.oidc, oidc: options.oidc ?? null } });
    if (path === "/v1/ui-auth-config") return route.fulfill({ json: { authority: options.oidc?.authority ?? null } });
    return route.fulfill({ status: 401, json: { detail: "Sign in required" } });
  });
}

export type EdgeChallengeOptions = { status?: number; redirect?: boolean };

export async function edgeChallengeApi(page: Page, options: EdgeChallengeOptions = {}) {
  await page.unroute("**/v1/**");
  await page.route("**/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/v1/session" && route.request().method() === "GET") {
      if (options.redirect) return route.fulfill({ status: 302, headers: { location: "/v1/session/challenge" } });
      return route.fulfill({ status: options.status ?? 200, headers: { "content-type": "text/html" }, body: "<html><title>Sign in</title></html>" });
    }
    if (path === "/v1/session/challenge") return route.fulfill({ status: 200, headers: { "content-type": "text/html" }, body: "<html><title>Sign in</title></html>" });
    if (path === "/v1/session/options") return route.fulfill({ json: { bootstrap_enabled: false, oidc: null } });
    if (path === "/v1/ui-auth-config") return route.fulfill({ json: { authority: null } });
    return route.fulfill({ status: 401, json: { detail: "Sign in required" } });
  });
}


export async function installSessionApi(page: Page, options: AuthenticatedApiOptions) {
  await page.route('**/v1/session', route => route.fulfill({json: fixtureIdentity(options)}));
}
