import type { Page } from "@playwright/test";

/** Synthetic API boundary for component tests, never evidence of live SSO. */
export async function signedOutApi(page: Page) {
  await page.route("**/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/v1/session/options") return route.fulfill({ json: { bootstrap_enabled: true, oidc: null } });
    if (path === "/v1/ui-auth-config") return route.fulfill({ json: { authority: null } });
    return route.fulfill({ status: 401, json: { detail: "Sign in required" } });
  });
}
