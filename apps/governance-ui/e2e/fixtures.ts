/** Composable synthetic HTTP boundaries; each test gets fresh state. */
import type { Page } from "@playwright/test";
import type { AuthenticatedApiOptions } from "./support/api-types";
import { installSessionApi } from "./support/session-api";
import { installAssetApi } from "./support/asset-api";
import { installManagementApi } from "./support/management-api";

export { deferredResponse, type DeferredResponse } from "./support/async";
export { edgeChallengeApi, signedOutApi, type EdgeChallengeOptions } from "./support/session-api";

export async function authenticatedApi(page: Page, options: AuthenticatedApiOptions = {}) {
  await page.route("**/v1/**", (route) => route.fulfill({
    status: 404,
    json: { detail: "synthetic fixture route missing" },
  }));
  await installAssetApi(page, options);
  await installManagementApi(page, options);
  await installSessionApi(page, options);
}
