/** Composable synthetic HTTP boundaries; each test gets fresh state. */
import type { Page } from "@playwright/test";
import type { AuthenticatedApiOptions } from "./api-types";
import { installSessionApi } from "./session-api";
import { installAssetApi } from "./asset-api";
import { installManagementApi } from "./management-api";

export { deferredResponse, type DeferredResponse } from "./async";
export { edgeChallengeApi, signedOutApi, type EdgeChallengeOptions } from "./session-api";

export async function authenticatedApi(page: Page, options: AuthenticatedApiOptions = {}) {
  await page.route("**/v1/**", (route) => route.fulfill({
    status: 404,
    json: { detail: "synthetic fixture route missing" },
  }));
  await installAssetApi(page, options);
  await installManagementApi(page, options);
  await installSessionApi(page, options);
}
