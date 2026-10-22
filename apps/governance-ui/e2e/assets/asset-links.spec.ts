import { expect, test } from "@playwright/test";
import { authenticatedApi } from "../support/fixtures";

test("authorized deep links load independently of the first inventory page", async ({ page }) => {
  await authenticatedApi(page);
  await page.route("**/v1/assets/page**", (route) =>
    route.fulfill({ json: { items: [], next_cursor: null } }),
  );
  await page.goto("/?asset=00000000-0000-4000-8000-000000000001#assets");
  await expect(page.getByRole("heading", { name: "orders" })).toBeVisible();
});
