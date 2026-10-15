import { mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { expect, test } from "@playwright/test";
import { authenticatedApi } from "../e2e/support/fixtures";

const phase = process.env.DAL_OBSCURA_REVIEW_PHASE ?? "after";
const output = resolve("../../docs/ui-review/secret-backend");

test("startup secret backend has a distinct plugin label", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 1000 });
  await authenticatedApi(page, { admin: true, configuredConnections: true });
  await page.route("**/v1/plugins", (route) =>
    route.fulfill({
      json: {
        plugins: [
          {
            kind: "secret_backend",
            plugin_id: "environment",
            api_version: "3",
            config_version: 1,
            distribution: "dal-obscura-secrets",
            version: "0.3.0",
            display_name: "Environment secrets",
            capabilities: [],
            output_formats: [],
            handle_versions: [],
            config_schema: { fields: [] },
            status: "admitted",
          },
        ],
        states: [],
        pairs: [],
      },
    }),
  );
  await page.goto("/#connections");
  const card = page.locator(".plugin-card").filter({ hasText: "Environment secrets" });
  await expect(card).toContainText(phase === "before" ? "Table format" : "Secret backend");
  await page.evaluate(() => document.fonts.ready);
  mkdirSync(output, { recursive: true });
  await card.screenshot({ path: resolve(output, `${phase}.png`) });
});
