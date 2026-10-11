import { mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { expect, test } from "@playwright/test";
import { wideSelectionApi } from "../e2e/support/wide-selection";
const phase = process.env.DAL_OBSCURA_REVIEW_PHASE ?? "after";
if (phase !== "before" && phase !== "after") throw new Error("Invalid review phase");
const output = resolve("../../docs/ui-review/wide-selection");
for (const width of [1440, 390]) {
  test(`wide selection ${phase} at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 1100 });
    await wideSelectionApi(page, 1000);
    await page.getByRole("button", { name: "Allowed columns: 1000 selected", exact: true }).click();
    if (phase === "after" && width === 390) await page.getByRole("button", { name: "Review selection", exact: true }).click();
    await expect(page.getByRole("dialog", { name: "Choose allowed columns" })).toHaveCSS("opacity", "1");
    await page.evaluate(() => document.fonts.ready);
    mkdirSync(output, { recursive: true });
    await page.screenshot({ path: resolve(output, `${phase}-${width}.png`) });
  });
}
