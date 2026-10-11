import { mkdir } from "node:fs/promises";
import { expect, test } from "@playwright/test";
import { wideSelectionApi } from "../e2e/support/wide-selection";

const stage = process.env.DAL_MASK_REVIEW_STAGE ?? "after";
if (stage !== "before" && stage !== "after") throw new Error("Invalid review stage");
for (const width of [1440, 390]) {
  test(`mask summary ${stage} ${width}`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await wideSelectionApi(page, 1000, true);
    const card = page.locator(".mask-card").first();
    await expect(card.getByRole("button", { name: "Edit Hash mask", exact: true })).toBeVisible();
    await page.evaluate(() => document.fonts.ready);
    await card.evaluate(element => window.scrollTo({ top: window.scrollY + element.getBoundingClientRect().top - 220 }));
    const directory = "../../docs/ui-review/mask-summary";
    await mkdir(directory, { recursive: true });
    await page.screenshot({ path: `${directory}/${stage}-${width}.png` });
    if (stage === "after") {
      await card.locator("summary").click();
      await expect(card.getByRole("searchbox", { name: "Search masked columns" })).toBeVisible();
      await page.screenshot({ path: `${directory}/review-${width}.png` });
    }
  });
}
