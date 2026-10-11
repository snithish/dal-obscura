import { mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { test } from "@playwright/test";
import { openDeepReference, referencePath, schemaReferenceApi } from "../e2e/support/schema-reference";

const phase = process.env.DAL_OBSCURA_REVIEW_PHASE ?? "after";
if (phase !== "before" && phase !== "after") throw new Error("Invalid review phase");
const output = resolve("../../docs/ui-review/schema-reference");
for (const width of [1440, 390]) {
  test(`schema reference ${phase} at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 1100 });
    await schemaReferenceApi(page);
    const sidebar = await openDeepReference(page);
    if (phase === "after") await sidebar.getByRole("button", { name: `Inspect ${referencePath}`, exact: true }).click();
    await sidebar.scrollIntoViewIfNeeded();
    await page.evaluate(() => document.fonts.ready);
    mkdirSync(output, { recursive: true });
    await page.screenshot({ path: resolve(output, `${phase}-${width}.png`) });
  });
}
