import { test, expect } from "@playwright/test";
import { mkdir } from "node:fs/promises";
import { resolve } from "node:path";
import { assetId, identityApi } from "../e2e/identity-fixture";

test("capture previous and current attribute authoring screens", async ({ browser }) => {
  const output = resolve("../../docs/ui-review/identity");
  await mkdir(output, { recursive: true });
  for (const [state, origin] of [["before", "http://127.0.0.1:4174"], ["after", "http://127.0.0.1:4173"]] as const) {
    const page = await browser.newPage({ viewport: { width: 1440, height: 1400 } });
    await identityApi(page);
    await page.route("**/policy-evaluate", (route) => route.fulfill({ json: {
      status: "completed", decision: "allow", allowed_columns: ["email"], masks: [], row_filter: null,
      rows: [], input_rows: 0, output_rows: 0, schema: "", policy_revision: 1,
      evidence: { identity: { principal: "alice", groups: ["analysts"], attributes: { department: "Finance" } },
        conditions: [{ rule_ordinal: 1, key: "department", expected: ["Engineering", "Finance"], actual: "Finance", matched: true, missing: false }] },
    } }));
    await page.goto(`${origin}/?asset=${assetId}#assets`);
    await page.getByText(state === "before" ? "Identity claim conditions" : "Identity attribute conditions (1)", { exact: true }).click();
    await expect(page.locator(".audience-conditions")).toBeVisible();
    await page.evaluate(() => document.fonts.ready);
    await page.locator(".audience-conditions").screenshot({ path: resolve(output, `${state}-conditions.png`) });
    await page.getByRole("button", { name: "Test Policy", exact: true }).click();
    const modal = page.getByRole("dialog", { name: "Test policy", exact: true });
    if (state === "after") {
      await modal.getByRole("combobox", { name: "Identity input", exact: true }).click();
      await page.getByRole("option", { name: /Provider 1/ }).click();
    } else await modal.getByLabel("Principal", { exact: true }).fill("alice");
    await modal.getByLabel("Synthetic persona claims").fill(state === "after" ? '{"sub":"alice","groups":["analysts"],"employee":{"department":"Finance"}}' : '{"department":"Finance"}');
    await modal.getByRole("button", { name: "Test saved policy", exact: true }).click();
    await expect(modal.getByText(state === "before" ? "Current test: 1 fields evaluated" : "Current test: 1 field evaluated")).toBeVisible();
    await modal.screenshot({ path: resolve(output, `${state}-policy-test.png`) });
    await modal.getByRole("button", { name: "Close policy test" }).click();
    await page.goto(`${origin}/#settings`);
    await expect(page.getByRole("heading", { name: "Authentication providers" })).toBeVisible();
    await (state === "after" ? page.locator(".attribute-mappings") : page.locator(".form-card").filter({ has: page.getByRole("heading", { name: "Authentication providers", exact: true }) })).screenshot({ path: resolve(output, `${state}-mappings.png`) });
    await page.close();
  }
});
