import { expect, type Page } from "@playwright/test";
import type { SchemaNode } from "../../src/api";
import { uxApi, uxAssetId } from "../ux-fixture";

export const referenceName = `contact_${"界".repeat(1024)}.literal`;
export const referenceType = "timestamp[us, tz=Europe/Amsterdam]";
const names = Array.from({ length: 24 }, (_, index) => `level_${index}`);
export const referencePath = `${names.join(".")}.[${JSON.stringify(referenceName)}]`;

export async function schemaReferenceApi(page: Page) {
  await uxApi(page);
  const segments = [...names, referenceName].map((name, index) => ({ kind: "field" as const, name, field_id: index + 1 }));
  let child: SchemaNode | undefined;
  for (let index = names.length; index >= 0; index--) {
    const name = index === names.length ? referenceName : names[index];
    child = {
      field_id: index + 1, name, human_path: index === names.length ? referencePath : names.slice(0, index + 1).join("."),
      path: { version: 1, segments: segments.slice(0, index + 1) }, nullable: false,
      type: child ? "struct" : referenceType, kind: child ? "struct" : "scalar",
      ...(child ? { children: [child] } : {}),
    };
  }
  await page.route(`**/v1/assets/${uxAssetId}/schema`, (route) => route.fulfill({ json: {
    asset_id: uxAssetId, catalog: "retail_demo", target: "retail.customer_revenue", schema_version: 1,
    schema_fingerprint: "reference-deep", stable_field_ids: true, supported_masks: ["null"], fields: [child],
  } }));
}

export async function openDeepReference(page: Page) {
  await page.goto(`/?asset=${uxAssetId}#assets`);
  await page.getByRole("button", { name: "Show schema", exact: true }).click();
  const sidebar = page.getByRole("complementary", { name: "Asset schema" });
  const viewport = sidebar.locator(".schema-reference-list");
  for (let depth = 1; depth < names.length; depth++) {
    await viewport.evaluate((element, depth) => { element.scrollTop = Math.max(0, depth * 56 - 150); }, depth);
    await sidebar.getByRole("button", { name: `Expand ${names.slice(0, depth + 1).join(".")}`, exact: true }).click();
  }
  await viewport.evaluate((element) => { element.scrollTop = element.scrollHeight; });
  await expect(sidebar.locator(".schema-reference-row").last()).toBeInViewport();
  return sidebar;
}
