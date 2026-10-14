import type { Page } from "@playwright/test";
import { authenticatedApi } from "./fixtures";
import { fixtureAssetId } from "./asset-api";

export async function wideSelectionApi(page: Page, count = 10_000, masked = false) {
  const names = Array.from({ length: count }, (_, index) => `field_${index}`);
  await authenticatedApi(page, { initialPolicyRules: [{ ordinal: 10, effect: "allow", principals: ["analyst"], groups: [], columns: names, masks: masked ? Object.fromEntries(names.map(name => [name, { type: "hash" as const }])) : {}, row_filter: null, when: {} }] });
  await page.route(`**/v1/assets/${fixtureAssetId}/schema`, route => route.fulfill({ json: {
    asset_id: fixtureAssetId, catalog: "demo", target: "demo.orders", schema_version: 1,
    schema_fingerprint: "wide-selected-schema", stable_field_ids: true,
    supported_masks: ["null", "redact", "hash", "email", "keep_last", "default"],
    fields: names.map((name, index) => ({ field_id: index + 1, name, human_path: name, type: "string", nullable: true, kind: "scalar", path: { version: 1, segments: [{ kind: "field", name, field_id: index + 1 }] } })),
  } }));
  await page.goto(`/?asset=${fixtureAssetId}#assets`);
  await page.getByRole("button", { name: `Allowed columns: ${count} selected`, exact: true }).waitFor();
}
