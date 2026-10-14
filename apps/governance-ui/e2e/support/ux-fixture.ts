import { Page } from "@playwright/test";
import { authenticatedApi } from "./fixtures";

export const uxAssetId = "00000000-0000-4000-8000-000000000001";
export const uxRule = { ordinal: 10, name: "Regional analysts", description: "Customer reporting for the European analytics team.", effect: "allow", principals: ["group:analysts", "group:finance"], columns: ["customer.email", "country", "revenue"], masks: {}, row_filter: null, when: {} };
const field = (name: string, field_id: number, type = "string") => ({ field_id, name, human_path: name, type, nullable: true, kind: "scalar", path: { version: 1, segments: [{ kind: "field", name, field_id }] } });
const customer = field("customer", 1, "struct");
export const uxFields = [
  { ...customer, kind: "struct", children: ["id", "name", "email", "phone"].map((name, index) => ({ ...field(name, index + 2), human_path: `customer.${name}`, path: { version: 1, segments: [...customer.path.segments, { kind: "field", name, field_id: index + 2 }] } })) },
  field("country", 6), field("region", 7), field("revenue", 8, "decimal(18, 2)"), field("order_count", 9, "int64"), field("last_order_at", 10, "timestamp"), field("marketing_opt_in", 11, "bool"), field("internal_notes", 12),
];
const inventory = { id: uxAssetId, catalog: "retail_demo", name: "retail.customer_revenue", backend: "iceberg", table_identifier: "retail.customer_revenue", owner_count: 1, owners: ["http://localhost:20080/realms/dal-obscura-demo|group:asset-owners"], policy_status: "configured", policy_revision: 1, revision: 1 };

/** Representative synthetic data, shared by behavioral tests and visual evidence. */
export async function uxApi(page: Page) {
  await authenticatedApi(page, { ruleEditor: true, initialPolicyRules: [uxRule] });
  await page.route("**/v1/assets/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (route.request().method() !== "GET") return route.fallback();
    if (path === "/v1/assets/page") return route.fulfill({ json: { items: [inventory], next_cursor: null } });
    if (path === `/v1/assets/${uxAssetId}`) return route.fulfill({ json: { ...inventory, options: {}, policy_rules: [uxRule], schema_fields: uxFields.map(({ name, type, nullable }) => ({ name, type, nullable })) } });
    if (path === `/v1/assets/${uxAssetId}/schema`) return route.fulfill({ json: { asset_id: uxAssetId, catalog: inventory.catalog, target: inventory.name, schema_version: 1, schema_fingerprint: "ux-demo", stable_field_ids: true, supported_masks: ["null", "hash", "email", "redact", "keep_last", "default"], fields: uxFields } });
    return route.fallback();
  });
}
