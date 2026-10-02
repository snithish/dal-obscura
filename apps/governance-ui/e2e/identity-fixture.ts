import { Page } from "@playwright/test";
import { authenticatedApi } from "./fixtures";

export const assetId = "00000000-0000-4000-8000-000000000001";
const issuer = "https://sso.example.com/realms/company";
export const attributes = [
  { key: "department", label: "Department", description: "Employee business unit", claim_path: "employee.department", allowed_values: ["Engineering", "Finance", "Research, EU"] },
  { key: "region", label: "Region", description: "Operating region", claim_path: "employee.region", allowed_values: ["US", "EU"] },
];
const providerArgs = { issuer, subject_claim: "sub", group_claims: ["groups"], attribute_claims: Object.fromEntries(attributes.map((item) => [item.key, item.claim_path])), attribute_definitions: Object.fromEntries(attributes.map((item) => [item.key, { label: item.label, description: item.description, allowed_values: item.allowed_values }])) };
export const provider = { id: "provider-1", ordinal: 1, module: "dal_obscura.identity.oidc.OidcJwksIdentityProvider", args: providerArgs, enabled: true, revision: 1 };
const rules = [{ ordinal: 1, name: "Department analysts", effect: "allow", principals: ["*"], columns: ["email"], masks: {}, when: { department: ["Engineering", "Finance"] }, row_filter: null }];

export async function identityApi(page: Page) {
  await authenticatedApi(page, { admin: true, configuredSettings: true, ruleEditor: true, initialPolicyRules: rules });
  await page.route("**/identity-attributes", (route) => route.fulfill({ json: [{ ordinal: 1, issuer, revision: 1, attributes }] }));
  await page.route("**/v1/settings/auth-providers", (route) => route.fulfill({ json: [provider] }));
  await page.route("**/v1/settings/auth-providers/revision", (route) => route.fulfill({ json: { revision: 1 } }));
}
