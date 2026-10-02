import type { Page } from '@playwright/test';
import type { AuthenticatedApiOptions } from './api-types';
import { fixtureIdentity } from './session-api';
export const fixtureAssetId = '00000000-0000-4000-8000-000000000001';

export async function installAssetApi(page: Page, options: AuthenticatedApiOptions) {
  const assetId = fixtureAssetId;
  const identity = fixtureIdentity(options);
  const inventory = {
    id: assetId,
    catalog: "demo",
    name: "orders",
    backend: "iceberg",
    table_identifier: "demo.orders",
    owner_count: 1,
    owners: ["alex@example.invalid"],
    policy_status: "configured",
  };
  const schemaFields = options.ruleEditor ? [
    { field_id: 1, name: "email", human_path: "email", type: "string", nullable: false, kind: "scalar", path: { version: 1, segments: [{ kind: "field", name: "email", field_id: 1 }] } },
    { field_id: 2, name: "phone", human_path: "phone", type: "string", nullable: false, kind: "scalar", path: { version: 1, segments: [{ kind: "field", name: "phone", field_id: 2 }] } },
    { field_id: 3, name: "country", human_path: "country", type: "string", nullable: false, kind: "scalar", path: { version: 1, segments: [{ kind: "field", name: "country", field_id: 3 }] } },
  ] : [{ field_id: 1, name: "order_id", human_path: "order_id", type: "string", nullable: false, kind: "scalar", path: { version: 1, segments: [{ kind: "field", name: "order_id", field_id: 1 }] } }];
  const detail = {
    ...inventory,
    revision: 1,
    policy_revision: 1,
    options: {},
    policy_rules: structuredClone(options.initialPolicyRules ?? []),
    schema_fields: schemaFields.map(({ name, type, nullable }) => ({ name, type, nullable })),
  };
  await page.route('**/v1/**', async route => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === "/v1/assets/page") {
      const search = new URL(request.url()).searchParams.get("search");
      if (search === "stale" && options.deferredInventory) {
        options.deferredInventory.markStarted();
        await options.deferredInventory.wait();
      }
      if (search === "stale") return route.fulfill({ json: { items: [{ ...inventory, name: "stale-orders" }], next_cursor: null } });
      if (search === "fresh") return route.fulfill({ json: { items: [{ ...inventory, name: "fresh-orders" }], next_cursor: null } });
      return route.fulfill({ json: { items: [inventory], next_cursor: null } });
    }
    const match = path.match(/^\/v1\/assets\/([^/]+)(?:\/(.*))?$/);
    if (match && match[1] === assetId) {
      const suffix = match[2] ?? "";
      if (suffix === "identity-attributes") return route.fulfill({ json: [] });
      if (suffix === "schema") return route.fulfill({ json: {
        asset_id: assetId,
        catalog: "demo",
        target: "demo.orders",
        schema_version: 1,
        schema_fingerprint: "orders-schema",
        stable_field_ids: true,
        supported_masks: ["null", "redact", "hash", "email", "keep_last", "default"],
        fields: schemaFields,
      } });
      if (suffix === "grants" && request.method() === "PUT") {
        const payload = request.postDataJSON() as { grants?: unknown[] };
        return route.fulfill({ json: { asset_id: assetId, grants: payload.grants ?? [] } });
      }
      if (suffix === "grants") return route.fulfill({ json: [] });
      if (suffix === "access") return route.fulfill({ json: { asset_id: assetId, principal: identity.principal, issuer: null, can_revoke_tokens: true, capabilities: [
        { capability: "read", allowed: true, reasons: ["owner"] },
    { capability: "edit", allowed: !options.readOnly, reasons: options.readOnly ? ["Read-only fixture"] : ["Asset owner"] },
        { capability: "grant", allowed: false, reasons: [] },
      ] } });
      if (suffix === "owners" && request.method() === "PUT") {
        const payload = request.postDataJSON() as { owners?: string[] };
        return route.fulfill({ json: { asset_id: assetId, owners: payload.owners ?? [] } });
      }
      if (suffix === "policy" && request.method() === "PUT") {
        if (options.deferredSave) {
          options.deferredSave.markStarted();
          await options.deferredSave.wait();
        }
        const payload = request.postDataJSON() as { expected_revision: number; revoke_existing_tokens?: boolean; rules?: Array<Record<string, unknown>> };
        if (options.conflictSave) return route.fulfill({ status: 409, json: { detail: "Live policy changed; reload before saving." } });
        if (payload.expected_revision !== detail.policy_revision) return route.fulfill({ status: 409, json: { detail: "Live policy changed; reload before saving." } });
        // The repository returns rules in ordinal order, independently of the
        // request array order. Match that persistence contract in UI tests.
        detail.policy_rules = [...(payload.rules ?? [])].sort((a, b) => Number(a.ordinal) - Number(b.ordinal));
        detail.policy_revision += 1;
        const revokedTokenCount = payload.revoke_existing_tokens ? 2 : 0;
        return route.fulfill({ json: { asset_id: assetId, policy_revision: detail.policy_revision, revoked_token_count: revokedTokenCount } });
      }
      if (suffix === "tickets/revoke" && request.method() === "POST") return route.fulfill({ json: { asset_id: assetId, revoked_token_count: 2 } });
      if (suffix === "policy-evaluate" && request.method() === "POST") {
        if (options.deferredEvaluate) {
          options.deferredEvaluate.markStarted();
          await options.deferredEvaluate.wait();
        }
        return route.fulfill({ json: { decision: "deny", allowed_columns: [], masks: [], row_filter: null, rows: [], input_rows: 0, output_rows: 0, evidence: {}, schema: "", status: "completed" } });
      }
      return route.fulfill({ json: detail });
    }
    return route.fallback();
  });
}
