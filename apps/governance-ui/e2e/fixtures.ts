import type { Page } from "@playwright/test";

export type DeferredResponse = {
  started: Promise<void>;
  release: () => void;
  wait: () => Promise<void>;
  markStarted: () => void;
};

export function deferredResponse(): DeferredResponse {
  let markStarted!: () => void;
  let release!: () => void;
  const started = new Promise<void>((resolve) => { markStarted = resolve; });
  const released = new Promise<void>((resolve) => { release = resolve; });
  return { started, release, wait: () => released, markStarted };
}

/** Synthetic API boundary for component tests, never evidence of live SSO. */
export async function signedOutApi(page: Page) {
  await page.route("**/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/v1/session/options") return route.fulfill({ json: { bootstrap_enabled: true, oidc: null } });
    if (path === "/v1/ui-auth-config") return route.fulfill({ json: { authority: null } });
    return route.fulfill({ status: 401, json: { detail: "Sign in required" } });
  });
}

export type EdgeChallengeOptions = { status?: number; redirect?: boolean };

export async function edgeChallengeApi(page: Page, options: EdgeChallengeOptions = {}) {
  await page.unroute("**/v1/**");
  await page.route("**/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/v1/session" && route.request().method() === "GET") {
      if (options.redirect) return route.fulfill({ status: 302, headers: { location: "/v1/session/challenge" } });
      return route.fulfill({ status: options.status ?? 200, headers: { "content-type": "text/html" }, body: "<html><title>Sign in</title></html>" });
    }
    if (path === "/v1/session/challenge") return route.fulfill({ status: 200, headers: { "content-type": "text/html" }, body: "<html><title>Sign in</title></html>" });
    if (path === "/v1/session/options") return route.fulfill({ json: { bootstrap_enabled: false, oidc: null } });
    if (path === "/v1/ui-auth-config") return route.fulfill({ json: { authority: null } });
    return route.fulfill({ status: 401, json: { detail: "Sign in required" } });
  });
}

/** Synthetic authenticated API boundary for shell integration checks.
 * This fixture proves browser composition and capability presentation only;
 * it is never evidence of a live identity provider or production backend.
 */
export async function authenticatedApi(page: Page, options: { deferredAudit?: DeferredResponse; deferredHistory?: DeferredResponse } = {}) {
  const assetId = "00000000-0000-4000-8000-000000000001";
  const identity = {
    principal: "alex@example.invalid",
    groups: ["analysts"],
    platform_admin: false,
    capabilities: ["asset:read", "asset:edit"],
  };
  const inventory = {
    id: assetId,
    catalog: "demo",
    name: "orders",
    backend: "iceberg",
    table_identifier: "demo.orders",
    owner_count: 1,
    owners: ["alex@example.invalid"],
    policy_status: "configured",
    draft_status: "published",
    active_policy_version: 1,
    last_published_at: "2026-09-21T00:00:00Z",
  };
  const detail = {
    ...inventory,
    revision: 1,
    options: {},
    policy_rules: [],
    schema_fields: [{ name: "order_id", type: "string", nullable: false }],
  };
  let auditCalls = 0;
  let historyCalls = 0;
  await page.route("**/v1/**", async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === "/v1/session" && request.method() === "GET") return route.fulfill({ json: identity });
    if (path === "/v1/assets/page") return route.fulfill({ json: { items: [inventory], next_cursor: null } });
    if (path === "/v1/audit/events/page") {
      auditCalls += 1;
      if (auditCalls === 1 && options.deferredAudit) {
        options.deferredAudit.markStarted();
        await options.deferredAudit.wait();
      }
      const action = auditCalls === 1 ? "stale-audit" : "fresh-audit";
      return route.fulfill({ json: { items: [{ id: `event-${auditCalls}`, actor: "alex@example.invalid", action, resource_type: "workspace", resource_id: "workspace", outcome: "success", details: {}, correlation_id: null, created_at: "2026-09-21T00:00:00Z" }], next_cursor: null } });
    }
    if (path === "/v1/policy-versions/page") {
      historyCalls += 1;
      if (historyCalls === 1 && options.deferredHistory) {
        options.deferredHistory.markStarted();
        await options.deferredHistory.wait();
      }
      if (!options.deferredHistory) return route.fulfill({ json: { items: [], next_cursor: null } });
      const assetName = historyCalls === 1 ? "stale-history" : "fresh-history";
      return route.fulfill({ json: { items: [{ asset_id: assetId, asset_name: assetName, catalog: "demo", target: "demo.orders", policy_version: historyCalls, active: true, created_at: "2026-09-21T00:00:00Z" }], next_cursor: null } });
    }
    if (path === "/v1/workspace/summary") return route.fulfill({ json: { asset_count: 1, catalog_count: 1, draft_change_count: 0, enabled_auth_provider_count: 1, missing_policy_count: 0, runtime_configured: true, unowned_asset_count: 0 } });
    if (path === "/v1/workspace/observations") return route.fulfill({ json: { available: true, data_plane: { status: "ready", reason: "synthetic fixture" }, generation: null, observed_at: "2026-09-21T00:00:00Z", source: "synthetic fixture" } });
    const match = path.match(/^\/v1\/assets\/([^/]+)(?:\/(.*))?$/);
    if (match && match[1] === assetId) {
      const suffix = match[2] ?? "";
      if (suffix === "schema") return route.fulfill({ json: {
        asset_id: assetId,
        catalog: "demo",
        target: "demo.orders",
        schema_version: 1,
        schema_fingerprint: "orders-schema",
        stable_field_ids: true,
        supported_masks: ["null", "redact", "hash", "email", "keep_last", "default"],
        fields: [{ field_id: 1, name: "order_id", human_path: "order_id", type: "string", nullable: false, kind: "scalar", path: { version: 1, segments: [{ kind: "field", name: "order_id", field_id: 1 }] } }],
      } });
      if (suffix === "grants") return route.fulfill({ json: [] });
      if (suffix === "access") return route.fulfill({ json: { asset_id: assetId, principal: identity.principal, issuer: null, capabilities: [
        { capability: "read", allowed: true, reasons: ["owner"] },
        { capability: "edit", allowed: true, reasons: ["owner"] },
        { capability: "publish", allowed: false, reasons: [] },
        { capability: "grant", allowed: false, reasons: [] },
      ] } });
      if (suffix === "draft" || suffix.startsWith("draft/")) return route.fulfill({ json: { id: "current-draft", asset_id: assetId, author_principal: identity.principal, revision: 0, base_policy_version: 1, rules: [], content_hash: "" } });
      if (suffix === "policy-versions") return route.fulfill({ json: [] });
      return route.fulfill({ json: detail });
    }
    return route.fulfill({ status: 404, json: { detail: "synthetic fixture route missing" } });
  });
}
