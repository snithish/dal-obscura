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
export async function authenticatedApi(page: Page, options: { admin?: boolean; deferredAudit?: DeferredResponse; deferredHistory?: DeferredResponse; deferredSettings?: DeferredResponse; deferredConnections?: DeferredResponse; deferredInventory?: DeferredResponse; deferredVersion?: DeferredResponse; deferredSave?: DeferredResponse; deferredEvaluate?: DeferredResponse; deferredRestore?: DeferredResponse } = {}) {
  const assetId = "00000000-0000-4000-8000-000000000001";
  const identity = {
    principal: "alex@example.invalid",
    groups: ["analysts"],
    platform_admin: Boolean(options.admin),
    capabilities: options.admin ? ["asset:read", "asset:edit", "workspace:admin"] : ["asset:read", "asset:edit"],
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
  let settingsCalls = 0;
  let catalogCalls = 0;
  await page.route("**/v1/**", async (route) => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
    if (path === "/v1/session" && request.method() === "GET") return route.fulfill({ json: identity });
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
    if (options.admin && path === "/v1/plugins") return route.fulfill({ json: { plugins: [], states: [], pairs: [] } });
    if (options.admin && path === "/v1/catalogs") {
      catalogCalls += 1;
      if (catalogCalls === 1 && options.deferredConnections) {
        options.deferredConnections.markStarted();
        await options.deferredConnections.wait();
      }
      if (!options.deferredConnections) return route.fulfill({ json: [] });
      const catalogName = catalogCalls === 1 ? "stale-catalog" : "fresh-catalog";
      return route.fulfill({ json: [{ id: `catalog-${catalogCalls}`, name: catalogName, module: "synthetic.catalog", plugin_id: null, options: {}, status: "ready", revision: catalogCalls, discovered_table_count: 0, governed_asset_count: 0 }] });
    }
    if (options.admin && path === "/v1/workspace/publications") return route.fulfill({ json: [] });
    if (options.admin && path === "/v1/settings/runtime") {
      settingsCalls += 1;
      if (settingsCalls === 1 && options.deferredSettings) {
        options.deferredSettings.markStarted();
        await options.deferredSettings.wait();
      }
      if (!options.deferredSettings) return route.fulfill({ json: null });
      return route.fulfill({ json: { ticket_ttl_seconds: settingsCalls === 1 ? 11 : 22, max_tickets: 64, max_ticket_exchanges: 2, path_rules: [{ root: "file:///fresh-settings" }], revision: settingsCalls } });
    }
    if (options.admin && path === "/v1/settings/auth-providers") return route.fulfill({ json: [] });
    if (options.admin && path === "/v1/settings/auth-providers/revision") return route.fulfill({ json: { revision: 0 } });
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
      if (suffix === "draft" && request.method() === "PUT") {
        if (options.deferredSave) {
          options.deferredSave.markStarted();
          await options.deferredSave.wait();
        }
        return route.fulfill({ json: { id: "stale-draft", asset_id: assetId, author_principal: identity.principal, revision: 1, base_policy_version: 1, rules: [], content_hash: "stale" } });
      }
      if (suffix === "policy-evaluate" && request.method() === "POST") {
        if (options.deferredEvaluate) {
          options.deferredEvaluate.markStarted();
          await options.deferredEvaluate.wait();
        }
        return route.fulfill({ json: { decision: "deny", allowed_columns: [], masks: [], row_filter: null, rows: [], input_rows: 0, output_rows: 0, evidence: {}, schema: "", status: "completed" } });
      }
      if (suffix === "draft" || suffix.startsWith("draft/")) return route.fulfill({ json: { id: "current-draft", asset_id: assetId, author_principal: identity.principal, revision: 0, base_policy_version: 1, rules: [], content_hash: "" } });
      if (suffix === "policy-versions") {
        if (!options.deferredVersion && !options.deferredRestore) return route.fulfill({ json: [] });
        return route.fulfill({ json: [
          { asset_id: assetId, asset_name: "orders", catalog: "demo", target: "demo.orders", policy_version: 1, active: false, created_at: "2026-09-20T00:00:00Z" },
          { asset_id: assetId, asset_name: "orders", catalog: "demo", target: "demo.orders", policy_version: 2, active: true, created_at: "2026-09-21T00:00:00Z" },
        ] });
      }
      const restoreMatch = suffix.match(/^policy-versions\/(\d+)\/restore$/);
      if (restoreMatch && request.method() === "POST" && options.deferredRestore) {
        options.deferredRestore.markStarted();
        await options.deferredRestore.wait();
        return route.fulfill({ json: { id: "stale-restore", asset_id: assetId, author_principal: identity.principal, revision: 2, base_policy_version: Number(restoreMatch[1]), rules: [{ ordinal: 10, effect: "allow", principals: ["group:stale-restore"], columns: ["order_id"], masks: {}, row_filter: null }], content_hash: "stale-restore" } });
      }
      const versionMatch = suffix.match(/^policy-versions\/(\d+)$/);
      if (versionMatch && options.deferredVersion) {
        const version = Number(versionMatch[1]);
        if (version === 1) {
          options.deferredVersion.markStarted();
          await options.deferredVersion.wait();
        }
        return route.fulfill({ json: {
          asset_id: assetId,
          policy_version: version,
          rules: [{ ordinal: 10, effect: "allow", principals: [version === 1 ? "group:stale" : "group:fresh"], columns: ["order_id"], masks: {}, row_filter: null }],
        } });
      }
      return route.fulfill({ json: detail });
    }
    return route.fulfill({ status: 404, json: { detail: "synthetic fixture route missing" } });
  });
}
