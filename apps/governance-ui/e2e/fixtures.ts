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
export async function signedOutApi(page: Page, options: { oidc?: { authority: string; client_id: string; redirect_uri: string } } = {}) {
  await page.route("**/v1/**", async (route) => {
    const path = new URL(route.request().url()).pathname;
    if (path === "/v1/session/options") return route.fulfill({ json: { bootstrap_enabled: !options.oidc, oidc: options.oidc ?? null } });
    if (path === "/v1/ui-auth-config") return route.fulfill({ json: { authority: options.oidc?.authority ?? null } });
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
export async function authenticatedApi(page: Page, options: { admin?: boolean; readOnly?: boolean; conflictSave?: boolean; configuredSettings?: boolean; configuredConnections?: boolean; configuredLifecycle?: boolean; configuredAudit?: boolean; multipleFormats?: boolean; initialPolicyRules?: Array<Record<string, unknown>>; ruleEditor?: boolean; deferredAudit?: DeferredResponse; deferredSettings?: DeferredResponse; deferredConnections?: DeferredResponse; deferredDiscovery?: DeferredResponse; deferredInventory?: DeferredResponse; deferredSave?: DeferredResponse; deferredEvaluate?: DeferredResponse } = {}) {
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
    policy_rules: options.initialPolicyRules ?? [],
    schema_fields: schemaFields.map(({ name, type, nullable }) => ({ name, type, nullable })),
  };
  const configuredCatalog = {
    id: "catalog-analytics",
    name: "analytics",
    module: "synthetic.catalog",
    plugin_id: "synthetic.catalog.iceberg",
    options: { uri: "https://catalog.example", password: { secret: "catalog/analytics", scope: "catalog:analytics" } },
    status: "ready",
    revision: 1,
    discovered_table_count: 1,
    governed_asset_count: 0,
  };
  let pluginStates = options.configuredLifecycle ? [
    { kind: "catalog", plugin_id: "synthetic.catalog.iceberg", status: "ready", lifecycle: "enabled" },
    { kind: "table_format", plugin_id: "synthetic.table.iceberg", status: "ready", lifecycle: "enabled" },
  ] : [];
  const configuredPlugins = {
    plugins: [
      { kind: "catalog", plugin_id: "synthetic.catalog.iceberg", api_version: "1", config_version: 1, distribution: "synthetic-catalog", version: "1.0.0", display_name: "Synthetic Iceberg Catalog", capabilities: ["discover"], output_formats: ["iceberg"], handle_versions: [1], config_schema: { fields: [{ name: "uri", type: "uri", required: true }, { name: "password", type: "secret_reference", required: true, secret: true }] }, status: "admitted" },
      { kind: "table_format", plugin_id: "synthetic.table.iceberg", api_version: "1", config_version: 1, distribution: "synthetic-iceberg", version: "1.0.0", display_name: "Synthetic Iceberg", capabilities: ["scan"], output_formats: ["iceberg"], handle_versions: [1], config_schema: { fields: [] }, status: "admitted" },
      ...(options.multipleFormats ? [{ kind: "table_format", plugin_id: "synthetic.table.delta", api_version: "1", config_version: 1, distribution: "synthetic-delta", version: "1.0.0", display_name: "Synthetic Delta", capabilities: ["scan"], output_formats: ["delta"], handle_versions: [1], config_schema: { fields: [] }, status: "admitted" }] : []),
    ],
    states: pluginStates,
    pairs: [
      { catalog_plugin_id: "synthetic.catalog.iceberg", format_plugin_id: "synthetic.table.iceberg", capabilities: ["scan"], handle_versions: [1], status: "admitted" },
      ...(options.multipleFormats ? [{ catalog_plugin_id: "synthetic.catalog.iceberg", format_plugin_id: "synthetic.table.delta", capabilities: ["scan"], handle_versions: [1], status: "admitted" }] : []),
    ],
  };
  let auditCalls = 0;
  let settingsCalls = 0;
  let catalogCalls = 0;
  let discoveryCalls = 0;
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
      if (options.configuredAudit) {
        const requestUrl = new URL(request.url());
        const action = requestUrl.searchParams.get("action");
        const cursor = requestUrl.searchParams.get("cursor");
        if (action !== "asset.policy.replace") return route.fulfill({ json: { items: [], next_cursor: null } });
        const event = (eventAction: string, id: string) => ({ id, actor: "alex@example.invalid", action: eventAction, resource_type: "policy", resource_id: assetId, outcome: "success", details: {}, correlation_id: null, created_at: "2026-09-21T00:00:00Z" });
        if (!cursor) return route.fulfill({ json: { items: [event("asset.policy.replace", "filtered-policy")], next_cursor: "audit-next" } });
        return route.fulfill({ json: { items: [event("asset.tokens.revoke", "filtered-revoke")], next_cursor: null } });
      }
      auditCalls += 1;
      if (auditCalls === 1 && options.deferredAudit) {
        options.deferredAudit.markStarted();
        await options.deferredAudit.wait();
      }
      const action = auditCalls === 1 ? "stale-audit" : "fresh-audit";
      return route.fulfill({ json: { items: [{ id: `event-${auditCalls}`, actor: "alex@example.invalid", action, resource_type: "workspace", resource_id: "workspace", outcome: "success", details: {}, correlation_id: null, created_at: "2026-09-21T00:00:00Z" }], next_cursor: null } });
    }
    if (path === "/v1/workspace/summary") return route.fulfill({ json: { asset_count: 1, catalog_count: 1, enabled_auth_provider_count: 1, missing_policy_count: 0, runtime_configured: true, unowned_asset_count: 0 } });
    if (path === "/v1/workspace/observations") return route.fulfill({ json: { available: true, data_plane: { status: "ready", reason: "synthetic fixture" }, generation: null, observed_at: "2026-09-21T00:00:00Z", source: "synthetic fixture" } });
    if (options.admin && path === "/v1/plugins") return route.fulfill({ json: options.configuredConnections ? { ...configuredPlugins, states: pluginStates } : { plugins: [], states: [], pairs: [] } });
    const lifecycleMatch = path.match(/^\/v1\/plugins\/([^/]+)\/([^/]+)\/lifecycle$/);
    if (options.admin && options.configuredConnections && lifecycleMatch && request.method() === "PATCH") {
      const kind = decodeURIComponent(lifecycleMatch[1]);
      const pluginId = decodeURIComponent(lifecycleMatch[2]);
      const target = (request.postDataJSON() as { target?: string }).target;
      if (!target || !pluginStates.some((state) => state.kind === kind && state.plugin_id === pluginId)) {
        return route.fulfill({ status: 404, json: { detail: "Plugin lifecycle target not found" } });
      }
      pluginStates = pluginStates.map((state) => state.kind === kind && state.plugin_id === pluginId ? { ...state, lifecycle: target } : state);
      return route.fulfill({ json: { kind, plugin_id: pluginId, lifecycle: target } });
    }
    if (options.admin && path === "/v1/catalogs") {
      if (options.configuredConnections) return route.fulfill({ json: [configuredCatalog] });
      catalogCalls += 1;
      if (catalogCalls === 1 && options.deferredConnections) {
        options.deferredConnections.markStarted();
        await options.deferredConnections.wait();
      }
      if (!options.deferredConnections) return route.fulfill({ json: [] });
      const catalogName = catalogCalls === 1 ? "stale-catalog" : "fresh-catalog";
      return route.fulfill({ json: [{ id: `catalog-${catalogCalls}`, name: catalogName, module: "synthetic.catalog", plugin_id: null, options: {}, status: "ready", revision: catalogCalls, discovered_table_count: 0, governed_asset_count: 0 }] });
    }
    const catalogMatch = path.match(/^\/v1\/catalogs\/([^/]+)(?:\/(.*))?$/);
    if (options.admin && options.configuredConnections && catalogMatch) {
      const catalogName = decodeURIComponent(catalogMatch[1]);
      const suffix = catalogMatch[2] ?? "";
      if (request.method() === "PUT" && !suffix) return route.fulfill({ json: { ...configuredCatalog, name: catalogName, id: `catalog-${catalogName}` } });
      if (suffix === "diagnostics") return route.fulfill({ json: { catalog: catalogName, status: "ready", message: "Catalog reachable", checked_at: "2026-09-21T00:00:00Z", table_count: 1, sample_tables: ["orders"] } });
      if (suffix === "tables") {
        discoveryCalls += 1;
        if (discoveryCalls === 1 && options.deferredDiscovery) {
          options.deferredDiscovery.markStarted();
          await options.deferredDiscovery.wait();
        }
        const tableName = options.deferredDiscovery ? (discoveryCalls === 1 ? "stale-orders" : "fresh-orders") : "orders";
        return route.fulfill({ json: { catalog: catalogName, tables: [{ name: tableName, backend: "iceberg", identifier: `demo.${tableName}`, governed: false }] } });
      }
    }
    if (options.admin && path === "/v1/settings/runtime") {
      if (request.method() === "PUT") return route.fulfill({ json: { ticket_ttl_seconds: 30, max_tickets: 64, max_ticket_exchanges: 2, path_rules: [{ root: "file:///fresh-settings" }], revision: 2 } });
      settingsCalls += 1;
      if (settingsCalls === 1 && options.deferredSettings) {
        options.deferredSettings.markStarted();
        await options.deferredSettings.wait();
      }
      if (!options.deferredSettings && !options.configuredSettings) return route.fulfill({ json: null });
      return route.fulfill({ json: { ticket_ttl_seconds: settingsCalls === 1 ? 11 : 22, max_tickets: 64, max_ticket_exchanges: 2, path_rules: [{ root: "file:///fresh-settings" }], revision: settingsCalls } });
    }
    if (options.admin && path === "/v1/settings/auth-providers") return route.fulfill({ json: [] });
    if (options.admin && path === "/v1/settings/auth-providers/revision") return route.fulfill({ json: { revision: 0 } });
    if (options.admin && path === "/v1/assets/analytics/orders" && request.method() === "PUT") {
      return route.fulfill({ json: { id: "governed-orders", catalog: "analytics", name: "orders", backend: "synthetic.table.iceberg", table_identifier: "demo.orders", owners: [], schema_fields: [] } });
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
    return route.fulfill({ status: 404, json: { detail: "synthetic fixture route missing" } });
  });
}
