import type { Page } from '@playwright/test';
import type { AuthenticatedApiOptions } from './api-types';
import { fixtureAssetId } from './asset-api';

export async function installManagementApi(page: Page, options: AuthenticatedApiOptions) {
  const assetId = fixtureAssetId;
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
      { kind: "catalog", plugin_id: "synthetic.catalog.iceberg", api_version: "2", config_version: 1, distribution: "synthetic-catalog", version: "1.0.0", display_name: "Synthetic Iceberg Catalog", capabilities: ["discover"], output_formats: ["iceberg"], handle_versions: [1], config_schema: { fields: [{ name: "uri", type: "uri", required: true }, { name: "password", type: "secret_reference", required: true, secret: true }] }, status: "admitted" },
      { kind: "table_format", plugin_id: "synthetic.table.iceberg", api_version: "2", config_version: 1, distribution: "synthetic-iceberg", version: "1.0.0", display_name: "Synthetic Iceberg", capabilities: ["scan"], output_formats: ["iceberg"], handle_versions: [1], config_schema: { fields: [] }, status: "admitted" },
      ...(options.multipleFormats ? [{ kind: "table_format", plugin_id: "synthetic.table.delta", api_version: "2", config_version: 1, distribution: "synthetic-delta", version: "1.0.0", display_name: "Synthetic Delta", capabilities: ["scan"], output_formats: ["delta"], handle_versions: [1], config_schema: { fields: [] }, status: "admitted" }] : []),
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
  await page.route('**/v1/**', async route => {
    const request = route.request();
    const path = new URL(request.url()).pathname;
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
    return route.fallback();
  });
}
