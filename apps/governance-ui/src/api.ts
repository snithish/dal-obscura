import type { components } from "./generated/control_plane";

type ApiSchemas = components["schemas"];

export type Asset = {
  id: string;
  revision?: number;
  policy_revision?: number;
  catalog: string;
  name: string;
  backend: string;
  table_identifier: string;
  owners: string[];
  schema_fields: SchemaField[];
  schema?: AssetSchema;
  /** Inventory state supplied by the workspace asset list. */
  policy_status?: "configured" | "missing" | string;
  policy_rules?: PolicyRule[];
};

export type AssetPage = {
  items: Asset[];
  next_cursor: string | null;
};

export type SchemaField = {
  name: string;
  type: string;
  nullable: boolean;
};

export type SchemaPath = {
  version: number;
  segments: Array<{
    kind: "field" | "list_element" | "map_key" | "map_value";
    name?: string;
    field_id?: number;
  }>;
};

export type SchemaNode = {
  field_id: number;
  name: string;
  path: SchemaPath;
  human_path: string;
  type: string;
  nullable: boolean;
  kind: "scalar" | "struct" | "list" | "map";
  children?: SchemaNode[];
};

export type AssetSchema = {
  asset_id: string;
  catalog: string;
  target: string;
  schema_version: number;
  schema_fingerprint: string;
  stable_field_ids?: boolean;
  supported_masks: Array<Mask["type"]>;
  fields: SchemaNode[];
};

export type Mask = {
  type: "null" | "redact" | "hash" | "email" | "keep_last" | "default";
  value?: string | number | boolean | null;
  exempt_principals?: string[];
};

export type PolicyRule = {
  ordinal: number;
  principals: string[];
  columns: string[];
  masks: Record<string, Mask>;
  row_filter: string | null;
  effect: "allow" | "allow_all";
  name?: string;
  description?: string;
  when?: Record<string, string | string[]>;
};

export type Preview = {
  decision?: "allow" | "deny";
  allowed_columns: string[];
  masks: Record<string, Mask>;
  row_filter: string | null;
  policy_revision: number;
  status?: string;
  rows?: Array<Record<string, unknown>>;
  output_rows?: number;
  evidence?: Record<string, unknown>;
};

export type Session = {
  principal: string;
  groups: string[];
  platform_admin: boolean;
  capabilities: string[];
  issuer?: string | null;
};

export type UiAuthConfig = {
  authority?: string | null;
  client_id?: string | null;
  redirect_uri?: string | null;
};

export type SessionOptions = {
  bootstrap_enabled: boolean;
  oidc: UiAuthConfig | null;
};

export type AuditEvent = {
  id: string;
  actor: string;
  action: string;
  resource_type: string;
  resource_id: string;
  outcome: string;
  details: Record<string, unknown>;
  correlation_id: string | null;
  created_at: string;
};

export type AuditEventPage = {
  items: AuditEvent[];
  next_cursor: string | null;
};

export type Catalog = {
  id: string;
  name: string;
  plugin_id: string;
  options: Record<string, unknown>;
  status?: string;
  revision?: number;
  discovered_table_count?: number;
  governed_asset_count?: number;
};

export type CatalogDiagnostic = {
  catalog: string;
  status: "ready" | "unavailable";
  message: string;
  checked_at: string;
  table_count?: number;
  sample_tables?: string[];
};

export type AssetGrant = {
  principal: string;
  capability: "read" | "edit" | "grant";
};

export type AssetCapability = {
  capability: AssetGrant["capability"];
  allowed: boolean;
  reasons: string[];
};

export type AssetAccess = {
  asset_id: string;
  principal: string;
  issuer: string | null;
  can_revoke_tokens: boolean;
  capabilities: AssetCapability[];
};

export type RuntimeSettings = {
  ticket_ttl_seconds: number;
  max_tickets: number;
  max_ticket_exchanges: number;
  path_rules: Array<Record<string, string>>;
  revision?: number;
};

export type PluginDescriptor = {
  kind: "catalog" | "table_format";
  plugin_id: string;
  api_version: string;
  config_version: number;
  distribution: string;
  version: string;
  display_name: string;
  capabilities: string[];
  output_formats: string[];
  handle_versions: number[];
  config_schema: Record<string, unknown>;
  status: "admitted" | "incompatible";
};

export type PluginState = {
  kind: "catalog" | "table_format";
  plugin_id: string;
  status: "enabled" | "not_installed" | "incompatible";
  lifecycle?: "enabled" | "draining" | "disabled" | "revoked" | "removed";
  reason?: string;
};

export type PluginPair = {
  catalog_plugin_id: string;
  format_plugin_id: string;
  capabilities: string[];
  handle_versions: number[];
  status: "admitted" | "incompatible";
};

export type AuthProvider = {
  id: string;
  ordinal: number;
  module: string;
  args: Record<string, unknown>;
  enabled: boolean;
  revision: number;
};

export type WorkspaceSummary = {
  catalog_count: number;
  asset_count: number;
  unowned_asset_count: number;
  missing_policy_count: number;
  runtime_configured: boolean;
  enabled_auth_provider_count: number;
};

export type WorkspaceObservations = {
  available: boolean;
  observed_at: string;
  source: string;
  data_plane: { status: string; reason: string };
};

export type ApiFailure = Error & {
  status?: number;
  code?: string;
  requestId?: string;
  currentRevision?: number;
  fieldErrors?: ApiFieldError[];
};

export type ApiFieldError = {
  field: string;
  message: string;
  type: string;
};

export function assetPath(assetId: string): string {
  return `/v1/assets/${encodeURIComponent(assetId)}`;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const csrf = readCookie("dal_obscura_csrf");
  const headers = new Headers(init?.headers);
  const method = init?.method?.toUpperCase();
  if (method && !["GET", "HEAD", "OPTIONS"].includes(method) && csrf) {
    headers.set("x-csrf-token", csrf);
  }
  if (init?.body && !headers.has("content-type")) headers.set("content-type", "application/json");
  const response = await fetch(path, {
    ...init,
    credentials: "same-origin",
    headers,
  });
  const contentType = response.headers.get("content-type")?.toLowerCase() ?? "";
  const isHtmlChallenge = response.redirected || contentType.includes("text/html") || response.url.includes("/auth/login");
  if (isHtmlChallenge) {
    window.dispatchEvent(new CustomEvent("dal-obscura-auth-expired", { detail: { code: "auth_challenge" } }));
    const failure = new Error("Authentication challenge returned instead of API JSON") as ApiFailure;
    failure.status = 401;
    failure.code = "auth_challenge";
    const requestId = response.headers.get("x-request-id") ?? undefined;
    if (requestId) failure.requestId = requestId;
    throw failure;
  }
  if (!response.ok) {
    if (response.status === 401) {
      window.dispatchEvent(new Event("dal-obscura-auth-expired"));
    }
    let body: unknown;
    try {
      body = await response.clone().json();
    } catch {
      body = undefined;
    }
    const envelope = body && typeof body === "object" && !Array.isArray(body)
      ? (body as Record<string, unknown>)
      : {};
    const errorEnvelope = envelope.error && typeof envelope.error === "object" && !Array.isArray(envelope.error)
      ? (envelope.error as Record<string, unknown>)
      : {};
    const detail = typeof errorEnvelope.message === "string"
      ? errorEnvelope.message
      : typeof envelope.detail === "string" ? envelope.detail : `Request failed (${response.status})`;
    const failure = new Error(detail) as ApiFailure;
    failure.status = response.status;
    if (typeof errorEnvelope.code === "string") failure.code = errorEnvelope.code;
    const requestId = typeof errorEnvelope.request_id === "string"
      ? errorEnvelope.request_id
      : response.headers.get("x-request-id") ?? undefined;
    if (requestId) failure.requestId = requestId;
    if (typeof errorEnvelope.current_revision === "number") failure.currentRevision = errorEnvelope.current_revision;
    if (Array.isArray(errorEnvelope.field_errors)) {
      failure.fieldErrors = errorEnvelope.field_errors.flatMap((item): ApiFieldError[] => {
        if (!item || typeof item !== "object" || Array.isArray(item)) return [];
        const value = item as Record<string, unknown>;
        if (typeof value.field !== "string" || typeof value.message !== "string" || typeof value.type !== "string") return [];
        return [{ field: value.field, message: value.message, type: value.type }];
      });
    }
    throw failure;
  }
  return response.json() as Promise<T>;
}

export const controlPlane = {
  startLogin: () => {
    try {
      window.sessionStorage.setItem("dal_obscura_post_login_hash", window.location.hash);
    } catch {
      // Private browsing policies may deny sessionStorage; login still works.
    }
    window.location.assign("/auth/login");
  },
  getSession: async (signal?: AbortSignal) => {
    const session = await request<ApiSchemas["SessionResponse"]>("/v1/session", { signal });
    return {
      ...session,
      // Older control-plane instances may omit the additive field during a
      // rolling upgrade. Missing metadata must fail closed for management UI.
      capabilities: Array.isArray(session.capabilities) ? session.capabilities : [],
    };
  },
  getUiAuthConfig: (signal?: AbortSignal) => request<ApiSchemas["UiAuthConfigResponse"]>("/v1/ui-auth-config", { signal }),
  getSessionOptions: async (signal?: AbortSignal) => {
    const options = await request<ApiSchemas["SessionOptionsResponse"]>("/v1/session/options", { signal });
    return { ...options, oidc: options.oidc ?? null } satisfies SessionOptions;
  },
  bootstrapLogin: (token: string, signal?: AbortSignal) => request<ApiSchemas["AuthenticationMutationResponse"]>("/v1/session/bootstrap", {
    method: "POST",
    headers: { authorization: `Bearer ${token}` },
    signal,
  }),
  logout: (signal?: AbortSignal) => request<ApiSchemas["AuthenticationMutationResponse"]>("/v1/logout", { method: "POST", signal }),
  listAssetPage: async (params: { limit?: number; cursor?: string; search?: string; signal?: AbortSignal } = {}) => {
    const query = new URLSearchParams();
    if (params.limit !== undefined) query.set("limit", String(params.limit));
    if (params.cursor) query.set("cursor", params.cursor);
    if (params.search) query.set("search", params.search);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    const page = await request<ApiSchemas["AssetInventoryPageResponse"]>(`/v1/assets/page${suffix}`, { signal: params.signal });
    return { items: page.items.map(normalizeInventoryAsset), next_cursor: page.next_cursor ?? null } satisfies AssetPage;
  },
  getAsset: async (assetId: string, signal?: AbortSignal) => normalizeDetailAsset(await request<ApiSchemas["AssetDetailResponse"]>(assetPath(assetId), { signal })),
  getAssetAccess: async (assetId: string, signal?: AbortSignal) => {
    const access = await request<ApiSchemas["AssetAccessResponse"]>(`${assetPath(assetId)}/access`, { signal });
    return { ...access, issuer: access.issuer ?? null } satisfies AssetAccess;
  },
  listGrants: (assetId: string, signal?: AbortSignal) => request<ApiSchemas["AssetGrantResponse"][]>(`${assetPath(assetId)}/grants`, { signal }),
  saveOwners: (assetId: string, owners: string[], expectedRevision?: number, signal?: AbortSignal) => request<ApiSchemas["AssetOwnersResponse"]>(`${assetPath(assetId)}/owners`, {
    method: "PUT",
    body: JSON.stringify({ owners, ...(expectedRevision === undefined ? {} : { expected_revision: expectedRevision }) }),
    signal,
  }),
  saveGrants: (assetId: string, grants: AssetGrant[], expectedRevision?: number, signal?: AbortSignal) => request<ApiSchemas["AssetGrantsResponse"]>(`${assetPath(assetId)}/grants`, {
    method: "PUT",
    body: JSON.stringify({ grants, ...(expectedRevision === undefined ? {} : { expected_revision: expectedRevision }) }),
    signal,
  }),
  getSchema: (assetId: string, signal?: AbortSignal) => request<ApiSchemas["AssetSchemaResponse"]>(`${assetPath(assetId)}/schema`, { signal }) as Promise<AssetSchema>,
  listAuditEventsPage: async (params: { limit?: number; cursor?: string; assetId?: string; actor?: string; action?: string; resourceType?: string; outcome?: string; correlationId?: string; createdAfter?: string; createdBefore?: string; signal?: AbortSignal } = {}) => {
    const query = new URLSearchParams();
    if (params.limit !== undefined) query.set("limit", String(params.limit));
    if (params.cursor) query.set("cursor", params.cursor);
    if (params.assetId) query.set("asset_id", params.assetId);
    if (params.actor) query.set("actor", params.actor);
    if (params.action) query.set("action", params.action);
    if (params.resourceType) query.set("resource_type", params.resourceType);
    if (params.outcome) query.set("outcome", params.outcome);
    if (params.correlationId) query.set("correlation_id", params.correlationId);
    if (params.createdAfter) query.set("created_after", params.createdAfter);
    if (params.createdBefore) query.set("created_before", params.createdBefore);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    const page = await request<ApiSchemas["AuditEventPageResponse"]>(`/v1/audit/events/page${suffix}`, { signal: params.signal });
    return { items: page.items.map((event) => ({ ...event, correlation_id: event.correlation_id ?? null })), next_cursor: page.next_cursor ?? null } satisfies AuditEventPage;
  },
  listCatalogs: (signal?: AbortSignal) => request<ApiSchemas["CatalogInventoryResponse"][]>("/v1/catalogs", { signal }),
  discoverCatalogTables: (name: string, signal?: AbortSignal) => request<ApiSchemas["CatalogTablesResponse"]>(`/v1/catalogs/${encodeURIComponent(name)}/tables`, { signal }),
  diagnoseCatalog: async (name: string, signal?: AbortSignal) => {
    const diagnostic = await request<ApiSchemas["CatalogDiagnosticResponse"]>(`/v1/catalogs/${encodeURIComponent(name)}/diagnostics`, { signal });
    return { ...diagnostic, sample_tables: diagnostic.sample_tables ?? undefined, table_count: diagnostic.table_count ?? undefined } satisfies CatalogDiagnostic;
  },
  getRuntimeSettings: (signal?: AbortSignal) => request<ApiSchemas["RuntimeSettingsResponse"] | null>("/v1/settings/runtime", { signal }),
  getAuthProviders: (signal?: AbortSignal) => request<ApiSchemas["AuthProviderResponse"][]>("/v1/settings/auth-providers", { signal }),
  getAuthProviderRevision: (signal?: AbortSignal) => request<ApiSchemas["AuthProviderRevisionResponse"]>("/v1/settings/auth-providers/revision", { signal }),
  saveAuthProviders: (providers: Array<{ ordinal: number; module: string; args: Record<string, unknown>; enabled: boolean }>, expectedRevision?: number, signal?: AbortSignal) => request<ApiSchemas["AuthProviderResponse"][]>("/v1/settings/auth-providers", {
    method: "PUT",
    body: JSON.stringify({ providers, ...(expectedRevision === undefined ? {} : { expected_revision: expectedRevision }) }),
    signal,
  }),
  listPlugins: async (signal?: AbortSignal) => {
    const plugins = await request<ApiSchemas["PluginListResponse"]>("/v1/plugins", { signal });
    return {
      ...plugins,
      states: plugins.states.map((state) => ({ ...state, lifecycle: state.lifecycle ?? undefined, reason: state.reason ?? undefined })),
    } satisfies { plugins: PluginDescriptor[]; states: PluginState[]; pairs: PluginPair[] };
  },
  setPluginLifecycle: (
    kind: "catalog" | "table_format",
    pluginId: string,
    target: "enabled" | "draining" | "disabled" | "revoked" | "removed",
    signal?: AbortSignal,
  ) => request<ApiSchemas["PluginLifecycleResponse"]>(
    `/v1/plugins/${encodeURIComponent(kind)}/${encodeURIComponent(pluginId)}/lifecycle`,
    { method: "PATCH", body: JSON.stringify({ target }), signal },
  ),
  getSummary: (signal?: AbortSignal) => request<ApiSchemas["WorkspaceSummaryResponse"]>("/v1/workspace/summary", { signal }),
  getObservations: async (signal?: AbortSignal) => {
    const observations = await request<ApiSchemas["WorkspaceObservationsResponse"]>("/v1/workspace/observations", { signal });
    return observations satisfies WorkspaceObservations;
  },
  saveCatalog: (name: string, pluginId: string, options: Record<string, unknown>, expectedRevision?: number, signal?: AbortSignal) => request<ApiSchemas["CatalogMutationResponse"]>(`/v1/catalogs/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify({ plugin_id: pluginId, options, ...(expectedRevision === undefined ? {} : { expected_revision: expectedRevision }) }),
    signal,
  }),
  saveAsset: (catalog: string, target: string, backend: string, tableIdentifier: string, signal?: AbortSignal) => request<ApiSchemas["AssetMutationResponse"]>(`/v1/assets/${encodeURIComponent(catalog)}/${encodeURIComponent(target)}`, {
    method: "PUT",
    body: JSON.stringify({ backend, table_identifier: tableIdentifier, options: {} }),
    signal,
  }),
  replaceAssetPolicy: (assetId: string, expectedRevision: number, rules: PolicyRule[], revokeExistingTokens: boolean, signal?: AbortSignal) => request<ApiSchemas["PolicyMutationResponse"]>(`${assetPath(assetId)}/policy`, {
    method: "PUT",
    body: JSON.stringify({ expected_revision: expectedRevision, rules, revoke_existing_tokens: revokeExistingTokens }),
    signal,
  }),
  revokeAssetTokens: (assetId: string, signal?: AbortSignal) => request<ApiSchemas["AssetTokenRevocationResponse"]>(`${assetPath(assetId)}/tickets/revoke`, {
    method: "POST",
    signal,
  }),
  saveRuntimeSettings: (settings: RuntimeSettings, signal?: AbortSignal) => request<ApiSchemas["RuntimeSettingsResponse"]>("/v1/settings/runtime", {
    method: "PUT",
    body: JSON.stringify({ ticket_ttl_seconds: settings.ticket_ttl_seconds, max_tickets: settings.max_tickets, max_ticket_exchanges: settings.max_ticket_exchanges, path_rules: settings.path_rules, ...(settings.revision === undefined ? {} : { expected_revision: settings.revision }) }),
    signal,
  }),
  evaluate: async (assetId: string, persona: { principal: string; groups: string[]; claims: Record<string, unknown> }, signal?: AbortSignal) => {
    const raw = await request<ApiSchemas["PolicyEvaluationResponse"]>(`${assetPath(assetId)}/policy-evaluate`, {
      method: "POST",
      body: JSON.stringify(persona),
      signal,
    });
    return {
      decision: raw.decision,
      allowed_columns: raw.allowed_columns,
      masks: Object.fromEntries((raw.masks as Array<{ column: string; type: Mask["type"] }>).map((mask) => [mask.column, { type: mask.type }])),
      row_filter: raw.row_filter ?? null,
      policy_revision: raw.policy_revision,
      status: "completed",
      rows: raw.rows,
      output_rows: raw.output_rows,
      evidence: raw.evidence,
    } satisfies Preview;
  },
};

function readCookie(name: string): string | undefined {
  const prefix = `${name}=`;
  return document.cookie.split("; ").find((cookie) => cookie.startsWith(`__Host-${prefix}`))?.slice(`__Host-`.length + prefix.length)
    ?? document.cookie.split("; ").find((cookie) => cookie.startsWith(prefix))?.slice(prefix.length);
}

function normalizeAsset(asset: Asset): Asset {
  if (typeof asset.name !== "string" || !asset.name || !isStringArray(asset.owners) || !Array.isArray(asset.schema_fields)) throw new Error("Invalid asset response.");
  return asset;
}

function normalizeInventoryAsset(asset: ApiSchemas["AssetInventoryResponse"]): Asset {
  return normalizeAsset({
    id: asset.id,
    catalog: asset.catalog,
    name: asset.name,
    backend: asset.backend,
    table_identifier: asset.table_identifier,
    owners: asset.owners,
    schema_fields: [],
    policy_status: asset.policy_status,
    policy_revision: asset.policy_revision,
  });
}

function normalizeDetailAsset(asset: ApiSchemas["AssetDetailResponse"]): Asset {
  return normalizeAsset({
    id: asset.id,
    revision: asset.revision,
    policy_revision: asset.policy_revision,
    catalog: asset.catalog,
    name: asset.name,
    backend: asset.backend,
    table_identifier: asset.table_identifier,
    owners: asset.owners,
    policy_status: asset.policy_status,
    schema_fields: asset.schema_fields.map((field) => {
      if (typeof field.name !== "string" || typeof field.type !== "string" || typeof field.nullable !== "boolean") throw new Error("Invalid schema field response.");
      return { name: field.name, type: field.type, nullable: field.nullable };
    }),
    policy_rules: asset.policy_rules.map(normalizePolicyRule),
  });
}

function normalizePolicyRule(value: ApiSchemas["AssetDetailResponse"]["policy_rules"][number], index: number): PolicyRule {
  const rule = value as Record<string, unknown>;
  const effect = rule.effect;
  if (effect !== "allow" && effect !== "allow_all") throw new Error(`Asset policy rule ${index + 1} has unsupported effect.`);
  if (!Number.isInteger(rule.ordinal)) throw new Error(`Asset policy rule ${index + 1} has invalid ordinal.`);
  if (!isStringArray(rule.principals) || !isStringArray(rule.columns)) throw new Error(`Asset policy rule ${index + 1} has invalid principal or column selections.`);
  if (rule.row_filter !== null && typeof rule.row_filter !== "string") throw new Error(`Asset policy rule ${index + 1} has invalid row filter.`);
  if (rule.masks === null || typeof rule.masks !== "object" || Array.isArray(rule.masks)) throw new Error(`Asset policy rule ${index + 1} has invalid masks.`);
  if (rule.when === null || typeof rule.when !== "object" || Array.isArray(rule.when)) throw new Error(`Asset policy rule ${index + 1} has invalid conditions.`);

  const supportedMasks: Mask["type"][] = ["null", "redact", "hash", "email", "keep_last", "default"];
  const masks = Object.fromEntries(Object.entries(rule.masks).map(([field, rawMask]) => {
    if (rawMask === null || typeof rawMask !== "object" || Array.isArray(rawMask)) throw new Error(`Asset policy rule ${index + 1} has invalid mask for ${field}.`);
    const mask = rawMask as Record<string, unknown>;
    if (typeof mask.type !== "string" || !supportedMasks.includes(mask.type as Mask["type"])) throw new Error(`Asset policy rule ${index + 1} has unsupported mask for ${field}.`);
    const scalar = mask.value;
    if (scalar !== undefined && scalar !== null && !["string", "number", "boolean"].includes(typeof scalar)) throw new Error(`Asset policy rule ${index + 1} has invalid mask value for ${field}.`);
    const exemptions = mask.exempt_principals;
    const validExemptions = exemptions === undefined || (isStringArray(exemptions) && exemptions.every((token) => token.trim() && token === token.trim() && token !== "*" && token.toLowerCase() !== "everyone" && (!token.startsWith("group:") || (token.slice(6).trim() && !["*", "everyone"].includes(token.slice(6).toLowerCase()) && !/\s/.test(token)))));
    if (!validExemptions) throw new Error(`Asset policy rule ${index + 1} has invalid mask exemptions for ${field}.`);
    return [field, { type: mask.type as Mask["type"], ...(scalar === undefined ? {} : { value: scalar as Mask["value"] }), ...(exemptions?.length ? { exempt_principals: [...new Set(exemptions)].sort() } : {}) }];
  }));
  const when: Record<string, string | string[]> = {};
  for (const [claim, condition] of Object.entries(rule.when)) {
    if (typeof condition === "string") when[claim] = condition;
    else if (isStringArray(condition)) when[claim] = condition;
    else throw new Error(`Asset policy rule ${index + 1} has invalid condition for ${claim}.`);
  }
  return {
    ordinal: rule.ordinal as number,
    effect,
    name: typeof rule.name === "string" ? rule.name : "",
    description: typeof rule.description === "string" ? rule.description : "",
    principals: rule.principals,
    columns: rule.columns,
    masks,
    row_filter: rule.row_filter,
    when,
  };
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}
