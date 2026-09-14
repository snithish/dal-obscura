import type { components } from "./generated/control_plane";

type ApiSchemas = components["schemas"];

export type Asset = {
  id: string;
  revision?: number;
  catalog: string;
  name: string;
  backend: string;
  table_identifier: string;
  owners: string[];
  schema_fields: SchemaField[];
  schema?: AssetSchema;
  /** Inventory state supplied by the workspace asset list. */
  policy_status?: "configured" | "missing" | string;
  draft_status?: "draft" | "published" | "none" | string;
  active_policy_version?: number | null;
  last_published_at?: string | null;
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
  fields: SchemaNode[];
};

export type Mask = {
  type: "null" | "redact" | "hash" | "email" | "keep_last" | "default";
  value?: string | number | boolean | null;
};

export type PolicyRule = {
  ordinal: number;
  principals: string[];
  columns: string[];
  masks: Record<string, Mask>;
  row_filter: string | null;
  effect: "allow";
  when?: Record<string, string | string[]>;
};

export type PolicyDraft = {
  id: string | null;
  asset_id: string;
  author_principal: string;
  revision: number;
  base_policy_version: number;
  rules: PolicyRule[];
  content_hash: string;
};

export type Preview = {
  decision?: "allow" | "deny";
  allowed_columns: string[];
  masks: Record<string, Mask>;
  row_filter: string | null;
  policy_version: number;
  status?: string;
  rows?: Array<Record<string, unknown>>;
  output_rows?: number;
  evidence?: Record<string, unknown>;
  review_token?: string;
  review_expires_at?: number;
  review_draft_id?: string | null;
  review_draft_author?: string | null;
  reviewer?: string;
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

export type PolicyVersion = {
  asset_id: string;
  asset_name: string;
  catalog: string;
  target: string;
  policy_version: number;
  active: boolean;
  created_at: string;
};

export type WorkspacePublication = {
  id: string;
  schema_version: number;
  status: string;
  manifest_hash: string;
  active: boolean;
  asset_count: number;
  catalog_count: number;
  created_at: string;
};

export type PolicyVersionPage = {
  items: PolicyVersion[];
  next_cursor: string | null;
};

export type PolicyVersionDetail = {
  asset_id: string;
  policy_version: number;
  rules: PolicyRule[];
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
  module: string;
  plugin_id?: string | null;
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
  capability: "read" | "edit" | "publish" | "grant";
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
  capabilities: AssetCapability[];
};

export type RuntimeSettings = {
  ticket_ttl_seconds: number;
  max_tickets: number;
  max_ticket_exchanges: number;
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
  draft_change_count: number;
  runtime_configured: boolean;
  enabled_auth_provider_count: number;
};

export type WorkspaceObservations = {
  available: boolean;
  observed_at: string;
  source: string;
  generation: { cell_id: string; publication_id: string; manifest_hash: string; status: string } | null;
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
  bootstrapLogin: (token: string) => request<ApiSchemas["AuthenticationMutationResponse"]>("/v1/session/bootstrap", {
    method: "POST",
    headers: { authorization: `Bearer ${token}` },
  }),
  logout: () => request<ApiSchemas["AuthenticationMutationResponse"]>("/v1/logout", { method: "POST" }),
  listAssets: async () => (await request<ApiSchemas["AssetInventoryResponse"][]>("/v1/assets")).map(normalizeInventoryAsset),
  listAssetPage: async (params: { limit?: number; cursor?: string; search?: string; signal?: AbortSignal } = {}) => {
    const query = new URLSearchParams();
    if (params.limit !== undefined) query.set("limit", String(params.limit));
    if (params.cursor) query.set("cursor", params.cursor);
    if (params.search) query.set("search", params.search);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    const page = await request<ApiSchemas["AssetInventoryPageResponse"]>(`/v1/assets/page${suffix}`, { signal: params.signal });
    return { items: page.items.map(normalizeInventoryAsset), next_cursor: page.next_cursor ?? null } satisfies AssetPage;
  },
  getAsset: async (assetId: string, signal?: AbortSignal) => normalizeDetailAsset(await request<ApiSchemas["AssetDetailResponse"]>(`/v1/assets/${assetId}`, { signal })),
  getAssetAccess: async (assetId: string, signal?: AbortSignal) => {
    const access = await request<ApiSchemas["AssetAccessResponse"]>(`/v1/assets/${assetId}/access`, { signal });
    return { ...access, issuer: access.issuer ?? null } satisfies AssetAccess;
  },
  listGrants: (assetId: string, signal?: AbortSignal) => request<ApiSchemas["AssetGrantResponse"][]>(`/v1/assets/${assetId}/grants`, { signal }),
  saveOwners: (assetId: string, owners: string[], expectedRevision?: number) => request<ApiSchemas["AssetOwnersResponse"]>(`/v1/assets/${assetId}/owners`, {
    method: "PUT",
    body: JSON.stringify({ owners, ...(expectedRevision === undefined ? {} : { expected_revision: expectedRevision }) }),
  }),
  saveGrants: (assetId: string, grants: AssetGrant[], expectedRevision?: number) => request<ApiSchemas["AssetGrantsResponse"]>(`/v1/assets/${assetId}/grants`, {
    method: "PUT",
    body: JSON.stringify({ grants, ...(expectedRevision === undefined ? {} : { expected_revision: expectedRevision }) }),
  }),
  getSchema: (assetId: string, signal?: AbortSignal) => request<ApiSchemas["AssetSchemaResponse"]>(`/v1/assets/${assetId}/schema`, { signal }) as Promise<AssetSchema>,
  listHistory: async (signal?: AbortSignal) => (await request<ApiSchemas["PolicyVersionResponse"][]>("/v1/policy-versions", { signal })),
  listHistoryPage: async (params: { limit?: number; cursor?: string; signal?: AbortSignal } = {}) => {
    const query = new URLSearchParams();
    if (params.limit !== undefined) query.set("limit", String(params.limit));
    if (params.cursor) query.set("cursor", params.cursor);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    const page = await request<ApiSchemas["PolicyVersionPageResponse"]>(`/v1/policy-versions/page${suffix}`, { signal: params.signal });
    return { items: page.items, next_cursor: page.next_cursor ?? null } satisfies PolicyVersionPage;
  },
  listAuditEvents: (assetId?: string) => request<AuditEvent[]>("/v1/audit/events" + (assetId ? "?asset_id=" + encodeURIComponent(assetId) : "")),
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
  listAssetHistory: (assetId: string, signal?: AbortSignal) => request<ApiSchemas["PolicyVersionResponse"][]>(`/v1/assets/${assetId}/policy-versions`, { signal }),
  getPublicationOperation: (assetId: string, idempotencyKey: string) => request<ApiSchemas["PolicyOperationResponse"]>(`/v1/assets/${assetId}/policy-operations/${encodeURIComponent(idempotencyKey)}`),
  getPolicyVersion: async (assetId: string, policyVersion: number, signal?: AbortSignal) => {
    const detail = await request<ApiSchemas["PolicyVersionDetailResponse"]>(`/v1/assets/${assetId}/policy-versions/${policyVersion}`, { signal });
    return { ...detail, rules: detail.rules as PolicyRule[] } satisfies PolicyVersionDetail;
  },
  restorePolicyVersion: (assetId: string, policyVersion: number, expectedRevision: number) =>
    request<ApiSchemas["PolicyDraftResponse"]>(`/v1/assets/${assetId}/policy-versions/${policyVersion}/restore`, {
      method: "POST",
      body: JSON.stringify({ expected_revision: expectedRevision }),
    }) as Promise<PolicyDraft>,
  listCatalogs: (signal?: AbortSignal) => request<ApiSchemas["CatalogInventoryResponse"][]>("/v1/catalogs", { signal }),
  listWorkspacePublications: (signal?: AbortSignal) => request<ApiSchemas["WorkspacePublicationResponse"][]>("/v1/workspace/publications", { signal }),
  createWorkspacePublication: () => request<ApiSchemas["WorkspacePublicationCreateResponse"]>("/v1/workspace/publications", { method: "POST" }),
  activateWorkspacePublication: (publicationId: string, expectedPublicationId?: string) => request<ApiSchemas["PublicationActivationResponse"]>(`/v1/workspace/publications/${encodeURIComponent(publicationId)}/activate`, {
    method: "POST",
    body: JSON.stringify({ expected_publication_id: expectedPublicationId ?? null }),
  }),
  discoverCatalogTables: (name: string, signal?: AbortSignal) => request<ApiSchemas["CatalogTablesResponse"]>(`/v1/catalogs/${encodeURIComponent(name)}/tables`, { signal }),
  diagnoseCatalog: async (name: string, signal?: AbortSignal) => {
    const diagnostic = await request<ApiSchemas["CatalogDiagnosticResponse"]>(`/v1/catalogs/${encodeURIComponent(name)}/diagnostics`, { signal });
    return { ...diagnostic, sample_tables: diagnostic.sample_tables ?? undefined, table_count: diagnostic.table_count ?? undefined } satisfies CatalogDiagnostic;
  },
  getRuntimeSettings: (signal?: AbortSignal) => request<ApiSchemas["RuntimeSettingsResponse"] | null>("/v1/settings/runtime", { signal }),
  getAuthProviders: (signal?: AbortSignal) => request<ApiSchemas["AuthProviderResponse"][]>("/v1/settings/auth-providers", { signal }),
  getAuthProviderRevision: (signal?: AbortSignal) => request<ApiSchemas["AuthProviderRevisionResponse"]>("/v1/settings/auth-providers/revision", { signal }),
  saveAuthProviders: (providers: Array<{ ordinal: number; module: string; args: Record<string, unknown>; enabled: boolean }>, expectedRevision?: number) => request<ApiSchemas["AuthProviderResponse"][]>("/v1/settings/auth-providers", {
    method: "PUT",
    body: JSON.stringify({ providers, ...(expectedRevision === undefined ? {} : { expected_revision: expectedRevision }) }),
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
  ) => request<ApiSchemas["PluginLifecycleResponse"]>(
    `/v1/plugins/${encodeURIComponent(kind)}/${encodeURIComponent(pluginId)}/lifecycle`,
    { method: "PATCH", body: JSON.stringify({ target }) },
  ),
  getSummary: (signal?: AbortSignal) => request<ApiSchemas["WorkspaceSummaryResponse"]>("/v1/workspace/summary", { signal }),
  getObservations: async (signal?: AbortSignal) => {
    const observations = await request<ApiSchemas["WorkspaceObservationsResponse"]>("/v1/workspace/observations", { signal });
    return { ...observations, generation: observations.generation ?? null } satisfies WorkspaceObservations;
  },
  saveCatalog: (name: string, module: string, options: Record<string, unknown>, expectedRevision?: number) => request<ApiSchemas["CatalogMutationResponse"]>(`/v1/catalogs/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify({ module, options, ...(expectedRevision === undefined ? {} : { expected_revision: expectedRevision }) }),
  }),
  saveAsset: (catalog: string, target: string, backend: string, tableIdentifier: string) => request<ApiSchemas["AssetMutationResponse"]>(`/v1/assets/${encodeURIComponent(catalog)}/${encodeURIComponent(target)}`, {
    method: "PUT",
    body: JSON.stringify({ backend, table_identifier: tableIdentifier, options: {} }),
  }),
  saveRuntimeSettings: (settings: RuntimeSettings) => request<ApiSchemas["RuntimeSettingsResponse"]>("/v1/settings/runtime", {
    method: "PUT",
    body: JSON.stringify({ ticket_ttl_seconds: settings.ticket_ttl_seconds, max_tickets: settings.max_tickets, max_ticket_exchanges: settings.max_ticket_exchanges, ...(settings.revision === undefined ? {} : { expected_revision: settings.revision }) }),
  }),
  publishAsset: (assetId: string, expectedDraftRevision?: number, reviewToken?: string, idempotencyKey?: string, draftId?: string) => request<ApiSchemas["PolicyVersionCreateResponse"]>(`/v1/assets/${assetId}/policy-versions`, {
    method: "POST",
    headers: idempotencyKey ? { "Idempotency-Key": idempotencyKey } : undefined,
    body: JSON.stringify({ ...(draftId ? { draft_id: draftId } : {}), ...(expectedDraftRevision === undefined ? {} : { expected_draft_revision: expectedDraftRevision }), ...(reviewToken ? { review_token: reviewToken } : {}) }),
  }),
  getDraft: (assetId: string, draftId?: string, signal?: AbortSignal) => request<ApiSchemas["PolicyDraftResponse"]>(draftId ? `/v1/assets/${assetId}/draft/${encodeURIComponent(draftId)}` : `/v1/assets/${assetId}/draft`, { signal }) as Promise<PolicyDraft>,
  saveDraft: (assetId: string, expectedRevision: number, rules: PolicyRule[]) =>
    request<ApiSchemas["PolicyDraftResponse"]>(`/v1/assets/${assetId}/draft`, {
      method: "PUT",
      body: JSON.stringify({ expected_revision: expectedRevision, rules }),
    }) as Promise<PolicyDraft>,
  evaluate: async (assetId: string, persona: { principal: string; groups: string[]; claims: Record<string, unknown>; draft_id?: string; draft_revision?: number }) => {
    const raw = await request<ApiSchemas["PolicyEvaluationResponse"]>(`/v1/assets/${assetId}/policy-evaluate`, {
      method: "POST",
      body: JSON.stringify(persona),
    });
    return {
      decision: raw.decision,
      allowed_columns: raw.allowed_columns,
      masks: Object.fromEntries((raw.masks as Array<{ column: string; type: Mask["type"] }>).map((mask) => [mask.column, { type: mask.type }])),
      row_filter: raw.row_filter ?? null,
      policy_version: 0,
      status: "completed",
      rows: raw.rows,
      output_rows: raw.output_rows,
      evidence: raw.evidence,
    } satisfies Preview;
  },
  review: async (assetId: string, persona: { principal: string; groups: string[]; claims: Record<string, unknown>; draft_id?: string; draft_revision?: number }) => {
    const raw = await request<ApiSchemas["PolicyReviewResponse"]>("/v1/assets/" + assetId + "/policy-review", {
      method: "POST",
      body: JSON.stringify(persona),
    });
    return {
      decision: raw.decision,
      allowed_columns: raw.allowed_columns,
      masks: Object.fromEntries((raw.masks as Array<{ column: string; type: Mask["type"] }>).map((mask) => [mask.column, { type: mask.type }])),
      row_filter: raw.row_filter ?? null,
      policy_version: 0,
      status: "completed",
      rows: raw.rows,
      output_rows: raw.output_rows,
      evidence: raw.evidence,
      review_token: raw.review_token ?? undefined,
      review_expires_at: raw.review_expires_at ?? undefined,
      review_draft_id: raw.review_draft_id ?? undefined,
      review_draft_author: raw.review_draft_author ?? undefined,
      reviewer: raw.reviewer ?? undefined,
    } satisfies Preview;
  },
};

function readCookie(name: string): string | undefined {
  const prefix = `${name}=`;
  return document.cookie.split("; ").find((cookie) => cookie.startsWith(`__Host-${prefix}`))?.slice(`__Host-`.length + prefix.length)
    ?? document.cookie.split("; ").find((cookie) => cookie.startsWith(prefix))?.slice(prefix.length);
}

function normalizeAsset(asset: Asset): Asset {
  return {
    ...asset,
    name: asset.name ?? (asset as Asset & { target?: string }).target ?? "Unnamed asset",
    owners: asset.owners ?? [],
    schema_fields: asset.schema_fields ?? [],
  };
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
    draft_status: asset.draft_status,
    active_policy_version: asset.active_policy_version,
    last_published_at: asset.last_published_at,
  });
}

function normalizeDetailAsset(asset: ApiSchemas["AssetDetailResponse"]): Asset {
  return normalizeAsset({
    id: asset.id,
    revision: asset.revision,
    catalog: asset.catalog,
    name: asset.name,
    backend: asset.backend,
    table_identifier: asset.table_identifier,
    owners: asset.owners,
    schema_fields: asset.schema_fields.map((field) => ({
      name: typeof field.name === "string" ? field.name : "",
      type: typeof field.type === "string" ? field.type : "string",
      nullable: field.nullable !== false,
    })),
  });
}
