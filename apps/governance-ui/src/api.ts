export type Asset = {
  id: string;
  revision?: number;
  catalog: string;
  name: string;
  backend: "iceberg";
  table_identifier: string;
  owners: string[];
  schema_fields: SchemaField[];
  schema?: AssetSchema;
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
};

export type Session = {
  principal: string;
  groups: string[];
  platform_admin: boolean;
  issuer?: string;
};

export type UiAuthConfig = {
  authority?: string;
  client_id?: string;
  redirect_uri?: string;
  login_shortcuts?: Array<{ label: string; login_hint: string; demo_login_path?: string }>;
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

export type Catalog = {
  id: string;
  name: string;
  module: string;
  options: Record<string, unknown>;
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

export type RuntimeSettings = {
  ticket_ttl_seconds: number;
  max_tickets: number;
  max_ticket_exchanges: number;
};

export type AuthProvider = {
  id: string;
  ordinal: number;
  module: string;
  args: Record<string, unknown>;
  enabled: boolean;
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

type RawPreview = {
  decision: "allow" | "deny";
  visible_columns: string[];
  masks: Array<{ column: string; type: Mask["type"] }>;
  row_filter: string | null;
};

type ApiFailure = Error & { status?: number };

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
    const failure = new Error(`Request failed (${response.status})`) as ApiFailure;
    failure.status = response.status;
    throw failure;
  }
  return response.json() as Promise<T>;
}

export const controlPlane = {
  startLogin: () => { window.location.assign("/auth/login"); },
  getSession: () => request<Session>("/v1/session"),
  getUiAuthConfig: () => request<UiAuthConfig>("/v1/ui-auth-config"),
  getSessionOptions: () => request<SessionOptions>("/v1/session/options"),
  bootstrapLogin: (token: string) => request<{ authenticated: true }>("/v1/session/bootstrap", {
    method: "POST",
    headers: { authorization: `Bearer ${token}` },
  }),
  demoLogin: (loginHint: string) => request<{ authenticated: true }>("/v1/demo-login", {
    method: "POST",
    body: JSON.stringify({ login_hint: loginHint }),
  }),
  logout: () => request<{ authenticated: false }>("/v1/logout", { method: "POST" }),
  listAssets: async () => (await request<Asset[]>("/v1/assets")).map(normalizeAsset),
  listAssetPage: async (params: { limit?: number; cursor?: string; search?: string } = {}) => {
    const query = new URLSearchParams();
    if (params.limit !== undefined) query.set("limit", String(params.limit));
    if (params.cursor) query.set("cursor", params.cursor);
    if (params.search) query.set("search", params.search);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    const page = await request<AssetPage>(`/v1/assets/page${suffix}`);
    return { ...page, items: page.items.map(normalizeAsset) };
  },
  getAsset: async (assetId: string) => normalizeAsset(await request<Asset>(`/v1/assets/${assetId}`)),
  listGrants: (assetId: string) => request<AssetGrant[]>(`/v1/assets/${assetId}/grants`),
  saveOwners: (assetId: string, owners: string[], expectedRevision?: number) => request<{ asset_id: string; owners: string[] }>(`/v1/assets/${assetId}/owners`, {
    method: "PUT",
    body: JSON.stringify({ owners, ...(expectedRevision === undefined ? {} : { expected_revision: expectedRevision }) }),
  }),
  saveGrants: (assetId: string, grants: AssetGrant[], expectedRevision?: number) => request<{ asset_id: string; grants: AssetGrant[] }>(`/v1/assets/${assetId}/grants`, {
    method: "PUT",
    body: JSON.stringify({ grants, ...(expectedRevision === undefined ? {} : { expected_revision: expectedRevision }) }),
  }),
  getSchema: (assetId: string) => request<AssetSchema>(`/v1/assets/${assetId}/schema`),
  listRules: (assetId: string) => request<PolicyRule[]>(`/v1/assets/${assetId}/policy-rules`),
  listHistory: () => request<PolicyVersion[]>("/v1/policy-versions"),
  listHistoryPage: async (params: { limit?: number; cursor?: string } = {}) => {
    const query = new URLSearchParams();
    if (params.limit !== undefined) query.set("limit", String(params.limit));
    if (params.cursor) query.set("cursor", params.cursor);
    const suffix = query.toString() ? `?${query.toString()}` : "";
    return request<PolicyVersionPage>(`/v1/policy-versions/page${suffix}`);
  },
  listAuditEvents: (assetId?: string) => request<AuditEvent[]>("/v1/audit/events" + (assetId ? "?asset_id=" + encodeURIComponent(assetId) : "")),
  listAssetHistory: (assetId: string) => request<PolicyVersion[]>(`/v1/assets/${assetId}/policy-versions`),
  getPublicationOperation: (assetId: string, idempotencyKey: string) => request<{ id: string; status: string; result: { asset_id: string; policy_version: number } }>(`/v1/assets/${assetId}/policy-operations/${encodeURIComponent(idempotencyKey)}`),
  getPolicyVersion: (assetId: string, policyVersion: number) => request<PolicyVersionDetail>(`/v1/assets/${assetId}/policy-versions/${policyVersion}`),
  restorePolicyVersion: (assetId: string, policyVersion: number, expectedRevision: number) =>
    request<PolicyDraft>(`/v1/assets/${assetId}/policy-versions/${policyVersion}/restore`, {
      method: "POST",
      body: JSON.stringify({ expected_revision: expectedRevision }),
    }),
  listCatalogs: () => request<Catalog[]>("/v1/catalogs"),
  listWorkspacePublications: () => request<WorkspacePublication[]>("/v1/workspace/publications"),
  createWorkspacePublication: () => request<{ publication_id: string; asset_count: number; catalog_count: number; manifest_hash: string }>("/v1/workspace/publications", { method: "POST" }),
  activateWorkspacePublication: (publicationId: string) => request<{ publication_id: string }>(`/v1/workspace/publications/${encodeURIComponent(publicationId)}/activate`, { method: "POST" }),
  discoverCatalogTables: (name: string) => request<{ catalog: string; tables: Array<Record<string, unknown>> }>(`/v1/catalogs/${encodeURIComponent(name)}/tables`),
  diagnoseCatalog: (name: string) => request<CatalogDiagnostic>(`/v1/catalogs/${encodeURIComponent(name)}/diagnostics`),
  getRuntimeSettings: () => request<RuntimeSettings | null>("/v1/settings/runtime"),
  getAuthProviders: () => request<AuthProvider[]>("/v1/settings/auth-providers"),
  getSummary: () => request<WorkspaceSummary>("/v1/workspace/summary"),
  getObservations: () => request<WorkspaceObservations>("/v1/workspace/observations"),
  saveCatalog: (name: string, options: Record<string, unknown>) => request<{ id: string; name: string }>(`/v1/catalogs/${encodeURIComponent(name)}`, {
    method: "PUT",
    body: JSON.stringify({ module: "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog", options }),
  }),
  saveAsset: (catalog: string, target: string, tableIdentifier: string) => request<{ id: string; catalog: string; target: string }>(`/v1/assets/${encodeURIComponent(catalog)}/${encodeURIComponent(target)}`, {
    method: "PUT",
    body: JSON.stringify({ backend: "iceberg", table_identifier: tableIdentifier, options: {} }),
  }),
  saveRuntimeSettings: (settings: RuntimeSettings) => request<RuntimeSettings>("/v1/settings/runtime", {
    method: "PUT",
    body: JSON.stringify(settings),
  }),
  publishAsset: (assetId: string, expectedDraftRevision?: number, reviewToken?: string, idempotencyKey?: string) => request<{ asset_id: string; policy_version: number }>(`/v1/assets/${assetId}/policy-versions`, {
    method: "POST",
    headers: idempotencyKey ? { "Idempotency-Key": idempotencyKey } : undefined,
    body: JSON.stringify({ ...(expectedDraftRevision === undefined ? {} : { expected_draft_revision: expectedDraftRevision }), ...(reviewToken ? { review_token: reviewToken } : {}) }),
  }),
  getDraft: (assetId: string) => request<PolicyDraft>(`/v1/assets/${assetId}/draft`),
  saveDraft: (assetId: string, expectedRevision: number, rules: PolicyRule[]) =>
    request<PolicyDraft>(`/v1/assets/${assetId}/draft`, {
      method: "PUT",
      body: JSON.stringify({ expected_revision: expectedRevision, rules }),
    }),
  saveRules: (assetId: string, rules: PolicyRule[]) =>
    request(`/v1/assets/${assetId}/policy-rules`, {
      method: "PUT",
      body: JSON.stringify({ rules }),
    }),
  preview: async (assetId: string, persona: { principal: string; groups: string[]; claims: Record<string, unknown> }) => {
    const raw = await request<RawPreview>(`/v1/assets/${assetId}/policy-preview`, {
      method: "POST",
      body: JSON.stringify(persona),
    });
    return {
      decision: raw.decision,
      allowed_columns: raw.decision === "allow" ? raw.visible_columns : [],
      masks: Object.fromEntries(raw.masks.map((mask) => [mask.column, { type: mask.type }])),
      row_filter: raw.row_filter,
      policy_version: 0,
    } satisfies Preview;
  },
  evaluate: async (assetId: string, persona: { principal: string; groups: string[]; claims: Record<string, unknown> }) => {
    const raw = await request<{ decision: "allow" | "deny"; allowed_columns: string[]; masks: Array<{ column: string; type: Mask["type"] }>; row_filter: string | null; output_rows: number; rows: Array<Record<string, unknown>>; evidence: Record<string, unknown> }>(`/v1/assets/${assetId}/policy-evaluate`, {
      method: "POST",
      body: JSON.stringify(persona),
    });
    return {
      decision: raw.decision,
      allowed_columns: raw.allowed_columns,
      masks: Object.fromEntries(raw.masks.map((mask) => [mask.column, { type: mask.type }])),
      row_filter: raw.row_filter,
      policy_version: 0,
      status: "completed",
      rows: raw.rows,
      output_rows: raw.output_rows,
      evidence: raw.evidence,
    } satisfies Preview;
  },
  review: async (assetId: string, persona: { principal: string; groups: string[]; claims: Record<string, unknown> }) => {
    const raw = await request<{ decision: "allow" | "deny"; allowed_columns: string[]; masks: Array<{ column: string; type: Mask["type"] }>; row_filter: string | null; output_rows: number; rows: Array<Record<string, unknown>>; evidence: Record<string, unknown>; review_token: string; review_expires_at: number }>("/v1/assets/" + assetId + "/policy-review", {
      method: "POST",
      body: JSON.stringify(persona),
    });
    return {
      decision: raw.decision,
      allowed_columns: raw.allowed_columns,
      masks: Object.fromEntries(raw.masks.map((mask) => [mask.column, { type: mask.type }])),
      row_filter: raw.row_filter,
      policy_version: 0,
      status: "completed",
      rows: raw.rows,
      output_rows: raw.output_rows,
      evidence: raw.evidence,
      review_token: raw.review_token,
      review_expires_at: raw.review_expires_at,
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
