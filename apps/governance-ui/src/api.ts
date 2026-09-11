export type Asset = {
  id: string;
  catalog: string;
  name: string;
  backend: "iceberg";
  table_identifier: string;
  owners: string[];
  schema_fields: SchemaField[];
};

export type SchemaField = {
  name: string;
  type: string;
  nullable: boolean;
};

export type Mask = {
  type: "null" | "redact" | "hash" | "email" | "keep_last" | "default";
  value?: string | number | null;
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

export type Preview = {
  allowed_columns: string[];
  masks: Record<string, Mask>;
  row_filter: string | null;
  policy_version: number;
};

export type Session = {
  principal: string;
  groups: string[];
  platform_admin: boolean;
};

export type UiAuthConfig = {
  login_shortcuts?: Array<{ label: string; login_hint: string; demo_login_path?: string }>;
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
  if (init?.method && !["GET", "HEAD", "OPTIONS"].includes(init.method) && csrf) {
    headers.set("x-csrf-token", csrf);
  }
  if (init?.body && !headers.has("content-type")) headers.set("content-type", "application/json");
  const response = await fetch(path, {
    credentials: "same-origin",
    headers,
    ...init,
  });
  if (!response.ok) {
    const failure = new Error(`Request failed (${response.status})`) as ApiFailure;
    failure.status = response.status;
    throw failure;
  }
  return response.json() as Promise<T>;
}

export const controlPlane = {
  getSession: () => request<Session>("/v1/session"),
  getUiAuthConfig: () => request<UiAuthConfig>("/v1/ui-auth-config"),
  demoLogin: (loginHint: string) => request<{ authenticated: true }>("/v1/demo-login", {
    method: "POST",
    body: JSON.stringify({ login_hint: loginHint }),
  }),
  logout: () => request<{ authenticated: false }>("/v1/logout", { method: "POST" }),
  listAssets: async () => (await request<Asset[]>("/v1/assets")).map(normalizeAsset),
  getAsset: async (assetId: string) => normalizeAsset(await request<Asset>(`/v1/assets/${assetId}`)),
  listRules: (assetId: string) => request<PolicyRule[]>(`/v1/assets/${assetId}/policy-rules`),
  saveRules: (assetId: string, rules: PolicyRule[]) =>
    request(`/v1/assets/${assetId}/policy-rules`, {
      method: "PUT",
      body: JSON.stringify({ rules }),
    }),
  preview: async (assetId: string, persona: { principal: string; groups: string[]; claims: Record<string, object> }) => {
    const raw = await request<RawPreview>(`/v1/assets/${assetId}/policy-preview`, {
      method: "POST",
      body: JSON.stringify(persona),
    });
    return {
      allowed_columns: raw.decision === "allow" ? raw.visible_columns : [],
      masks: Object.fromEntries(raw.masks.map((mask) => [mask.column, { type: mask.type }])),
      row_filter: raw.row_filter,
      policy_version: 0,
    } satisfies Preview;
  },
};

function readCookie(name: string): string | undefined {
  const prefix = `${name}=`;
  return document.cookie.split("; ").find((cookie) => cookie.startsWith(prefix))?.slice(prefix.length);
}

function normalizeAsset(asset: Asset): Asset {
  return {
    ...asset,
    name: asset.name ?? (asset as Asset & { target?: string }).target ?? "Unnamed asset",
    owners: asset.owners ?? [],
    schema_fields: asset.schema_fields ?? [],
  };
}
