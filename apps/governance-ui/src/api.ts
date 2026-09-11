export type Asset = {
  id: string;
  catalog: string;
  target: string;
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

type ApiFailure = Error & { status?: number };

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    credentials: "same-origin",
    headers: { "content-type": "application/json", ...init?.headers },
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
  listAssets: () => request<Asset[]>("/v1/assets"),
  listRules: (assetId: string) => request<PolicyRule[]>(`/v1/assets/${assetId}/policy-rules`),
  saveRules: (assetId: string, rules: PolicyRule[]) =>
    request(`/v1/assets/${assetId}/policy-rules`, {
      method: "PUT",
      body: JSON.stringify({ rules }),
    }),
  preview: (assetId: string, persona: { principal: string; groups: string[]; claims: Record<string, object> }) =>
    request<Preview>(`/v1/assets/${assetId}/policy-preview`, {
      method: "POST",
      body: JSON.stringify(persona),
    }),
};
