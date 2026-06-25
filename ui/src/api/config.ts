export type RuntimeConfig = {
  apiBaseUrl: string;
};

let runtimeConfig: RuntimeConfig = { apiBaseUrl: "" };
let loaded = false;

export async function loadRuntimeConfig(): Promise<RuntimeConfig> {
  if (loaded) {
    return runtimeConfig;
  }
  try {
    const response = await fetch("/config.json", {
      headers: { Accept: "application/json" },
    });
    if (response.ok) {
      const payload = (await response.json()) as Partial<RuntimeConfig>;
      runtimeConfig = {
        apiBaseUrl: normalizeApiBaseUrl(payload.apiBaseUrl || ""),
      };
    }
  } catch {
    runtimeConfig = { apiBaseUrl: "" };
  }
  loaded = true;
  return runtimeConfig;
}

export function apiUrl(path: string): string {
  if (/^https?:\/\//.test(path)) {
    return path;
  }
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${runtimeConfig.apiBaseUrl}${normalizedPath}`;
}

export function normalizeApiBaseUrl(value: string): string {
  return value.trim().replace(/\/+$/, "");
}

export function clearRuntimeConfigCacheForTests(): void {
  runtimeConfig = { apiBaseUrl: "" };
  loaded = false;
}
