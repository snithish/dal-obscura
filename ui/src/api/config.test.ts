import { afterEach, describe, expect, it, vi } from "vitest";

import {
  apiUrl,
  clearRuntimeConfigCacheForTests,
  loadRuntimeConfig,
  normalizeApiBaseUrl,
} from "./config";

describe("runtime UI config", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    clearRuntimeConfigCacheForTests();
  });

  it("normalizes blank API base URL to same-origin requests", () => {
    expect(normalizeApiBaseUrl("")).toBe("");
    expect(normalizeApiBaseUrl("   ")).toBe("");
  });

  it("trims trailing slashes from API base URL", () => {
    expect(normalizeApiBaseUrl("http://127.0.0.1:8820///")).toBe("http://127.0.0.1:8820");
  });

  it("builds same-origin API URLs when no base URL is configured", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({}),
      }),
    );

    await loadRuntimeConfig();

    expect(apiUrl("/v1/assets")).toBe("/v1/assets");
  });

  it("builds absolute API URLs from runtime config", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue({
        ok: true,
        json: async () => ({ apiBaseUrl: "http://127.0.0.1:8820/" }),
      }),
    );

    await loadRuntimeConfig();

    expect(apiUrl("/v1/assets")).toBe("http://127.0.0.1:8820/v1/assets");
  });
});
