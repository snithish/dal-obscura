import { describe, expect, test } from "vitest";

import {
  buildAuthorizationUrl,
  callbackCodeFromUrl,
  completeDemoLogin,
  tokenEndpoint,
  type PkceTransaction,
  type UiAuthConfig,
} from "./auth";
import { getAccessToken } from "./client";

const authConfig: UiAuthConfig = {
  authority: "http://127.0.0.1:8080/realms/dal-obscura-demo",
  client_id: "dal-obscura-ui",
  post_logout_redirect_uri: "http://127.0.0.1:8821",
  redirect_uri: "http://127.0.0.1:8821/auth/callback",
  scope: "openid profile",
};

const transaction: PkceTransaction = {
  codeVerifier: "verifier-123",
  returnTo: "/policies",
  state: "state-123",
};

describe("OIDC PKCE browser auth", () => {
  test("builds an authorization URL for a public SPA client", () => {
    const url = buildAuthorizationUrl(authConfig, transaction, "challenge-123");

    expect(url.origin + url.pathname).toBe(
      "http://127.0.0.1:8080/realms/dal-obscura-demo/protocol/openid-connect/auth",
    );
    expect(url.searchParams.get("client_id")).toBe("dal-obscura-ui");
    expect(url.searchParams.get("redirect_uri")).toBe("http://127.0.0.1:8821/auth/callback");
    expect(url.searchParams.get("response_type")).toBe("code");
    expect(url.searchParams.get("code_challenge")).toBe("challenge-123");
    expect(url.searchParams.get("code_challenge_method")).toBe("S256");
    expect(url.search).not.toContain("secret");
  });

  test("adds a selected login shortcut as an OIDC login hint", () => {
    const url = buildAuthorizationUrl(authConfig, transaction, "challenge-123", {
      loginHint: "asset-owner",
    });

    expect(url.searchParams.get("login_hint")).toBe("asset-owner");
  });

  test("stores a token returned by the demo login endpoint", async () => {
    const originalFetch = globalThis.fetch;
    globalThis.fetch = (async (_input, init) => {
      expect(init?.method).toBe("POST");
      expect(init?.body).toBe(JSON.stringify({ login_hint: "asset-owner" }));
      return new Response(JSON.stringify({ access_token: "demo-token" }), {
        headers: { "Content-Type": "application/json" },
        status: 200,
      });
    }) as typeof fetch;

    try {
      await completeDemoLogin("/v1/demo-login", "asset-owner");
    } finally {
      globalThis.fetch = originalFetch;
    }

    expect(getAccessToken()).toBe("demo-token");
  });

  test("validates callback state before returning the authorization code", () => {
    const code = callbackCodeFromUrl(
      "http://127.0.0.1:8821/auth/callback?code=code-123&state=state-123",
      transaction,
    );

    expect(code).toBe("code-123");
  });

  test("rejects callbacks with the wrong state", () => {
    expect(() =>
      callbackCodeFromUrl(
        "http://127.0.0.1:8821/auth/callback?code=code-123&state=wrong",
        transaction,
      ),
    ).toThrow("OIDC callback state did not match");
  });

  test("uses the issuer token endpoint", () => {
    expect(tokenEndpoint(authConfig).toString()).toBe(
      "http://127.0.0.1:8080/realms/dal-obscura-demo/protocol/openid-connect/token",
    );
  });
});
