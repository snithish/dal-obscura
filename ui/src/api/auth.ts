import { apiGetPublic, setAccessToken } from "./client";
import { apiUrl } from "./config";

const OIDC_TRANSACTION_KEY = "dal-obscura.oidcTransaction";

export type UiAuthConfig = {
  authority: string;
  client_id: string;
  login_shortcuts?: UiLoginShortcut[];
  post_logout_redirect_uri?: string;
  redirect_uri: string;
  scope?: string;
};

export type UiLoginShortcut = {
  demo_login_path?: string;
  label: string;
  login_hint: string;
};

export type PkceTransaction = {
  codeVerifier: string;
  returnTo: string;
  state: string;
};

type TokenResponse = {
  access_token?: string;
  error?: string;
  error_description?: string;
};

type DemoLoginResponse = {
  access_token?: string;
};

export async function loadUiAuthConfig(): Promise<UiAuthConfig | null> {
  try {
    return await apiGetPublic<UiAuthConfig>("/v1/ui-auth-config");
  } catch {
    return null;
  }
}

export async function startOidcLogin(
  config: UiAuthConfig,
  returnTo: string,
  options: { loginHint?: string } = {},
): Promise<void> {
  const transaction = await createPkceTransaction(returnTo);
  window.sessionStorage.setItem(OIDC_TRANSACTION_KEY, JSON.stringify(transaction));
  const challenge = await codeChallenge(transaction.codeVerifier);
  window.location.assign(buildAuthorizationUrl(config, transaction, challenge, options));
}

export async function completeOidcCallback(config: UiAuthConfig, callbackUrl: string): Promise<string> {
  const transaction = readTransaction();
  const code = callbackCodeFromUrl(callbackUrl, transaction);
  const body = new URLSearchParams({
    client_id: config.client_id,
    code,
    code_verifier: transaction.codeVerifier,
    grant_type: "authorization_code",
    redirect_uri: config.redirect_uri,
  });
  const response = await fetch(tokenEndpoint(config), {
    body,
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    method: "POST",
  });
  const payload = (await response.json()) as TokenResponse;
  if (!response.ok || !payload.access_token) {
    throw new Error(payload.error_description || payload.error || "OIDC token exchange failed");
  }
  setAccessToken(payload.access_token);
  window.sessionStorage.removeItem(OIDC_TRANSACTION_KEY);
  return transaction.returnTo;
}

export async function completeDemoLogin(path: string, loginHint: string): Promise<void> {
  const response = await fetch(apiUrl(path), {
    body: JSON.stringify({ login_hint: loginHint }),
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    method: "POST",
  });
  const payload = (await response.json()) as DemoLoginResponse;
  if (!response.ok || !payload.access_token) {
    throw new Error("Demo login failed");
  }
  setAccessToken(payload.access_token);
}

export function clearOidcSession(config: UiAuthConfig | null): void {
  setAccessToken("");
  window.sessionStorage.removeItem(OIDC_TRANSACTION_KEY);
  if (config?.post_logout_redirect_uri) {
    const url = new URL(endSessionEndpoint(config));
    url.searchParams.set("client_id", config.client_id);
    url.searchParams.set("post_logout_redirect_uri", config.post_logout_redirect_uri);
    window.location.assign(url);
  }
}

export function buildAuthorizationUrl(
  config: UiAuthConfig,
  transaction: PkceTransaction,
  challenge: string,
  options: { loginHint?: string } = {},
): URL {
  const url = new URL(authorizationEndpoint(config));
  url.searchParams.set("client_id", config.client_id);
  url.searchParams.set("code_challenge", challenge);
  url.searchParams.set("code_challenge_method", "S256");
  url.searchParams.set("redirect_uri", config.redirect_uri);
  url.searchParams.set("response_type", "code");
  url.searchParams.set("scope", config.scope || "openid profile");
  url.searchParams.set("state", transaction.state);
  if (options.loginHint) {
    url.searchParams.set("login_hint", options.loginHint);
  }
  return url;
}

export function callbackCodeFromUrl(callbackUrl: string, transaction: PkceTransaction): string {
  const url = new URL(callbackUrl);
  const state = url.searchParams.get("state");
  if (state !== transaction.state) {
    throw new Error("OIDC callback state did not match");
  }
  const code = url.searchParams.get("code");
  if (!code) {
    throw new Error("OIDC callback did not include an authorization code");
  }
  return code;
}

export function tokenEndpoint(config: UiAuthConfig): URL {
  return oidcEndpoint(config, "token");
}

function authorizationEndpoint(config: UiAuthConfig): URL {
  return oidcEndpoint(config, "auth");
}

function endSessionEndpoint(config: UiAuthConfig): URL {
  return oidcEndpoint(config, "logout");
}

function oidcEndpoint(config: UiAuthConfig, path: string): URL {
  return new URL(`${trimTrailingSlash(config.authority)}/protocol/openid-connect/${path}`);
}

async function createPkceTransaction(returnTo: string): Promise<PkceTransaction> {
  return {
    codeVerifier: randomUrlSafeText(64),
    returnTo: returnTo.startsWith("/") ? returnTo : "/assets",
    state: randomUrlSafeText(32),
  };
}

async function codeChallenge(verifier: string): Promise<string> {
  const bytes = new TextEncoder().encode(verifier);
  const digest = await window.crypto.subtle.digest("SHA-256", bytes);
  return base64Url(new Uint8Array(digest));
}

function readTransaction(): PkceTransaction {
  const raw = window.sessionStorage.getItem(OIDC_TRANSACTION_KEY);
  if (!raw) {
    throw new Error("OIDC login transaction was not found");
  }
  const value = JSON.parse(raw) as Partial<PkceTransaction>;
  if (!value.codeVerifier || !value.returnTo || !value.state) {
    throw new Error("OIDC login transaction was incomplete");
  }
  return {
    codeVerifier: value.codeVerifier,
    returnTo: value.returnTo,
    state: value.state,
  };
}

function randomUrlSafeText(byteLength: number): string {
  const bytes = new Uint8Array(byteLength);
  window.crypto.getRandomValues(bytes);
  return base64Url(bytes);
}

function base64Url(bytes: Uint8Array): string {
  let raw = "";
  bytes.forEach((byte) => {
    raw += String.fromCharCode(byte);
  });
  return window.btoa(raw).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

function trimTrailingSlash(value: string): string {
  return value.replace(/\/+$/, "");
}
