import { expect, test } from "@playwright/test";
import { signedOutApi } from "./fixtures";

// Component integration only. Real OIDC/Access qualification remains E07.
test.beforeEach(async ({ page }) => signedOutApi(page));

test("built shell has a fresh style nonce and working library controls under CSP", async ({ page }) => {
  const violations: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error" && /content security policy/i.test(message.text())) violations.push(message.text());
  });
  const response = await page.goto("/");
  const firstNonce = await page.locator('meta[name="csp-nonce"]').getAttribute("content");
  expect(firstNonce).toMatch(/^[a-f0-9]{32}$/);
  const policy = response!.headers()["content-security-policy"];
  expect(policy).toContain(`style-src 'self' 'nonce-${firstNonce}'`);
  expect(policy).not.toMatch(/unsafe-inline|unsafe-eval/);
  await expect(page.getByLabel("Local control-plane token")).toBeVisible();
  await page.getByLabel("Local control-plane token").fill("synthetic-only");
  await page.keyboard.press("Control+k");
  await expect(page.getByRole("dialog", { name: "Command palette" })).toBeVisible();
  await expect(page.getByRole("combobox", { name: "Command search" })).toBeFocused();
  await page.keyboard.press("ArrowDown");
  await expect(page.getByRole("combobox", { name: "Command search" })).toHaveAttribute("aria-activedescendant", /.+/);
  await page.keyboard.press("Shift+Tab");
  await expect(page.getByRole("button", { name: "Close command palette" })).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(page.getByRole("combobox", { name: "Command search" })).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(page.getByRole("dialog", { name: "Command palette" })).toHaveCount(0);
  await expect(page.getByLabel("Local control-plane token")).toBeFocused();
  expect(violations).toEqual([]);
  await page.reload();
  expect(await page.locator('meta[name="csp-nonce"]').getAttribute("content")).not.toBe(firstNonce);
});

test("System follows OS changes and explicit choice overrides the OS", async ({ page }) => {
  await page.emulateMedia({ colorScheme: "light" });
  await page.goto("/");
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "light");
  await expect(page.locator(".login-panel")).toHaveCSS("background-color", "rgb(255, 255, 255)");
  await page.emulateMedia({ colorScheme: "dark" });
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "dark");
  await expect(page.locator(".login-panel")).toHaveCSS("background-color", "rgb(23, 31, 44)");
  await page.getByRole("combobox", { name: "Color theme" }).click();
  await page.getByRole("option", { name: "Light theme", exact: true }).click();
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "light");
  await page.reload();
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "light");
});

test("rendered light and dark shell states meet the text contrast threshold", async ({ page }) => {
  const contrastRatio = (foreground: [number, number, number], background: [number, number, number]) => {
    const luminance = (rgb: [number, number, number]) => rgb.reduce((total, channel, index) => {
      const normalized = channel / 255;
      const linear = normalized <= 0.03928 ? normalized / 12.92 : ((normalized + 0.055) / 1.055) ** 2.4;
      return total + linear * [0.2126, 0.7152, 0.0722][index];
    }, 0);
    const foregroundLuminance = luminance(foreground);
    const backgroundLuminance = luminance(background);
    const lighter = Math.max(foregroundLuminance, backgroundLuminance);
    const darker = Math.min(foregroundLuminance, backgroundLuminance);
    return (lighter + 0.05) / (darker + 0.05);
  };
  const parseRgb = (value: string): [number, number, number] => {
    const match = value.match(/rgba?\(([^)]+)\)/);
    expect(match).not.toBeNull();
    const channels = match![1].split(",").slice(0, 3).map((channel) => Number(channel.trim()));
    expect(channels).toHaveLength(3);
    return channels as [number, number, number];
  };

  for (const scheme of ["light", "dark"] as const) {
    await page.emulateMedia({ colorScheme: scheme });
    await page.goto("/");
    const samples = await page.locator(".login-panel h2, .login-panel .mantine-Text-root, .login-panel button, .login-panel input").evaluateAll((elements) => elements.map((element) => {
      const color = getComputedStyle(element).color;
      let current: Element | null = element;
      let background = "rgb(255, 255, 255)";
      while (current) {
        const candidate = getComputedStyle(current).backgroundColor;
        const channels = candidate.match(/rgba?\(([^)]+)\)/)?.[1].split(",").map((channel) => Number(channel.trim()));
        if (channels && (channels.length < 4 || channels[3] > 0)) {
          background = candidate;
          break;
        }
        current = current.parentElement;
      }
      return { role: element.tagName.toLowerCase(), color, background };
    }));
    expect(samples.length).toBeGreaterThan(3);
    for (const sample of samples) {
      const ratio = contrastRatio(parseRgb(sample.color), parseRgb(sample.background));
      expect(ratio, `${scheme} ${sample.role} contrast`).toBeGreaterThanOrEqual(4.5);
    }
  }
});

test("forced-colors mode keeps the signed-out controls discoverable", async ({ page }) => {
  await page.emulateMedia({ forcedColors: "active" });
  await page.goto("/");

  await expect.poll(() => page.evaluate(() => window.matchMedia("(forced-colors: active)").matches)).toBe(true);
  const token = page.getByLabel("Local control-plane token");
  await token.focus();
  await expect(token).toBeFocused();
  await expect.poll(() => token.evaluate((element) => getComputedStyle(element).outlineStyle)).toBe("solid");
  await expect.poll(() => token.evaluate((element) => getComputedStyle(element).outlineWidth)).toBe("2px");
  await expect(page.getByRole("button", { name: "Sign in locally" })).toBeVisible();
});

test("reduced-motion preference disables shell motion", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await page.goto("/");

  await expect.poll(() => page.evaluate(() => window.matchMedia("(prefers-reduced-motion: reduce)").matches)).toBe(true);
  const motion = await page.locator(".login-panel").evaluate((element) => {
    const style = getComputedStyle(element);
    return { animationDuration: style.animationDuration, transitionDuration: style.transitionDuration };
  });
  expect(motion.animationDuration).toMatch(/^(0s|0\.01ms)$/);
  expect(motion.transitionDuration).toMatch(/^(0s|0\.01ms)$/);
});

test("CSP rejects missing and wrong style nonces", async ({ page }) => {
  await page.goto("/");
  const result = await page.evaluate(() => {
    const target = document.createElement("div");
    target.id = "nonce-probe";
    document.body.append(target);
    const before = getComputedStyle(target).color;
    const insert = (nonce?: string) => {
      const style = document.createElement("style");
      if (nonce) style.nonce = nonce;
      style.textContent = "#nonce-probe { color: rgb(1, 2, 3) !important; }";
      document.head.append(style);
      return getComputedStyle(target).color;
    };
    const absent = insert();
    const wrong = insert("00000000000000000000000000000000");
    const accepted = insert(document.querySelector<HTMLMetaElement>('meta[name="csp-nonce"]')!.content);
    return { before, absent, wrong, accepted };
  });
  expect(result.absent).toBe(result.before);
  expect(result.wrong).toBe(result.before);
  expect(result.accepted).toBe("rgb(1, 2, 3)");
});

test("late pre-logout asset responses cannot repopulate a reauthenticated workspace", async ({ page }) => {
  const assetA = "00000000-0000-4000-8000-000000000001";
  const assetB = "00000000-0000-4000-8000-000000000002";
  const identityA = { principal: "alice", groups: ["analysts"], platform_admin: false, capabilities: ["asset:read", "asset:edit"] };
  const identityB = { principal: "alice", groups: ["reviewers"], platform_admin: false, capabilities: ["asset:read"] };
  const inventory = (id: string, name: string) => ({
    id,
    catalog: "demo",
    name,
    backend: "iceberg",
    table_identifier: `demo.${name}`,
    owner_count: 1,
    owners: ["alice"],
    policy_status: "configured",
    draft_status: "published",
    active_policy_version: 1,
    last_published_at: "2026-09-21T00:00:00Z",
  });
  const detail = (id: string, name: string) => ({
    ...inventory(id, name),
    revision: 1,
    options: {},
    policy_rules: [],
    schema_fields: [{ name: "order_id", type: "string", nullable: false }],
  });
  const schema = (id: string, name: string) => ({
    asset_id: id,
    catalog: "demo",
    target: `demo.${name}`,
    schema_version: 1,
    schema_fingerprint: id,
    stable_field_ids: true,
    supported_masks: ["null", "redact", "hash", "email", "keep_last", "default"],
    fields: [{ field_id: 1, name: "order_id", human_path: "order_id", type: "string", nullable: false, kind: "scalar", path: { version: 1, segments: [{ kind: "field", name: "order_id", field_id: 1 }] } }],
  });
  const draft = (id: string) => ({ id: null, asset_id: id, author_principal: "alice", revision: 0, base_policy_version: 1, rules: [], content_hash: "" });
  let sessionCalls = 0;
  let staleSchemaRelease: (() => void) | undefined;
  let staleSchemaStarted: Promise<void> | undefined;
  await page.route("**/v1/**", async (route) => {
    const request = route.request();
    const url = new URL(request.url());
    const path = url.pathname;
    if (path === "/v1/session" && request.method() === "GET") {
      sessionCalls += 1;
      return route.fulfill({ json: sessionCalls === 1 ? identityA : identityB });
    }
    if (path === "/v1/assets/page") {
      const current = sessionCalls === 1 ? inventory(assetA, "alpha-orders") : inventory(assetB, "beta-orders");
      return route.fulfill({ json: { items: [current], next_cursor: null } });
    }
    const match = path.match(/^\/v1\/assets\/([^/]+)(?:\/(.*))?$/);
    if (match) {
      const id = match[1];
      const name = id === assetA ? "alpha-orders" : "beta-orders";
      const suffix = match[2] ?? "";
      if (id === assetA && suffix === "schema" && !staleSchemaStarted) {
        staleSchemaStarted = new Promise((resolve) => { staleSchemaRelease = resolve; });
        await staleSchemaStarted;
      }
      if (suffix === "schema") return route.fulfill({ json: schema(id, name) });
      if (suffix === "grants") return route.fulfill({ json: [] });
      if (suffix === "access") return route.fulfill({ json: { asset_id: id, principal: "alice", issuer: null, capabilities: [{ capability: "read", allowed: true, reasons: ["owner"] }, { capability: "edit", allowed: id === assetA, reasons: id === assetA ? ["owner"] : [] }, { capability: "publish", allowed: false, reasons: [] }, { capability: "grant", allowed: false, reasons: [] }] } });
      if (suffix === "draft") return route.fulfill({ json: draft(id) });
      if (suffix === "policy-versions") return route.fulfill({ json: [] });
      return route.fulfill({ json: detail(id, name) });
    }
    if (path === "/v1/session/options") return route.fulfill({ json: { bootstrap_enabled: true, oidc: null } });
    if (path === "/v1/ui-auth-config") return route.fulfill({ json: { authority: null } });
    if (path === "/v1/session/bootstrap" && request.method() === "POST") return route.fulfill({ json: { authenticated: true } });
    if (path === "/v1/logout" && request.method() === "POST") return route.fulfill({ json: { authenticated: false } });
    return route.fulfill({ status: 404, json: { detail: "fixture route missing" } });
  });

  await page.goto("/#assets");
  await expect(page.getByRole("button", { name: "Sign out" })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Assets" })).toBeVisible();

  await page.getByRole("button", { name: "Sign out" }).click();
  await expect(page.getByRole("heading", { name: "Sign in to your workspace" })).toBeVisible();
  await page.getByLabel("Local control-plane token").fill("synthetic-token");
  await page.getByRole("button", { name: "Sign in locally" }).click();
  await expect(page.getByRole("heading", { name: "beta-orders" })).toBeVisible();
  expect(await page.getByRole("heading", { name: "alpha-orders" }).count()).toBe(0);
  await expect(page.locator(".studio")).toBeVisible();
  const theme = page.getByRole("combobox", { name: "Color theme" });
  await theme.click();
  await page.getByRole("option", { name: "Dark theme", exact: true }).click();
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "dark");
  await expect(page.locator(".studio")).toHaveCSS("background-color", "rgb(23, 31, 44)");
  await theme.click();
  await page.getByRole("option", { name: "Light theme", exact: true }).click();
  await expect(page.locator("html")).toHaveAttribute("data-mantine-color-scheme", "light");
  await expect(page.locator(".studio")).toHaveCSS("background-color", "rgb(255, 255, 255)");
  staleSchemaRelease?.();
  await expect(page.getByRole("heading", { name: "beta-orders" })).toBeVisible();
});
