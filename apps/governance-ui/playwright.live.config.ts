import { defineConfig } from "@playwright/test";
import standard from "./playwright.config";

/** Real SSO requires the running demo; it is never replaced by synthetic routes. */
export default defineConfig({
  ...standard,
  testIgnore: [],
  testMatch: "**/live-oidc-demo.spec.ts",
  workers: 1,
  retries: 0,
});
