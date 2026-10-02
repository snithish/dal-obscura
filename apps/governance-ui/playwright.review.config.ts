import { defineConfig } from "@playwright/test";
import standard from "./playwright.config";

/** Explicit visual-review tooling, kept outside the correctness suite. */
export default defineConfig({
  ...standard,
  testDir: "review",
  testIgnore: [],
  fullyParallel: false,
  workers: 1,
});
