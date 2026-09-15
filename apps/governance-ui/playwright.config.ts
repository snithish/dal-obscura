import { defineConfig, devices } from "@playwright/test";

const baseURL = process.env.DAL_OBSCURA_E2E_BASE_URL ?? "http://127.0.0.1:4173";
const useExternalServer = process.env.DAL_OBSCURA_E2E_BASE_URL !== undefined;

export default defineConfig({
  testDir: "e2e",
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: process.env.CI ? 2 : 0,
  reporter: process.env.CI ? "github" : "list",
  use: {
    baseURL,
    trace: "retain-on-failure",
    ...devices["Desktop Chrome"],
    launchOptions: process.env.DAL_OBSCURA_E2E_EXECUTABLE_PATH
      ? { executablePath: process.env.DAL_OBSCURA_E2E_EXECUTABLE_PATH }
      : undefined,
  },
  webServer: useExternalServer
    ? undefined
    : {
        command: "pnpm dev --host 127.0.0.1 --port 4173",
        url: baseURL,
        reuseExistingServer: !process.env.CI,
      },
});
