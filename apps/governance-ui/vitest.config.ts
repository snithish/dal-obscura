import { defineConfig } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";
import { storybookTest } from "@storybook/addon-vitest/vitest-plugin";

export default defineConfig({
  optimizeDeps: { include: ["lucide-react", "@tanstack/react-query"] },
  plugins: [storybookTest({ configDir: ".storybook" })],
  test: {
    name: "stories",
    browser: {
      enabled: true,
      provider: playwright({ launchOptions: { executablePath: process.env.DAL_OBSCURA_E2E_EXECUTABLE_PATH } }),
      headless: true,
      instances: [{ browser: "chromium" }],
    },
  },
});
