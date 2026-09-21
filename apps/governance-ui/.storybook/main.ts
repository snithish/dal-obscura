import type { StorybookConfig } from "@storybook/react-vite";

const config: StorybookConfig = {
  stories: ["../src/**/*.stories.tsx", "../designbook/**/*.mdx"],
  addons: ["@storybook/addon-docs", "@storybook/addon-a11y", "@storybook/addon-vitest"],
  framework: { name: "@storybook/react-vite", options: { builder: { viteConfigPath: ".storybook/vite.config.ts" } } },
  core: { disableTelemetry: true },
};
export default config;
