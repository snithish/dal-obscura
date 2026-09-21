import { QueryClient } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect } from "storybook/test";
import type { Catalog, PluginDescriptor, PluginPair, PluginState, WorkspacePublication } from "../api";
import { ConnectionsView } from "./ConnectionsView";

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

const catalogPlugin: PluginDescriptor = {
  kind: "catalog",
  plugin_id: "iceberg-rest",
  api_version: "1.0",
  config_version: 1,
  distribution: "dal-obscura-iceberg-rest",
  version: "1.0.0",
  display_name: "Iceberg REST catalog",
  capabilities: ["discover", "diagnose"],
  output_formats: ["iceberg"],
  handle_versions: [1],
  config_schema: {
    fields: [
      { name: "uri", type: "string", required: true, secret: false },
      { name: "password", type: "secret_reference", required: false, secret: true },
    ],
  },
  status: "admitted",
};

const catalogs: Catalog[] = [{
  id: "catalog-analytics",
  name: "analytics",
  module: "dal_obscura.catalogs.iceberg_rest",
  plugin_id: "iceberg-rest",
  options: { uri: "https://catalog.example" },
  status: "ready",
  revision: 4,
  discovered_table_count: 24,
  governed_asset_count: 18,
}];

const pluginStates: PluginState[] = [{
  kind: "catalog",
  plugin_id: "iceberg-rest",
  status: "enabled",
  lifecycle: "enabled",
}];

const pluginPairs: PluginPair[] = [{
  catalog_plugin_id: "iceberg-rest",
  format_plugin_id: "iceberg",
  capabilities: ["scan"],
  handle_versions: [1],
  status: "admitted",
}];

const publications: WorkspacePublication[] = [{
  id: "generation-0004",
  schema_version: 1,
  status: "active",
  manifest_hash: "a".repeat(64),
  active: true,
  asset_count: 18,
  catalog_count: 1,
  created_at: "2026-09-21T08:00:00Z",
}];

const meta = {
  title: "Workflows/Connections management",
  component: ConnectionsView,
  tags: ["autodocs"],
  args: {
    catalogs,
    publications,
    plugins: [catalogPlugin],
    pluginStates,
    pluginPairs,
    canActivate: true,
    onReload: () => undefined,
    queryClient,
    sessionScope: "storybook|admin",
  },
  parameters: {
    docs: {
      description: {
        component: "Production connections management with admitted plugin descriptors, lifecycle state, and an active configuration generation. Fixtures contain no credentials and actions that would mutate the control plane are not invoked by the story.",
      },
    },
  },
} satisfies Meta<typeof ConnectionsView>;
export default meta;
type Story = StoryObj<typeof meta>;

export const AdminWorkspace: Story = {
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("heading", { name: "Catalog connections" })).toBeVisible();
    await expect(canvas.getByText("Iceberg REST catalog")).toBeVisible();
    await expect(canvas.getByText("generation-0")).toBeVisible();
    await expect(canvas.getByRole("button", { name: "Save connection" })).toBeVisible();
  },
};

export const EmptyState: Story = {
  args: { catalogs: [], publications: [], plugins: [], pluginStates: [], pluginPairs: [], canActivate: false },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("No catalogs configured")).toBeVisible();
    await expect(canvas.getByText("Connect an admitted catalog adapter to begin asset onboarding.")).toBeVisible();
  },
};
