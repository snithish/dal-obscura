import { QueryClient } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent } from "storybook/test";
import type { Catalog, PluginDescriptor, PluginPair, PluginState } from "../api";
import { ConnectionsView } from "./ConnectionsView";

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
const diagnosticQueryClient = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
diagnosticQueryClient.setQueryData(
  ["management", "storybook|admin", "connections", "diagnose", "analytics"],
  { catalog: "analytics", status: "unavailable", message: "TLS handshake failed", checked_at: "2026-09-21T08:00:00Z" },
);

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

const typedCatalogPlugin: PluginDescriptor = {
  ...catalogPlugin,
  plugin_id: "typed-catalog",
  display_name: "Typed catalog adapter",
  config_schema: {
    fields: [
      { name: "uri", type: "string", required: true, secret: false },
      { name: "port", type: "integer", required: true, secret: false },
      { name: "tls", type: "boolean", required: true, secret: false },
      { name: "region", type: "enum", required: false, secret: false, options: ["eu-west-1", "us-east-1"] },
    ],
  },
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

const meta = {
  title: "Workflows/Connections management",
  component: ConnectionsView,
  tags: ["autodocs"],
  args: {
    catalogs,
    plugins: [catalogPlugin],
    pluginStates,
    pluginPairs,
    canManagePlugins: true,
    onReload: () => undefined,
    queryClient,
    sessionScope: "storybook|admin",
  },
  parameters: {
    docs: {
      description: {
        component: "Production connections management with admitted plugin descriptors and lifecycle state. Fixtures contain no credentials and actions that would mutate the control plane are not invoked by the story.",
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
    await expect(canvas.getByRole("button", { name: "Save connection" })).toBeVisible();
  },
};

export const DiagnosticFailure: Story = {
  args: { queryClient: diagnosticQueryClient },
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole("button", { name: "Check connection" }));
    await expect(canvas.getByRole("status")).toHaveTextContent("TLS handshake failed");
  },
};

export const EmptyState: Story = {
  args: { catalogs: [], plugins: [], pluginStates: [], pluginPairs: [], canManagePlugins: false },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("No catalogs configured")).toBeVisible();
    await expect(canvas.getByText("Connect an admitted catalog adapter to begin asset onboarding.")).toBeVisible();
  },
};

export const SecretReferenceEditing: Story = {
  args: {
    catalogs: [{
      ...catalogs[0],
      options: {
        uri: "https://catalog.example",
        password: { secret: "vault/catalog/password", scope: "catalog:analytics" },
      },
    }],
  },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("button", { name: "Edit" })).toBeVisible();
    await canvas.getByRole("button", { name: "Edit" }).click();
    await expect(canvas.getByDisplayValue("https://catalog.example")).toBeVisible();
    const secretField = canvas.getByLabelText(/Password secret reference/);
    await expect(secretField).toHaveAttribute("type", "password");
    await expect(secretField).toHaveValue("vault/catalog/password");
    await expect(canvas.getByText(/Secret references remain deployment-managed/)).toBeVisible();
    await expect(canvas.queryByText("super-secret-password")).not.toBeInTheDocument();
  },
};

export const TypedConfiguration: Story = {
  args: {
    plugins: [typedCatalogPlugin],
    catalogs: [{
      ...catalogs[0],
      plugin_id: "typed-catalog",
      options: { uri: "https://catalog.example", port: 443, tls: true, region: "eu-west-1" },
    }],
  },
  play: async ({ canvas }) => {
    await canvas.getByRole("button", { name: "Edit" }).click();
    await expect(canvas.getByLabelText("Port")).toHaveAttribute("type", "number");
    await expect(canvas.getByLabelText("TLS")).toHaveValue("true");
    await expect(canvas.getByLabelText("Region (optional)")).toHaveValue("eu-west-1");
  },
};

export const InvalidTypedValue: Story = {
  args: {
    plugins: [typedCatalogPlugin],
    catalogs: [{
      ...catalogs[0],
      plugin_id: "typed-catalog",
      options: { uri: "https://catalog.example", port: 443, tls: true, region: "eu-west-1" },
    }],
  },
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole("button", { name: "Edit" }));
    await userEvent.clear(canvas.getByLabelText("Port"));
    await userEvent.type(canvas.getByLabelText("Port"), "1.5");
    await userEvent.click(canvas.getByRole("button", { name: "Save catalog changes" }));
    await expect(canvas.getByText("Port must be a valid integer value.")).toBeVisible();
  },
};

export const ReadOnlyPluginLifecycle: Story = {
  args: { canManagePlugins: false },
  play: async ({ canvas }) => {
    const lifecycle = canvas.getByLabelText("Lifecycle for Iceberg REST catalog");
    await expect(lifecycle).toBeDisabled();
    await expect(canvas.getByRole("button", { name: "Apply" })).toBeDisabled();
  },
};
