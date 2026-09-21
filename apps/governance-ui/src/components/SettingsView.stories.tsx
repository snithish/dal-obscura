import { QueryClient } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent } from "storybook/test";
import type { AuthProvider, RuntimeSettings, WorkspacePublication } from "../api";
import { SettingsView } from "./SettingsView";

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

const runtime: RuntimeSettings = {
  ticket_ttl_seconds: 900,
  max_tickets: 64,
  max_ticket_exchanges: 2,
  path_rules: [{ root: "s3://warehouse/curated" }],
  revision: 7,
};

const providers: AuthProvider[] = [{
  id: "provider-main",
  ordinal: 1,
  module: "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider",
  args: {
    issuer: "https://idp.example",
    subject_claim: "sub",
    group_claims: ["groups"],
    attribute_claims: { tenant: "tenant.id" },
    algorithms: ["RS256"],
    leeway_seconds: 30,
    jwks_refresh_interval_seconds: 300,
    max_jwks_keys: 32,
  },
  enabled: true,
  revision: 3,
}];

const publications: WorkspacePublication[] = [{
  id: "generation-0004",
  schema_version: 1,
  status: "active",
  manifest_hash: "b".repeat(64),
  active: true,
  asset_count: 18,
  catalog_count: 1,
  created_at: "2026-09-21T08:00:00Z",
}];

const meta = {
  title: "Workflows/Runtime and identity settings",
  component: SettingsView,
  tags: ["autodocs"],
  args: {
    runtime,
    providers,
    providerRevision: 3,
    publications,
    onReload: () => undefined,
    queryClient,
    sessionScope: "storybook|admin",
  },
  parameters: {
    docs: {
      description: {
        component: "Production runtime and identity settings with deployment-managed values represented as safe references. The story is fixture-only and does not save or contact an identity provider.",
      },
    },
  },
} satisfies Meta<typeof SettingsView>;
export default meta;
type Story = StoryObj<typeof meta>;

export const ConfiguredWorkspace: Story = {
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("heading", { name: "Runtime and identity" })).toBeVisible();
    await expect(canvas.getByLabelText("Ticket TTL (seconds)")).toHaveValue(900);
    await expect(canvas.getByRole("heading", { name: "Authentication providers" })).toBeVisible();
    await expect(canvas.getByDisplayValue("https://idp.example")).toBeVisible();
    await expect(canvas.getByText("generation-0")).toBeVisible();
  },
};

export const NoProvidersConfigured: Story = {
  args: { runtime: null, providers: [], providerRevision: undefined, publications: [] },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("No identity providers configured.")).toBeVisible();
    await expect(canvas.getByRole("button", { name: "Add OIDC provider" })).toBeVisible();
  },
};

export const InvalidAttributeMapping: Story = {
  play: async ({ canvas }) => {
    const field = canvas.getByPlaceholderText("tenant=tenant.id");
    await userEvent.clear(field);
    await userEvent.type(field, "tenant");
    await expect(canvas.getByRole("alert")).toHaveTextContent("Use name=claim.path entries separated by commas.");
    await expect(canvas.getByRole("button", { name: "Save identity providers" })).toBeDisabled();
  },
};

export const InvalidRuntimeLimits: Story = {
  play: async ({ canvas }) => {
    await userEvent.clear(canvas.getByLabelText("Ticket TTL (seconds)"));
    await userEvent.type(canvas.getByLabelText("Ticket TTL (seconds)"), "0");
    await userEvent.click(canvas.getByRole("button", { name: "Save runtime settings" }));
    await expect(canvas.getByRole("status")).toHaveTextContent("positive values");
  },
};
