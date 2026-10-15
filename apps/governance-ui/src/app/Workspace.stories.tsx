import { StrictMode } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor } from "storybook/test";
import type { components } from "../generated/control_plane";
import { SessionBoundary } from "../features/identity/SessionBoundary";
import { Workspace } from "./Workspace";

type ApiSchemas = components["schemas"];
const asset = {
  id: "story-orders",
  catalog: "analytics",
  name: "orders",
  backend: "iceberg",
  table_identifier: "orders",
  owners: ["reviewer"],
  owner_count: 1,
  policy_status: "configured",
  policy_revision: 1,
} satisfies ApiSchemas["AssetInventoryResponse"];
const meta = {
  title: "Workflows/Workspace",
  render: () => (
    <StrictMode>
      <SessionBoundary>
        <Workspace />
      </SessionBoundary>
    </StrictMode>
  ),
  beforeEach: () => {
    const originalFetch = window.fetch;
    const originalUrl = window.location.href;
    window.history.replaceState(null, "", window.location.pathname + "#assets");
    let signedIn = true;
    window.fetch = async (input) => {
      const path = new URL(String(input), window.location.href).pathname;
      if (path === "/v1/logout") {
        signedIn = false;
        return Response.json({ logout_url: null });
      }
      if (path === "/v1/session/bootstrap") {
        signedIn = true;
        return Response.json({ authenticated: true });
      }
      if (path === "/v1/session")
        return Response.json(
          signedIn
            ? ({
                principal: "reviewer",
                groups: [],
                capabilities: [],
                platform_admin: false,
              } satisfies ApiSchemas["SessionResponse"])
            : { detail: "Signed out" },
          { status: signedIn ? 200 : 403 },
        );
      if (path === "/v1/session/options")
        return Response.json({
          bootstrap_enabled: true,
          oidc: null,
        } satisfies ApiSchemas["SessionOptionsResponse"]);
      if (path === "/v1/ui-auth-config")
        return Response.json({ detail: "No OIDC" }, { status: 404 });
      if (path === "/v1/assets/page")
        return Response.json({
          items: [asset],
          next_cursor: null,
        } satisfies ApiSchemas["AssetInventoryPageResponse"]);
      if (path === "/v1/assets/story-orders")
        return Response.json({
          ...asset,
          revision: 1,
          options: {},
          schema_fields: [],
          policy_rules: [],
        } satisfies ApiSchemas["AssetDetailResponse"]);
      if (path === "/v1/assets/story-orders/schema")
        return Response.json({
          asset_id: asset.id,
          catalog: asset.catalog,
          target: asset.name,
          schema_version: 1,
          schema_fingerprint: "story",
          stable_field_ids: true,
          supported_masks: [],
          fields: [],
        } satisfies ApiSchemas["AssetSchemaResponse"]);
      if (path === "/v1/assets/story-orders/grants") return Response.json([]);
      if (path === "/v1/assets/story-orders/access")
        return Response.json({
          asset_id: asset.id,
          principal: "reviewer",
          capabilities: [],
          can_revoke_tokens: false,
        } satisfies ApiSchemas["AssetAccessResponse"]);
      if (path === "/v1/assets/story-orders/identity-attributes") return Response.json([]);
      throw new Error(`Unexpected synthetic request: ${path}`);
    };
    return () => {
      window.fetch = originalFetch;
      window.history.replaceState(null, "", originalUrl);
    };
  },
} satisfies Meta;
export default meta;
type Story = StoryObj<typeof meta>;
export const AssetNavigation: Story = {
  play: async ({ canvas }) => {
    await userEvent.click(await canvas.findByRole("button", { name: /orders/ }));
    await expect(
      await canvas.findByRole("button", { name: "Back to assets" }, { timeout: 5000 }),
    ).toBeVisible();
    await expect(await canvas.findByText("Live revision 1")).toBeVisible();
    await userEvent.click(canvas.getByRole("button", { name: "Back to assets" }));
    await expect(await canvas.findByRole("button", { name: /orders/ })).toBeVisible();
  },
};
export const LogoutLogin: Story = {
  play: async ({ canvas }) => {
    await canvas.findByRole("button", { name: /orders/ });
    await userEvent.click(canvas.getByRole("button", { name: /Sign out/ }));
    await waitFor(() =>
      expect(canvas.queryByRole("button", { name: /orders/ })).not.toBeInTheDocument(),
    );
    await userEvent.type(
      await canvas.findByLabelText("Local control-plane token"),
      "synthetic-token",
    );
    await userEvent.click(canvas.getByRole("button", { name: "Sign in locally" }));
    await expect(await canvas.findByRole("button", { name: /orders/ })).toBeVisible();
  },
};
