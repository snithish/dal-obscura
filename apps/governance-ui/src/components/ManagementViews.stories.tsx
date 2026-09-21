import { QueryClient } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fn, userEvent } from "storybook/test";
import type { AuditEvent, Session, WorkspaceObservations, WorkspaceSummary } from "../api";
import type { ManagementData } from "./ManagementViews";
import { ManagementView } from "./ManagementViews";

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

const session: Session = {
  principal: "user:admin@example.com",
  groups: ["data-platform"],
  platform_admin: true,
  capabilities: ["workspace:admin"],
  issuer: "https://idp.example",
};

const summary: WorkspaceSummary = {
  catalog_count: 1,
  asset_count: 18,
  unowned_asset_count: 0,
  missing_policy_count: 2,
  draft_change_count: 3,
  runtime_configured: true,
  enabled_auth_provider_count: 1,
};

const observations: WorkspaceObservations = {
  available: true,
  observed_at: "2026-09-21T08:00:00Z",
  source: "control-plane",
  generation: { cell_id: "cell-4", publication_id: "generation-0004", manifest_hash: "a".repeat(64), status: "active" },
  data_plane: { status: "ready", reason: "active generation is serving" },
};

const events: AuditEvent[] = [{
  id: "audit-1",
  actor: "user:admin@example.com",
  action: "policy.draft.save",
  resource_type: "asset",
  resource_id: "asset-orders",
  outcome: "success",
  details: {},
  correlation_id: "req-17",
  created_at: "2026-09-21T08:00:00Z",
}];

const data: ManagementData = { summary, observations, events, eventsNextCursor: null };

const meta = {
  title: "Workflows/Activity and audit",
  component: ManagementView,
  tags: ["autodocs"],
  args: {
    page: "activity" as const,
    data,
    loading: false,
    error: "",
    onReload: () => undefined,
    filters: {},
    onFiltersChange: () => undefined,
    session,
    queryClient,
    sessionScope: "storybook|admin",
  },
  parameters: {
    docs: {
      description: {
        component: "Production activity and audit workflow with server-provided summary counts, measured data-plane observations, exact audit records, and filter controls. Fixtures are redacted and deterministic.",
      },
    },
  },
} satisfies Meta<typeof ManagementView>;
export default meta;
type Story = StoryObj<typeof meta>;

export const ConnectedActivity: Story = {
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("heading", { name: "Workspace status" })).toBeVisible();
    await expect(canvas.getByText("Control plane connected")).toBeVisible();
    await expect(canvas.getByText("policy.draft.save")).toBeVisible();
    await expect(canvas.getByRole("button", { name: "Apply filters" })).toBeVisible();
  },
};

export const UnknownActivity: Story = {
  args: { data: { events: [], eventsNextCursor: null }, session: null },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("Status unavailable")).toBeVisible();
    await expect(canvas.getByText("No activity yet")).toBeVisible();
  },
};

export const FilteredAuditPagination: Story = {
  args: {
    data: { ...data, eventsNextCursor: "cursor-2" },
    onFiltersChange: fn(),
    onLoadMore: fn(),
  },
  play: async ({ canvas, args }) => {
    await userEvent.type(canvas.getByPlaceholderText("platform:admin"), "user:admin@example.com");
    await userEvent.click(canvas.getByRole("button", { name: "Apply filters" }));
    await expect(args.onFiltersChange).toHaveBeenCalledWith({ actor: "user:admin@example.com" });
    await expect(canvas.getByRole("button", { name: "Load more activity" })).toBeVisible();
    await userEvent.click(canvas.getByRole("button", { name: "Load more activity" }));
    await expect(args.onLoadMore).toHaveBeenCalledTimes(1);
  },
};
