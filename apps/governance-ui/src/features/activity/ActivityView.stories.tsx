import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fn, userEvent } from "storybook/test";
import type { AuditEvent, WorkspaceObservations, WorkspaceSummary } from "../../data/api";
import type { ActivityData } from "./useActivityQueries";
import { ActivityView } from "./ActivityView";

const summary: WorkspaceSummary = {
  catalog_count: 1,
  asset_count: 18,
  unowned_asset_count: 0,
  missing_policy_count: 2,
  runtime_configured: true,
  enabled_auth_provider_count: 1,
};

const observations: WorkspaceObservations = {
  available: true,
  observed_at: "2026-09-21T08:00:00Z",
  source: "control-plane",
  data_plane: { status: "ready", reason: "Flight health probe succeeded" },
};

const events: AuditEvent[] = [{
  id: "audit-1",
  actor: "user:admin@example.com",
  action: "asset.policy.replace",
  resource_type: "asset",
  resource_id: "asset-orders",
  outcome: "success",
  details: {},
  correlation_id: "req-17",
  created_at: "2026-09-21T08:00:00Z",
}];

const data: ActivityData = { summary, observations, events, eventsNextCursor: null };

const meta = {
  title: "Workflows/Activity and audit",
  component: ActivityView,
  tags: ["autodocs"],
  args: {
    data,
    loading: false,
    error: "",
    onReload: () => undefined,
    filters: {},
    onFiltersChange: () => undefined,
  },
  parameters: {
    docs: {
      description: {
        component: "Production activity and audit workflow with server-provided summary counts, measured data-plane observations, exact audit records, and filter controls. Fixtures are redacted and deterministic.",
      },
    },
  },
} satisfies Meta<typeof ActivityView>;
export default meta;
type Story = StoryObj<typeof meta>;

export const ConnectedActivity: Story = {
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("heading", { name: "Workspace status" })).toBeVisible();
    await expect(canvas.getByText("Control plane connected")).toBeVisible();
    await expect(canvas.getByText("asset.policy.replace")).toBeVisible();
    await expect(canvas.getByRole("button", { name: "Apply filters" })).toBeVisible();
  },
};

export const LoadingState: Story = {
  args: { loading: true, data: {} },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("heading", { name: "Loading activity" })).toBeVisible();
    await expect(canvas.getByText("Checking the current workspace state and your capabilities.")).toBeVisible();
  },
};

export const ErrorWithRetry: Story = {
  args: { error: "The control plane is temporarily unavailable.", data: {}, onReload: fn() },
  play: async ({ canvas, args }) => {
    await expect(canvas.getByRole("alert")).toHaveTextContent("temporarily unavailable");
    await userEvent.click(canvas.getByRole("button", { name: "Retry" }));
    await expect(args.onReload).toHaveBeenCalledOnce();
  },
};

export const RefreshFailureRetainsState: Story = {
  args: { error: "The control plane is temporarily unavailable.", data, onReload: fn() },
  play: async ({ canvas, args }) => {
    await expect(canvas.getByRole("heading", { name: "Workspace status" })).toBeVisible();
    await expect(canvas.getByText("asset.policy.replace")).toBeVisible();
    await expect(canvas.getByRole("alert")).toHaveTextContent("Workspace refresh failed");
    await userEvent.click(canvas.getByRole("button", { name: "Retry refresh" }));
    await expect(args.onReload).toHaveBeenCalledOnce();
  },
};

export const UnknownActivity: Story = {
  args: { data: { events: [], eventsNextCursor: null } },
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
    await expect(canvas.getByRole("option", { name: "Catalog" })).toBeVisible();
    await expect(canvas.getByRole("option", { name: "Plugin" })).toBeVisible();
    await userEvent.click(canvas.getByRole("button", { name: "Apply filters" }));
    await expect(args.onFiltersChange).toHaveBeenCalledWith({ actor: "user:admin@example.com" });
    await expect(canvas.getByRole("button", { name: "Load more activity" })).toBeVisible();
    await userEvent.click(canvas.getByRole("button", { name: "Load more activity" }));
    await expect(args.onLoadMore).toHaveBeenCalledTimes(1);
  },
};
