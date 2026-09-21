import { QueryClient } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, within } from "storybook/test";
import type { Asset, AssetAccess, Mask, PolicyRule, PolicyVersionDetail, SchemaNode, Session } from "../api";
import { AssetWorkspace } from "./AssetWorkspace";

const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

const emailNode: SchemaNode = {
  field_id: 2,
  name: "email",
  path: { version: 1, segments: [{ kind: "field", name: "customer", field_id: 1 }, { kind: "field", name: "email", field_id: 2 }] },
  human_path: "customer.email",
  type: "string",
  nullable: true,
  kind: "scalar",
};

const schema: Asset["schema"] = {
  asset_id: "asset-orders",
  catalog: "analytics",
  target: "orders",
  schema_version: 3,
  schema_fingerprint: "sha256:nested-orders",
  stable_field_ids: true,
  supported_masks: ["null", "redact", "hash", "email", "keep_last", "default"],
  fields: [
    {
      field_id: 1,
      name: "customer",
      path: { version: 1, segments: [{ kind: "field", name: "customer", field_id: 1 }] },
      human_path: "customer",
      type: "struct",
      nullable: false,
      kind: "struct",
      children: [emailNode],
    },
    {
      field_id: 3,
      name: "region",
      path: { version: 1, segments: [{ kind: "field", name: "region", field_id: 3 }] },
      human_path: "region",
      type: "string",
      nullable: false,
      kind: "scalar",
    },
  ],
};

const asset: Asset = {
  id: "asset-orders",
  revision: 8,
  catalog: "analytics",
  name: "orders",
  backend: "iceberg",
  table_identifier: "analytics.orders",
  owners: ["data-platform"],
  schema_fields: [
    { name: "customer", type: "struct", nullable: false },
    { name: "region", type: "string", nullable: false },
  ],
  schema,
  policy_status: "configured",
  draft_status: "draft",
  active_policy_version: 4,
};

const rule: PolicyRule = {
  ordinal: 1,
  principals: ["group:analysts"],
  columns: ["customer.email", "region"],
  masks: { "customer.email": { type: "email" } },
  row_filter: "region = 'EU'",
  effect: "allow",
  when: { tenant: "analytics" },
};

const publishedDetail: PolicyVersionDetail = {
  asset_id: asset.id,
  policy_version: 4,
  rules: [{ ...rule, columns: ["region"], masks: {}, row_filter: null }],
};
const historyQueryClient = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
historyQueryClient.setQueryData(["asset", "storybook|admin", asset.id, "version", 4], publishedDetail);

const access: AssetAccess = {
  asset_id: asset.id,
  principal: "user:admin@example.com",
  issuer: "https://idp.example",
  capabilities: [
    { capability: "read", allowed: true, reasons: ["platform administrator"] },
    { capability: "edit", allowed: true, reasons: ["asset owner"] },
    { capability: "publish", allowed: true, reasons: ["platform administrator"] },
    { capability: "grant", allowed: true, reasons: ["platform administrator"] },
  ],
};

const session: Session = {
  principal: "user:admin@example.com",
  groups: ["analysts", "data-platform"],
  platform_admin: true,
  capabilities: ["manage_assets", "publish"],
  issuer: "https://idp.example",
};

const noop = () => undefined;

const meta = {
  title: "Workflows/Asset policy workspace",
  component: AssetWorkspace,
  tags: ["autodocs"],
  args: {
    initialTab: "policy" as const,
    assets: [asset],
    asset,
    access,
    history: [],
    grants: [],
    onAsset: noop,
    assetSearch: "",
    assetHasMore: false,
    assetInventoryLoading: false,
    onSearch: noop,
    onLoadMore: noop,
    rules: [rule],
    activeRule: rule,
    activeRevision: 1,
    selectedRule: 0,
    onRule: noop,
    onMoveRule: noop,
    selectedField: "customer.email",
    onField: noop,
    selectedMask: { type: "email" } as Mask,
    effectiveFields: new Set(["customer.email", "region"]),
    saveState: "saved" as const,
    notice: "Draft policy loaded from the active workspace.",
    onToggleField: noop,
    onMask: noop,
    onUpdateRule: noop,
    onAddRule: noop,
    onRemoveRule: noop,
    onDuplicateRule: noop,
    onUndo: noop,
    onRedo: noop,
    canUndo: false,
    canRedo: false,
    onSave: noop,
    onPreview: noop,
    onReview: noop,
    previewPrincipal: "user:analyst@example.com",
    previewGroups: "analysts",
    previewClaims: '{"tenant":"analytics"}',
    onPreviewPrincipal: noop,
    onPreviewGroups: noop,
    onPreviewClaims: noop,
    onPublish: noop,
    publishing: false,
    onRestore: noop,
    preview: null,
    session,
    onReloadAccess: noop,
    reviewOnly: false,
    draftId: "draft-orders-4",
    queryClient,
    sessionScope: "storybook|admin",
  },
  parameters: {
    docs: {
      description: {
        component: "The production policy workspace with a nested struct schema, field-level access, DuckDB row filtering, masking, a reviewable draft, and an immutable-history semantic diff. The diff compares saved rules by ordinal while normalizing set-like fields; it does not evaluate policy or replace server review. It uses only deterministic fixtures; version loading is seeded and mutations stay out of Storybook.",
      },
    },
  },
} satisfies Meta<typeof AssetWorkspace>;
export default meta;
type Story = StoryObj<typeof meta>;

export const NestedPolicyDraft: Story = {
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("heading", { name: "Fields & access" })).toBeVisible();
    await expect(canvas.getByRole("button", { name: "Select customer.email" })).toBeVisible();
    await expect(canvas.getByLabelText("DuckDB row restriction")).toHaveValue("region = 'EU'");
    await expect(canvas.getByLabelText("Mask for customer.email")).toHaveValue("email");
    await expect(canvas.getByRole("button", { name: "Save draft" })).toBeEnabled();
  },
};

export const DarkNestedPolicy: Story = {
  ...NestedPolicyDraft,
  globals: { theme: "dark" },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("heading", { name: "Fields & access" })).toBeVisible();
    await expect(canvas.getByRole("button", { name: "Select customer.email" })).toBeVisible();
    await expect(canvas.getByRole("button", { name: "Save draft" })).toBeEnabled();
  },
};

export const AccessAndGrants: Story = {
  args: {
    initialTab: "access",
    grants: [{ principal: "group:analysts", capability: "read" }],
  },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("heading", { name: "Owners and delegated capabilities" })).toBeVisible();
    await expect(canvas.getByRole("heading", { name: "Your effective capabilities" })).toBeVisible();
    await expect(canvas.getByLabelText("Owner principals")).toHaveValue("data-platform");
    await expect(canvas.getByLabelText("Grant principal 1")).toHaveValue("group:analysts");
    await expect(canvas.getByLabelText("Grant capability 1")).toHaveValue("read");
    await expect(canvas.getByRole("button", { name: "Save capabilities" })).toBeEnabled();
  },
};

export const PreventLastOwnerRemoval: Story = {
  args: { initialTab: "access" },
  play: async ({ canvas }) => {
    const owners = canvas.getByLabelText("Owner principals");
    await userEvent.clear(owners);
    await userEvent.click(canvas.getByRole("button", { name: "Save owners" }));
    const statuses = canvas.getAllByRole("status");
    await expect(statuses[statuses.length - 1]).toHaveTextContent("At least one owner is required");
  },
};

export const SemanticHistoryDiff: Story = {
  args: {
    initialTab: "history",
    initialVersion: 4,
    queryClient: historyQueryClient,
    history: [{ asset_id: asset.id, asset_name: asset.name, catalog: asset.catalog, target: asset.table_identifier, policy_version: 4, active: true, created_at: "2026-09-21T08:00:00Z" }],
  },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("heading", { name: "Version 4 details" })).toBeVisible();
    await expect(canvas.getByRole("region", { name: "Semantic diff against policy version 4" })).toBeVisible();
    await expect(canvas.getByText("Changed rules")).toBeVisible();
    await expect(within(canvas.getByRole("region", { name: "Semantic diff against policy version 4" })).getByText("1", { exact: true })).toBeVisible();
  },
};

export const DelegatedGrantActor: Story = {
  args: {
    initialTab: "access",
    session: { ...session, platform_admin: false, capabilities: ["read"] },
    access: {
      ...access,
      capabilities: access.capabilities.map((item) => item.capability === "grant" ? { ...item, allowed: true, reasons: ["delegated grant capability"] } : { ...item, allowed: item.capability === "read", reasons: item.capability === "read" ? ["delegated read capability"] : [] }),
    },
    grants: [{ principal: "group:analysts", capability: "read" }],
  },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("delegated grant capability")).toBeVisible();
    await expect(canvas.getByLabelText("Owner principals")).toBeDisabled();
    await expect(canvas.getByRole("button", { name: "Save owners" })).toBeDisabled();
    await expect(canvas.getByLabelText("Grant principal 1")).toBeEnabled();
    await expect(canvas.getByRole("button", { name: "Save capabilities" })).toBeEnabled();
  },
};

export const DenyAllDraft: Story = {
  args: {
    rules: [],
    activeRule: undefined,
    selectedField: "",
    selectedMask: undefined,
    effectiveFields: new Set(),
    reviewToken: undefined,
  },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("No draft rules")).toBeVisible();
    await expect(canvas.getByText(/This is an intentional deny-all policy/)).toBeVisible();
    await expect(canvas.getByRole("button", { name: "Save deny-all draft" })).toBeEnabled();
    await expect(canvas.getByRole("button", { name: "Publish reviewed deny-all" })).toBeDisabled();
    await expect(canvas.getByRole("button", { name: "Add first rule" })).toBeEnabled();
  },
};

export const ReviewOnlyPublisher: Story = {
  args: {
    reviewOnly: true,
    reviewToken: "review-token",
  },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("button", { name: "Save draft" })).toBeDisabled();
    await expect(canvas.getByRole("button", { name: "Publish reviewed draft" })).toBeEnabled();
    await expect(canvas.getByLabelText("Find governed asset")).toBeDisabled();
    await expect(canvas.getByRole("button", { name: "Review current" })).toBeEnabled();
  },
};

export const SchemaUnavailable: Story = {
  args: { asset: { ...asset, schema: undefined }, selectedField: "", rules: [], activeRule: undefined, selectedMask: undefined },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("Authoritative schema unavailable")).toBeVisible();
    await expect(canvas.queryByRole("tree")).not.toBeInTheDocument();
    await expect(canvas.getByText("No authoritative fields available")).toBeVisible();
  },
};

export const ClipboardFailure: Story = {
  play: async ({ canvas }) => {
    const clipboard = navigator.clipboard;
    const originalWrite = clipboard?.writeText;
    if (!clipboard || !originalWrite) throw new Error("Clipboard API is unavailable in the Storybook browser");
    Object.defineProperty(clipboard, "writeText", { configurable: true, value: async () => { throw new Error("blocked"); } });
    try {
      await userEvent.click(canvas.getByRole("button", { name: "Copy review link" }));
      await expect(canvas.getByRole("alert")).toHaveTextContent("Clipboard access is unavailable");
      const link = canvas.getByLabelText("Review link") as HTMLInputElement;
      await expect(link.value).toContain("draft-orders-4");
    } finally {
      Object.defineProperty(clipboard, "writeText", { configurable: true, value: originalWrite });
    }
  },
};

export const ConsumerCopyFailure: Story = {
  args: { initialTab: "consumers" },
  play: async ({ canvas }) => {
    const clipboard = navigator.clipboard;
    const originalWrite = clipboard?.writeText;
    if (!clipboard || !originalWrite) throw new Error("Clipboard API is unavailable in the Storybook browser");
    Object.defineProperty(clipboard, "writeText", { configurable: true, value: async () => { throw new Error("blocked"); } });
    try {
      await userEvent.click(canvas.getAllByRole("button", { name: "Copy" })[0]);
      await expect(canvas.getAllByRole("status")[1]).toHaveTextContent("Clipboard access is unavailable");
      await expect(canvas.getAllByRole("button", { name: "Retry copy" })[0]).toBeVisible();
      await expect(canvas.getAllByText(/DAL_OBSCURA_FLIGHT_URI/)[0]).toBeVisible();
    } finally {
      Object.defineProperty(clipboard, "writeText", { configurable: true, value: originalWrite });
    }
  },
};
