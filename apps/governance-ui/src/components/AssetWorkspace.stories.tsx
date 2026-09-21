import { QueryClient } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent } from "storybook/test";
import type { Asset, AssetAccess, Mask, PolicyRule, SchemaNode, Session } from "../api";
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
        component: "The production policy workspace with a nested struct schema, field-level access, DuckDB row filtering, masking, and a reviewable draft. It uses only deterministic fixtures; version loading and mutations stay out of Storybook.",
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
