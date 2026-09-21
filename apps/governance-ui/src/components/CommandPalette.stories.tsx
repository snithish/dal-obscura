import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fn, userEvent, within } from "storybook/test";
import type { Asset } from "../api";
import { CommandPalette } from "./CommandPalette";

const assets: Asset[] = [
  {
    id: "asset-orders",
    catalog: "analytics",
    name: "orders",
    backend: "iceberg",
    table_identifier: "analytics.orders",
    owners: ["data-platform"],
    schema_fields: [],
  },
  {
    id: "asset-customers",
    catalog: "analytics",
    name: "customers",
    backend: "iceberg",
    table_identifier: "analytics.customers",
    owners: ["data-platform"],
    schema_fields: [],
  },
];

const meta = {
  title: "Components/Command palette",
  component: CommandPalette,
  tags: ["autodocs"],
  args: {
    opened: true,
    query: "",
    commands: ["assets", "changes", "activity", "connections", "settings", "help"],
    assets,
    onQueryChange: fn(),
    onCommand: fn(),
    onAsset: fn(),
    onClose: fn(),
  },
  parameters: {
    docs: {
      description: {
        component: "The production command palette searches authorized destinations and asset identity fields using deterministic fixtures. It never loads inventory or calls an API from Storybook.",
      },
    },
  },
} satisfies Meta<typeof CommandPalette>;
export default meta;
type Story = StoryObj<typeof meta>;

export const SearchScopesAssets: Story = {
  args: { query: "orders" },
  play: async ({ canvasElement }) => {
    const body = within(canvasElement.ownerDocument.body);
    await expect(body.getByRole("option", { name: "orders · analytics" })).toBeVisible();
    await expect(body.queryByRole("option", { name: "customers · analytics" })).not.toBeInTheDocument();
  },
};

export const SelectAsset: Story = {
  play: async ({ canvasElement, args }) => {
    const body = within(canvasElement.ownerDocument.body);
    await userEvent.click(body.getByRole("option", { name: "orders · analytics" }));
    await expect(args.onAsset).toHaveBeenCalledWith("asset-orders");
  },
};
