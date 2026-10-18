import type { components } from "../../generated/control_plane";
import { StrictMode, useState } from "react";
import { QueryClient } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor } from "storybook/test";
import { useAssetInventory } from "./useAssetInventory";

function InventoryProbe() {
  const [client] = useState(
    () => new QueryClient({ defaultOptions: { queries: { retry: false } } }),
  );
  const [scope, setScope] = useState("first-login");
  const inventory = useAssetInventory(client, scope, true);
  return (
    <section aria-label="Inventory probe">
      <label>
        Search
        <input
          value={inventory.search}
          onChange={(event) => inventory.searchAssets(event.currentTarget.value)}
        />
      </label>
      <p role="status">
        {inventory.assets.map((asset) => asset.name).join(", ") || "Empty inventory"}
      </p>
      {inventory.error && <p role="alert">{inventory.error}</p>}
      <button onClick={() => setScope("second-login")}>Change login</button>
    </section>
  );
}
const meta = {
  title: "Workflows/Inventory retention",
  render: () => (
    <StrictMode>
      <InventoryProbe />
    </StrictMode>
  ),
  beforeEach: () => {
    const original = window.fetch;
    window.fetch = async (input) => {
      const url = new URL(String(input), window.location.href);
      if (url.pathname !== "/v1/assets/page")
        throw new Error(`Unexpected synthetic request: ${url}`);
      if (url.searchParams.has("search"))
        return Response.json(
          {
            error: {
              code: "request_rejected",
              message: "Search unavailable",
              request_id: "test-request",
            },
          },
          { status: 503 },
        );
      return Response.json({
        items: [
          {
            id: "orders",
            name: "Orders",
            owners: [],
            catalog: "analytics",
            backend: "iceberg",
            table_identifier: "orders",
            owner_count: 0,
            policy_status: "configured",
            policy_revision: 1,
          },
        ],
        next_cursor: null,
      } satisfies components["schemas"]["AssetInventoryPageResponse"]);
    };
    return () => {
      window.fetch = original;
    };
  },
} satisfies Meta;
export default meta;
type Story = StoryObj<typeof meta>;
export const FailedSearch: Story = {
  play: async ({ canvas }) => {
    await waitFor(() => expect(canvas.getByRole("status")).toHaveTextContent("Orders"));
    await userEvent.type(canvas.getByLabelText("Search"), "missing");
    await waitFor(() =>
      expect(canvas.getByRole("alert")).toHaveTextContent("temporarily unavailable"),
    );
    await expect(canvas.getByRole("status")).toHaveTextContent("Orders");
    window.fetch = async () =>
      Response.json(
        { error: { code: "request_rejected", message: "Unavailable", request_id: "test-request" } },
        { status: 503 },
      );
    await userEvent.click(canvas.getByRole("button", { name: "Change login" }));
    await waitFor(() => expect(canvas.getByRole("status")).toHaveTextContent("Empty inventory"));
    await waitFor(() =>
      expect(canvas.getByRole("alert")).toHaveTextContent("temporarily unavailable"),
    );
  },
};
