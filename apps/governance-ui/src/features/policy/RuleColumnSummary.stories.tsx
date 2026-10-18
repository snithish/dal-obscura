import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect } from "storybook/test";
import { RuleColumnSummary } from "./RuleColumnSummary";
import type { PolicyRule } from "../../data/api";
const rule: PolicyRule = {
  ordinal: 1,
  effect: "allow",
  principals: [],
  columns: ["email", "country"],
  masks: { email: { type: "hash" } },
  row_filter: null,
};
const meta = {
  title: "Patterns/Rule column summary",
  component: RuleColumnSummary,
  args: { rule },
} satisfies Meta<typeof RuleColumnSummary>;
export default meta;
type Story = StoryObj<typeof meta>;
export const Full: Story = {
  play: async ({ canvas }) => {
    await expect(canvas.getByText("email")).toBeVisible();
    await expect(canvas.getByText("hash")).toBeVisible();
  },
};
export const Clamped: Story = {
  args: {
    rule: { ...rule, columns: Array.from({ length: 12 }, (_, i) => `field_${i}`), masks: {} },
  },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("Review all 12 columns")).toBeVisible();
  },
};
export const Aggregated: Story = {
  args: {
    rule: {
      ...rule,
      columns: Array.from({ length: 50 }, (_, i) => `field_${i}`),
      masks: { field_0: { type: "hash" }, field_1: { type: "keep_last", value: 4 } },
    },
  },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("48 no mask")).toBeVisible();
    await expect(canvas.getByText("1 keep_last")).toBeVisible();
    await expect(canvas.getAllByRole("listitem").map((item) => item.textContent?.trim())).toEqual([
      "48 no mask",
      "1 keep_last",
      "1 hash",
    ]);
  },
};
