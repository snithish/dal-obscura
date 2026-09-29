import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fn, userEvent } from "storybook/test";
import type { PolicyRule, SchemaNode } from "../api";
import { authoritativeColumnOptions } from "../policy_editor";
import { BulkMaskEditor } from "./BulkMaskEditor";

const field = (name: string, id: number): SchemaNode => ({
  field_id: id, name, human_path: name, type: "string", nullable: true, kind: "scalar",
  path: { version: 1, segments: [{ kind: "field", name, field_id: id }] },
});
const options = authoritativeColumnOptions([field("email", 1), field("phone", 2), field("country", 3)]);
const rule: PolicyRule = { ordinal: 1, effect: "allow", principals: ["group:analysts"], columns: ["email", "phone"], masks: { email: { type: "hash" }, phone: { type: "email" } }, row_filter: null };
const meta = {
  title: "Workflows/Policy editor mask states",
  component: BulkMaskEditor,
  args: { rule, options, supportedMasks: ["null", "redact", "hash", "email", "keep_last", "default"], readOnly: false, onApply: fn() },
} satisfies Meta<typeof BulkMaskEditor>;
export default meta;
type Story = StoryObj<typeof meta>;

export const MixedMasks: Story = {
  play: async ({ canvas }) => { await expect(canvas.getByRole("button", { name: "Edit Hash mask" })).toBeVisible(); await expect(canvas.getByRole("button", { name: "Edit Mask email mask" })).toBeVisible(); },
};

export const InvalidMaskValue: Story = {
  args: { rule: { ...rule, masks: {} } },
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole("button", { name: "Add mask" }));
    await userEvent.selectOptions(canvas.getByLabelText("Mask"), "keep_last");
    await userEvent.clear(canvas.getByLabelText("Characters to retain"));
    await expect(canvas.getByRole("button", { name: "Apply mask" })).toBeDisabled();
  },
};

export const MissingSchema: Story = {
  args: { options: authoritativeColumnOptions(undefined, ["email", "phone"]) },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("button", { name: "Add mask" })).toBeDisabled();
  },
};

export const ReadOnly: Story = {
  args: { readOnly: true },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("button", { name: "Add mask" })).toBeDisabled();
    await expect(canvas.getByRole("button", { name: "Edit Hash mask" })).toBeDisabled();
  },
};
