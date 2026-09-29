import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fn, userEvent } from "storybook/test";
import type { PolicyRule, SchemaNode } from "../api";
import { authoritativeColumnOptions } from "../policy_editor";
import { BulkMaskEditor } from "./BulkMaskEditor";
import { ColumnMultiSelect } from "./ColumnMultiSelect";

const field = (name: string, id: number): SchemaNode => ({
  field_id: id, name, human_path: name, type: "string", nullable: true, kind: "scalar",
  path: { version: 1, segments: [{ kind: "field", name, field_id: id }] },
});
const fields = [field("email", 1), field("phone", 2), field("country", 3)];
const options = authoritativeColumnOptions(fields);
const rule: PolicyRule = { ordinal: 1, effect: "allow", principals: ["group:analysts"], columns: ["email", "phone"], masks: { email: { type: "hash" }, phone: { type: "email" } }, row_filter: null };

const meta = { title: "Workflows/Policy editor controls", component: ColumnMultiSelect, args: { label: "Allowed columns", options, value: ["email"], onChange: fn() }, parameters: { docs: { description: { component: "Keyboard accessible searchable columns and explicit bulk mask targeting controls." } } } } satisfies Meta<typeof ColumnMultiSelect>;
export default meta;
type Story = StoryObj<typeof meta>;

export const MultiColumnKeyboard: Story = {
  play: async ({ canvas, args }) => {
    const trigger = canvas.getByRole("button", { name: "Allowed columns: 1 selected" });
    await userEvent.click(trigger);
    const search = canvas.getByRole("combobox", { name: "Search allowed columns" });
    await userEvent.type(search, "phone");
    await userEvent.keyboard("{ArrowDown}{Enter}");
    await expect(args.onChange).toHaveBeenCalledWith(["email", "phone"]);
    await userEvent.keyboard("{Escape}");
    await expect(trigger).toHaveFocus();
  },
};

export const StaleSelection: Story = {
  args: { options: authoritativeColumnOptions([fields[0]], ["retired.contact"]), value: ["retired.contact"] },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("retired.contact · unavailable")).toBeVisible();
    await expect(canvas.getByRole("button", { name: "Remove retired.contact" })).toBeEnabled();
  },
};

export const WideSchema: Story = {
  args: {
    options: authoritativeColumnOptions(Array.from({ length: 500 }, (_, index) => field(`field-${index}`, index + 1))),
    value: [],
  },
  play: async ({ canvas }) => {
    await userEvent.click(canvas.getByRole("button", { name: "Allowed columns: 0 selected" }));
    await userEvent.type(canvas.getByRole("combobox", { name: "Search allowed columns" }), "field-24");
    const renderedOptions = canvas.getAllByRole("option").length;
    await expect(renderedOptions).toBeLessThanOrEqual(12);
    await expect(canvas.getByRole("button", { name: /Select search results \(/ })).toBeVisible();
  },
};

export const NarrowLayout: Story = {
  render: (args) => <div style={{ width: 320 }}><ColumnMultiSelect {...args} /></div>,
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("button", { name: "Allowed columns: 1 selected" })).toBeVisible();
    await expect(canvas.getByText("email")).toBeVisible();
  },
};
