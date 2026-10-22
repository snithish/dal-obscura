import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fn, userEvent, waitFor, within } from "storybook/test";
import type { SchemaNode } from "../../../data/api";
import { authoritativeColumnOptions } from "../policy_editor";
import { ColumnMultiSelect } from "./ColumnMultiSelect";

const field = (name: string, id: number): SchemaNode => ({
  field_id: id,
  name,
  human_path: name,
  type: "string",
  nullable: true,
  kind: "scalar",
  path: { version: 1, segments: [{ kind: "field", name, field_id: id }] },
});
const fields = [field("email", 1), field("phone", 2), field("country", 3)];
const options = authoritativeColumnOptions(fields);

const meta = {
  title: "Workflows/Policy column controls",
  component: ColumnMultiSelect,
  args: { label: "Allowed columns", options, value: ["email"], onChange: fn() },
  parameters: {
    docs: {
      description: {
        component:
          "Keyboard accessible searchable columns and explicit bulk mask targeting controls.",
      },
    },
  },
} satisfies Meta<typeof ColumnMultiSelect>;
export default meta;
type Story = StoryObj<typeof meta>;

export const MultiColumnKeyboard: Story = {
  play: async ({ canvas, canvasElement, args }) => {
    const body = within(canvasElement.ownerDocument.body);
    const trigger = canvas.getByRole("button", { name: "Allowed columns: 1 selected" });
    await userEvent.click(trigger);
    const search = await body.findByRole("combobox", { name: "Search allowed columns" });
    await userEvent.type(search, "phone");
    await userEvent.keyboard("{ArrowDown}{Enter}");
    await expect(args.onChange).toHaveBeenCalledWith(["email", "phone"]);
    await userEvent.keyboard("{Escape}");
    // Focus returns on the modal's exit-transition event, not on key dispatch.
    await waitFor(() => expect(trigger).toHaveFocus());
  },
};

export const StaleSelection: Story = {
  args: {
    options: authoritativeColumnOptions([fields[0]], ["retired.contact"]),
    value: ["retired.contact"],
  },
  play: async ({ canvas }) => {
    await expect(canvas.getByText("retired.contact · unavailable")).toBeVisible();
    await expect(canvas.getByRole("button", { name: "Remove retired.contact" })).toBeEnabled();
  },
};

export const WideSchema: Story = {
  args: {
    options: authoritativeColumnOptions(
      Array.from({ length: 500 }, (_, index) => field(`field-${index}`, index + 1)),
    ),
    value: [],
  },
  play: async ({ canvas, canvasElement }) => {
    const body = within(canvasElement.ownerDocument.body);
    await userEvent.click(canvas.getByRole("button", { name: "Allowed columns: 0 selected" }));
    await body.findByRole("combobox", { name: "Search allowed columns" });
    await waitFor(() => expect(body.getByText("500 results", { exact: true })).toBeVisible());
    await expect(body.getAllByRole("option").length).toBeLessThanOrEqual(12);
    await userEvent.type(
      await body.findByRole("combobox", { name: "Search allowed columns" }),
      "field-24",
    );
    await expect(body.getByRole("option", { name: /^field-24\s+string$/ })).toBeVisible();
    await expect(body.getByRole("button", { name: /Select search results \(/ })).toBeVisible();
  },
};

export const NarrowLayout: Story = {
  render: (args) => (
    <div data-testid="narrow-column-controls" style={{ width: 320 }}>
      <ColumnMultiSelect {...args} />
    </div>
  ),
  play: async ({ canvas, canvasElement, args }) => {
    const trigger = canvas.getByRole("button", { name: "Allowed columns: 1 selected" });
    await expect(trigger).toBeVisible();
    await expect(canvas.getByText("email")).toBeVisible();
    const container = canvas.getByTestId("narrow-column-controls");
    await expect(container.scrollWidth).toBeLessThanOrEqual(container.clientWidth);
    const bounds = trigger.getBoundingClientRect();
    const parent = container.getBoundingClientRect();
    await expect(bounds.left).toBeGreaterThanOrEqual(parent.left);
    await expect(bounds.right).toBeLessThanOrEqual(parent.right);
    await userEvent.click(trigger);
    const body = within(canvasElement.ownerDocument.body);
    await userEvent.type(
      await body.findByRole("combobox", { name: "Search allowed columns" }),
      "phone",
    );
    await userEvent.click(body.getByRole("option", { name: /phone/ }));
    await expect(args.onChange).toHaveBeenCalledWith(["email", "phone"]);
    await userEvent.keyboard("{Escape}");
  },
};
