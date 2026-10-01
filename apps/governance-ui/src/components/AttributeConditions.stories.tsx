import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fn, userEvent, within } from "storybook/test";
import { AttributeConditions } from "./AttributeConditions";

const providers = [{ ordinal: 1, issuer: "https://sso.example.com", revision: 4, attributes: [
  { key: "department", claim_path: "employee.department", label: "Department", description: "Employee business unit", allowed_values: ["Engineering", "Finance", "Research, EU"] },
  { key: "region", claim_path: "employee.region", label: "Region", description: "Operating region", allowed_values: ["US", "EU"] },
  { key: "project", claim_path: "project", label: "Project", description: "Assigned project", allowed_values: [] },
] }];
const meta = {
  title: "Policy/Identity attribute conditions", component: AttributeConditions,
  args: { providers, disabled: false, value: { department: ["Engineering", "Finance"] }, onChange: fn(), onInvalid: fn() },
  render: (args) => {
    const [value, setValue] = useState(args.value);
    return <AttributeConditions {...args} value={value} onChange={(value) => { setValue(value); args.onChange(value); }} />;
  },
} satisfies Meta<typeof AttributeConditions>;
export default meta;
type Story = StoryObj<typeof meta>;

export const ConfiguredAttributes: Story = {
  play: async ({ canvas }) => {
    await expect(canvas.getByLabelText("Attribute 1")).toHaveValue("Department");
    await expect(canvas.getByText(/3 allowed values/)).toBeVisible();
    await expect(canvas.getByText(/employee.department/)).toBeVisible();
  },
};

export const ChangeKeyClearsOldValues: Story = {
  play: async ({ canvas, args }) => {
    await userEvent.click(canvas.getByLabelText("Attribute 1"));
    await userEvent.click(within(document.body).getByRole("option", { name: "Region" }));
    await expect(args.onChange).toHaveBeenLastCalledWith({ region: [] });
    await expect(canvas.getByRole("alert")).toHaveTextContent("at least one value");
    await userEvent.click(canvas.getByRole("combobox", { name: "Allowed values 1" }));
    await userEvent.click(within(document.body).getByRole("option", { name: /^EU$/ }));
    await expect(args.onChange).toHaveBeenLastCalledWith({ region: ["EU"] });
  },
};

export const PreserveUnknownAttributes: Story = {
  args: { value: { legacy_department: "Finance" } },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("alert")).toHaveTextContent("Existing condition preserved");
    await expect(canvas.getByLabelText("Attribute 1")).toHaveValue("legacy_department");
    await expect(canvas.getByText("Finance", { exact: true })).toBeVisible();
  },
};

export const PreserveRemovedValues: Story = {
  args: { value: { department: ["Retired"] } },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("alert")).toHaveTextContent("Retired");
    await expect(canvas.getByText("Retired", { exact: true })).toBeVisible();
  },
};

export const CommasArePartOfValues: Story = {
  args: { value: { department: "Research, EU" } },
  play: async ({ canvas, args }) => {
    await expect(canvas.getByLabelText("Allowed value 1")).toHaveValue("Research, EU");
    await userEvent.selectOptions(canvas.getByLabelText("Operator 1"), "one_of");
    await expect(args.onChange).toHaveBeenLastCalledWith({ department: ["Research, EU"] });
  },
};
