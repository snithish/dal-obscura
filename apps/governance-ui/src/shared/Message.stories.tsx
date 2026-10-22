import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect } from "storybook/test";
import { Message } from "./Message";
const meta = {
  title: "Patterns/Action feedback",
  component: Message,
  args: { tone: "success", children: "Owners updated." },
} satisfies Meta<typeof Message>;
export default meta;
type Story = StoryObj<typeof meta>;
export const Success: Story = {
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("status")).toHaveTextContent("Owners updated.");
  },
};
export const Error: Story = {
  args: { tone: "error", children: "Owner update was rejected. Your edits remain available." },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("alert")).toHaveTextContent("rejected");
  },
};
export const Information: Story = {
  args: { tone: "info", children: "Changes are local. Save policy to apply them." },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("status")).toHaveTextContent("Changes are local");
  },
};

export const DarkError: Story = { args: Error.args, globals: { theme: "dark" } };
