import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fn, userEvent } from "storybook/test";
import { LoginPanel } from "./LoginPanel";

const meta = {
  title: "Workflows/Login",
  component: LoginPanel,
  tags: ["autodocs"],
  args: { showAuth: true, title: "Sign in to your workspace", message: "Use your organization account to manage governed data access." },
  parameters: { docs: { description: { component: "Real application login composition. SSO is the normal entry; bootstrap is only for explicitly enabled disposable local profiles. Tab reaches enabled actions; Enter submits the local form. Error copy retains user input. Uses shared surface, border, text and primary tokens. These synthetic states do not prove identity-provider integration." } } },
} satisfies Meta<typeof LoginPanel>;
export default meta;
type Story = StoryObj<typeof meta>;

export const NotConfigured: Story = {};
export const Unavailable: Story = {
  args: { showAuth: false, title: "Workspace unavailable", message: "Check your connection and try again.", retry: fn() },
  play: async ({ canvas, args }) => {
    await userEvent.click(canvas.getByRole("button", { name: "Retry connection" }));
    await expect(args.retry).toHaveBeenCalledOnce();
  },
};
export const LocalRejected: Story = {
  args: {
    sessionOptions: { bootstrap_enabled: true, oidc: null },
    bootstrapToken: "synthetic-invalid-token", onBootstrapToken: fn(), onBootstrapLogin: fn(),
    authError: "This local token was rejected. Check the configured token and retry.",
  },
  play: async ({ canvas, args }) => {
    await expect(canvas.getByLabelText("Local control-plane token")).toHaveValue("synthetic-invalid-token");
    await expect(canvas.getByRole("alert")).toHaveTextContent("rejected");
    await userEvent.click(canvas.getByRole("button", { name: "Sign in locally" }));
    await expect(args.onBootstrapLogin).toHaveBeenCalledOnce();
  },
};
export const LocalPending: Story = {
  args: { ...LocalRejected.args, authError: undefined, loggingIn: true },
  play: async ({ canvas }) => {
    await expect(canvas.getByRole("button", { name: "Signing in…" })).toBeDisabled();
  },
};
export const DarkFailure: Story = { ...LocalRejected, globals: { theme: "dark" } };
