import { useRef, useState } from "react";
import { Text, useMantineColorScheme } from "@mantine/core";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, fn, userEvent } from "storybook/test";
import { AppShell, type AppShellProps } from "./AppShell";

function StatefulShell(args: AppShellProps) {
  const mobileNavTrigger = useRef<HTMLButtonElement>(null);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const { colorScheme, setColorScheme } = useMantineColorScheme();
  return <AppShell {...args} mobileNavTrigger={mobileNavTrigger} mobileNavOpen={mobileNavOpen}
    onMobileNavOpen={() => setMobileNavOpen(true)} onMobileNavClose={() => setMobileNavOpen(false)}
    theme={colorScheme === "auto" ? "system" : colorScheme}
    onThemeChange={(value) => setColorScheme(value === "system" ? "auto" : value)} />;
}

const meta = {
  title: "Components/Workbench shell", component: AppShell, tags: ["autodocs"],
  render: StatefulShell,
  parameters: { layout: "fullscreen", docs: { description: { component: "Real workbench shell with synthetic identity. Primary navigation exposes unavailable destinations with an explanation; server authorization remains authoritative. Tab visits enabled controls; mobile navigation uses Mantine Drawer focus handling and Escape. Theme controls share production tokens and fonts. Admin capability changes which destinations can be selected, never backend grants." } } },
  args: {
    page: "assets", workspace: "ready", logoutPending: false,
    mobileNavOpen: false, mobileNavTrigger: { current: null }, theme: "system",
    onMobileNavOpen: fn(), onMobileNavClose: fn(), onThemeChange: fn(),
    session: { principal: "alex@example.invalid", groups: [], platform_admin: false, capabilities: [] },
    onNavigate: fn(), onLogout: fn(), children: <Text>Choose an asset to inspect its access policy.</Text>,
  },
} satisfies Meta<typeof AppShell>;
export default meta;
type Story = StoryObj<typeof meta>;

export const Reader: Story = {
  play: async ({ canvas, args }) => {
    const settings = canvas.getByRole("button", { name: "Settings" });
    await expect(settings).toHaveAttribute("aria-disabled", "true");
    settings.click(); // Programmatic activation must also respect the capability guard.
    await expect(args.onNavigate).not.toHaveBeenCalled();
    await userEvent.click(canvas.getByRole("button", { name: "Activity" }));
    await expect(args.onNavigate).toHaveBeenCalledWith("activity");
  },
};
export const Administrator: Story = {
  args: { session: { principal: "admin@example.invalid", groups: [], platform_admin: true, capabilities: ["workspace:admin"] } },
};
export const DarkReader: Story = { ...Reader, globals: { theme: "dark" } };
