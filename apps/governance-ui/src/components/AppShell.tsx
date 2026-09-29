import type { ReactNode, RefObject } from "react";
import { AppShell as MantineAppShell, Avatar, Badge, Breadcrumbs, Button, Drawer, Group, Menu, NavLink, Select, Stack, Text, Title } from "@mantine/core";
import type { Session } from "../api";
import { Icon, type IconName } from "./Icon";

export type ShellPage = "assets" | "activity" | "connections" | "settings";
export type ShellTheme = "system" | "light" | "dark";
export type ShellWorkspaceState = "loading" | "ready" | "unavailable";
export type AppShellProps = {
  page: ShellPage;
  session: Session | null;
  workspace: ShellWorkspaceState;
  assetName?: string;
  assetCatalog?: string;
  mobileNavOpen: boolean;
  mobileNavTrigger: RefObject<HTMLButtonElement | null>;
  theme: ShellTheme;
  logoutPending: boolean;
  onNavigate: (page: ShellPage) => void;
  onMobileNavOpen: () => void;
  onMobileNavClose: () => void;
  onThemeChange: (theme: ShellTheme) => void;
  onLogout: () => void;
  onSearch: () => void;
  children: ReactNode;
};
const navigation: Array<{ page: ShellPage; label: string; icon: IconName; admin?: boolean }> = [
  { page: "assets", label: "Assets", icon: "database" },
  { page: "connections", label: "Connections", icon: "plug", admin: true },
  { page: "activity", label: "Activity", icon: "activity" },
  { page: "settings", label: "Settings", icon: "settings", admin: true },
];

export function AppShell(props: AppShellProps) {
  const canAdmin = Boolean(props.session?.capabilities.includes("workspace:admin"));
  const links = <nav aria-label="Primary navigation">
    {navigation.map((item) => <NavLink component="button" key={item.page} label={item.label}
      leftSection={<Icon name={item.icon} />} active={props.page === item.page}
      aria-current={props.page === item.page ? "page" : undefined}
      disabled={!props.session || (item.admin && !canAdmin)}
      aria-disabled={!props.session || (item.admin && !canAdmin)}
      tabIndex={!props.session || (item.admin && !canAdmin) ? -1 : 0}
      title={!props.session ? "Sign in to open this view" : item.admin && !canAdmin ? "Platform administrator capability required" : undefined}
      onClick={() => { if (props.session && (!item.admin || canAdmin)) { props.onNavigate(item.page); props.onMobileNavClose(); } }} />)}
  </nav>;
  return (
    <MantineAppShell header={{ height: { base: 128, sm: 72 } }} navbar={{ width: 224, breakpoint: "sm", collapsed: { mobile: true } }} padding="lg">
      <MantineAppShell.Header className="workbench-header">
        <Group gap="sm" className="workbench-brand">
          <Button ref={props.mobileNavTrigger} variant="subtle" hiddenFrom="sm" aria-label="Open navigation menu"
            aria-expanded={props.mobileNavOpen} onClick={props.onMobileNavOpen}><Icon name="menu" /></Button>
          <a href="#assets" onClick={(event) => { event.preventDefault(); props.onNavigate("assets"); }} className="workbench-logo">OBSCURA <span>Governance</span></a>
          <Button variant="default" aria-label="Search workspace" onClick={props.onSearch} leftSection={<Icon name="search" />}>Search</Button>
        </Group>
        <Group gap="sm" className="workbench-account">
          <Menu position="bottom-end" width={280} withInitialFocusPlaceholder={false}>
            <Menu.Target><Button variant="subtle" aria-label="Account menu" className="workbench-account-button">
              <Avatar size="sm" aria-hidden="true">{props.session?.principal.slice(0, 1).toUpperCase() ?? "?"}</Avatar>
              <Text size="sm" className="workbench-principal" visibleFrom="sm">{props.session?.principal ?? "Not signed in"}</Text>
            </Button></Menu.Target>
            <Menu.Dropdown>
              <Menu.Label>{props.session?.principal ?? "Not signed in"}</Menu.Label>
              <Menu.Label>{window.location.host}</Menu.Label>
              {props.session?.issuer && <Menu.Label>Identity: {props.session.issuer}</Menu.Label>}
              <Menu.Item data-autofocus onClick={props.onSearch} leftSection={<Icon name="search" />}>Search workspace</Menu.Item>
            </Menu.Dropdown>
          </Menu>
          <Select aria-label="Color theme" value={props.theme} allowDeselect={false}
            data={[{ value: "system", label: "System theme" }, { value: "light", label: "Light theme" }, { value: "dark", label: "Dark theme" }]}
            onChange={(value) => { if (value === "system" || value === "light" || value === "dark") props.onThemeChange(value); }} className="workbench-theme" />
          {(props.session || props.logoutPending) && <Button variant="default" size="sm" onClick={props.onLogout} leftSection={<Icon name="log-out" size={16} />}>{props.logoutPending ? "Retry sign out" : "Sign out"}</Button>}
        </Group>
      </MantineAppShell.Header>
      <MantineAppShell.Navbar p="md">
        <Stack justify="space-between" h="100%">
          {links}
          <Stack gap="xs">
            <Badge variant="light" className="workbench-health" data-ready={props.workspace === "ready" || undefined}>{props.workspace === "loading" ? "Checking access" : props.workspace === "ready" ? "Connected" : "Unavailable"}</Badge>
            {props.assetCatalog && <Text size="sm" c="dimmed">{props.assetCatalog}</Text>}
          </Stack>
        </Stack>
      </MantineAppShell.Navbar>
      <Drawer opened={props.mobileNavOpen} onClose={props.onMobileNavClose} title="Navigation" closeButtonProps={{ "aria-label": "Close navigation" }} size="xs">{links}</Drawer>
      <MantineAppShell.Main className="workbench-main">
        <nav aria-label="Breadcrumbs"><Breadcrumbs>
          <a href="#assets" onClick={(event) => { event.preventDefault(); props.onNavigate("assets"); }}>Workspace</a>
          {props.page === "assets" && props.assetName && <a href="#assets" onClick={(event) => { event.preventDefault(); props.onNavigate("assets"); }}>Assets</a>}
          {props.page === "assets" && props.assetCatalog && <Text size="sm">{props.assetCatalog}</Text>}
          <Text size="sm" aria-current="page">{props.page === "assets" ? props.assetName ?? "Assets" : navigation.find((item) => item.page === props.page)?.label}</Text>
        </Breadcrumbs></nav>
        <Text size="sm" c="dimmed" className="workbench-origin">Workspace address: {window.location.host}</Text>
        <Title order={1}>{props.page === "assets" ? props.assetName ?? "Assets" : navigation.find((item) => item.page === props.page)?.label}</Title>
        {props.children}
      </MantineAppShell.Main>
    </MantineAppShell>
  );
}
