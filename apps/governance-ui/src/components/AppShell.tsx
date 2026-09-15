import type { ReactNode, RefObject } from "react";
import type { Session } from "../api";
import { Icon, type IconName } from "./Icon";

export type ShellPage = "assets" | "changes" | "activity" | "connections" | "settings";
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
  children: ReactNode;
};

const NAV_ITEMS: ShellPage[] = ["assets", "changes", "activity", "connections", "settings"];

export function AppShell({
  page,
  session,
  workspace,
  assetName,
  assetCatalog,
  mobileNavOpen,
  mobileNavTrigger,
  theme,
  logoutPending,
  onNavigate,
  onMobileNavOpen,
  onMobileNavClose,
  onThemeChange,
  onLogout,
  children,
}: AppShellProps) {
  const canManageWorkspace = Boolean(session?.capabilities.includes("workspace:admin"));

  return (
    <div className="app-shell">
      {mobileNavOpen && (
        <button className="mobile-nav-backdrop" aria-label="Close navigation menu" onClick={onMobileNavClose} />
      )}
      <aside
        id="primary-navigation"
        className={mobileNavOpen ? "sidebar open" : "sidebar"}
        aria-label="Primary navigation"
      >
        <a
          className="brand"
          href="#assets"
          onClick={(event) => {
            event.preventDefault();
            onNavigate("assets");
          }}
        >
          DAL OBSCURA<span>GOVERNANCE</span>
        </a>
        <nav>
          {NAV_ITEMS.map((item) => {
            const requiresWorkspaceAdmin = item === "connections" || item === "settings";
            const disabled = !session || (requiresWorkspaceAdmin && !canManageWorkspace);
            const reason = !session
              ? "Sign in to open this workspace view"
              : requiresWorkspaceAdmin && !canManageWorkspace
                ? "Platform administrator capability required"
                : undefined;
            const icon: IconName = item === "assets"
              ? "database"
              : item === "changes"
                ? "history"
                : item === "activity"
                  ? "activity"
                  : item === "connections"
                    ? "plug"
                    : "settings";
            return (
              <button
                key={item}
                className={page === item ? "nav-item active" : "nav-item"}
                onClick={() => onNavigate(item)}
                disabled={disabled}
                title={reason}
                aria-current={page === item ? "page" : undefined}
              >
                <Icon name={icon} />
                <span>{item}</span>
              </button>
            );
          })}
        </nav>
        <div className="sidebar-foot" role="status" aria-live="polite">
          <span className={`status-dot ${workspace}`} /> Workspace: {workspace === "ready" ? "connected" : "unavailable"}
          <br />
          <small>
            {workspaceLabel(workspace)}
            {assetCatalog ? ` · catalog ${assetCatalog}` : ""}
          </small>
        </div>
      </aside>
      <main>
        <header className="topbar">
          <div className="topbar-title">
            <button
              ref={mobileNavTrigger}
              className="mobile-menu-toggle"
              type="button"
              aria-label="Open navigation menu"
              aria-expanded={mobileNavOpen}
              aria-controls="primary-navigation"
              onClick={onMobileNavOpen}
            >
              <Icon name="menu" />
            </button>
            <div>
              <span className="eyebrow">{page === "assets" ? "ASSET WORKSPACE" : page.toUpperCase()}</span>
              <h1>{page === "assets" ? assetName ?? "Assets" : titleFor(page)}</h1>
            </div>
          </div>
          <div className="actor">
            <span className="avatar">{session?.principal.slice(0, 1).toUpperCase() ?? "?"}</span>
            <div>
              <strong>{session?.principal ?? "Not signed in"}</strong>
              <small>
                {session?.platform_admin ? "Platform admin" : "Authenticated user"}
                {session?.issuer ? ` · ${session.issuer}` : ""}
              </small>
            </div>
            <label className="theme-control">
              <span className="sr-only">Color theme</span>
              <select aria-label="Color theme" value={theme} onChange={(event) => onThemeChange(event.target.value as ShellTheme)}>
                <option value="system">System theme</option>
                <option value="light">Light theme</option>
                <option value="dark">Dark theme</option>
              </select>
            </label>
            {session && <button className="text-button" onClick={onLogout}>Sign out</button>}
            {logoutPending && <button className="text-button" onClick={onLogout}>Retry sign out</button>}
          </div>
        </header>
        {children}
      </main>
    </div>
  );
}

function titleFor(page: ShellPage): string {
  return ({ assets: "Assets", changes: "Changes", activity: "Activity", connections: "Connections", settings: "Settings" })[page];
}

function workspaceLabel(state: ShellWorkspaceState): string {
  return ({ loading: "Checking access", ready: "Connected", unavailable: "Unavailable" })[state];
}
