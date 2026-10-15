import { useWorkspaceNavigation } from "./navigation/useWorkspaceNavigation";
import { discardChanges, useConfirmation } from "../shared/ConfirmationProvider";
import { useCallback, lazy, Suspense, useEffect, useRef, useState } from "react";
import { Button, useMantineColorScheme } from "@mantine/core";
import { locationFromUrl, pageFromHash, type UiPage } from "./navigation/routes";
import { useSessionScope } from "../features/identity/SessionBoundary";
import { sessionQueryScope } from "../data/query_scope";
import { useAssetInventory } from "../features/assets/useAssetInventory";
import { AppShell } from "./AppShell";
import { AssetInventory } from "../features/assets/AssetInventory";
import { CommandPalette, type PaletteCommand } from "./navigation/CommandPalette";
import { LoginPanel } from "../features/identity/LoginPanel";
import { WorkspaceContent } from "./WorkspaceContent";

const AssetRoute = lazy(async () => ({
  default: (await import("../features/assets/AssetRoute")).AssetRoute,
}));
const ActivityRoute = lazy(async () => ({
  default: (await import("../features/activity/ActivityRoute")).ActivityRoute,
}));
const ConnectionsRoute = lazy(async () => ({
  default: (await import("../features/connections/ConnectionsRoute")).ConnectionsRoute,
}));
const SettingsRoute = lazy(async () => ({
  default: (await import("../features/settings/SettingsRoute")).SettingsRoute,
}));

type Page = UiPage;
type Theme = "system" | "light" | "dark";
type WorkspaceState = "loading" | "ready" | "unavailable";

export function Workspace() {
  const { confirm } = useConfirmation();
  const scope = useSessionScope();
  const {
    session,
    generation: sessionGeneration,
    queryClient,
    isCurrent,
    authConfig,
    sessionOptions,
    bootstrapToken,
    setBootstrapToken,
    authError,
    loggingIn,
    logoutPending,
    bootstrapLogin,
    logout,
  } = scope;
  const { colorScheme, setColorScheme } = useMantineColorScheme();
  const theme: Theme = colorScheme === "auto" ? "system" : colorScheme;
  const setTheme = (next: Theme) => setColorScheme(next === "system" ? "auto" : next);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [paletteQuery, setPaletteQuery] = useState("");
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const mobileNavTrigger = useRef<HTMLButtonElement | null>(null);
  const [assetContext, setAssetContext] = useState<{ name: string; catalog: string } | null>(null);
  const [notice, setNotice] = useState("Loading workspace…");
  const [managementDirty, setManagementDirty] = useState(false);
  const dirty = useRef(false);
  const onDirtyChange = useCallback((value: boolean) => {
    dirty.current = value;
    setManagementDirty(value);
  }, []);
  const navigation = useWorkspaceNavigation({
    isDirty: () => dirty.current,
    confirmLeaving: confirmDiscardUnsaved,
    onLeave: () => {
      setMobileNavOpen(false);
      setAssetContext(null);
      onDirtyChange(false);
    },
  });
  const { page, assetId: requestedAssetId, tab: routeTab, openAsset } = navigation;
  const sessionCacheKey = sessionQueryScope(session, sessionGeneration);
  const inventory = useAssetInventory(queryClient, sessionCacheKey, Boolean(session));
  const {
    assets,
    search: assetSearch,
    loading: assetInventoryLoading,
    hasMore: assetHasMore,
    error: assetInventoryError,
  } = inventory;
  const workspace: WorkspaceState = session
    ? inventory.pending
      ? "loading"
      : inventory.error && !assets.length
        ? "unavailable"
        : "ready"
    : scope.status;
  useEffect(() => {
    if (session) restorePostLoginHash();
  }, [session]);

  useEffect(() => {
    const onShortcut = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setPaletteOpen(true);
        setPaletteQuery("");
      } else if (event.key === "Escape") {
        closePalette();
        setMobileNavOpen(false);
      }
    };
    window.addEventListener("keydown", onShortcut);
    return () => window.removeEventListener("keydown", onShortcut);
  }, []);

  useEffect(() => {
    if (!managementDirty) return;
    const warn = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [managementDirty]);

  useEffect(() => {
    if (
      !session ||
      (page !== "connections" && page !== "settings") ||
      session.capabilities.includes("workspace:admin")
    )
      return;
    void queryClient.cancelQueries({
      queryKey: ["management", sessionCacheKey],
    });
    navigation.redirectToAssets();
    setNotice("This workspace view requires platform administrator capability.");
  }, [page, queryClient, session, sessionCacheKey]);

  const navigateTo = navigation.navigate;

  useEffect(() => {
    if (mobileNavOpen) return;
    mobileNavTrigger.current?.focus();
  }, [mobileNavOpen]);

  function runPaletteCommand(command: Page | "help") {
    closePalette();
    if (command === "help") {
      setNotice("Use navigation to inspect governed assets, author policies, and audit activity.");
      return;
    }
    navigateTo(command);
  }

  function openPaletteAsset(assetId: string) {
    closePalette();
    const target = assets.find((item) => item.id === assetId);
    if (!target) return;
    openAsset(target.id);
  }

  function closePalette() {
    setPaletteOpen(false);
  }

  function restorePostLoginHash() {
    try {
      const pending = window.sessionStorage.getItem("dal_obscura_post_login_hash");
      window.sessionStorage.removeItem("dal_obscura_post_login_hash");
      if (pending && pageFromHash(pending) !== "assets") {
        window.location.hash = pageFromHash(pending);
      }
    } catch {
      // Storage access is optional; the authenticated workspace remains usable.
    }
  }

  async function confirmDiscardUnsaved() {
    if (!dirty.current) return true;
    const confirmed = await confirm(discardChanges("editor"));
    if (!isCurrent(sessionGeneration)) return false;
    if (confirmed) onDirtyChange(false);
    return confirmed;
  }

  const signedOut = !session || workspace === "unavailable";
  const canManageWorkspace = Boolean(session?.capabilities.includes("workspace:admin"));
  const paletteCommands: PaletteCommand[] = [
    "assets",
    "activity",
    ...(canManageWorkspace ? (["connections", "settings"] as const) : []),
    "help",
  ];
  const paletteAssets = assets
    .filter((item) =>
      `${item.catalog} ${item.name}`.toLowerCase().includes(paletteQuery.trim().toLowerCase()),
    )
    .slice(0, 8);
  const accessView = (
    <LoginPanel
      showAuth={!session && scope.status === "unavailable"}
      title={
        (!session ? scope.status : workspace) === "loading"
          ? "Loading governed workspace"
          : "Sign in to your workspace"
      }
      message={
        (!session ? scope.status : workspace) === "loading"
          ? "Checking your workspace access and available assets."
          : !session
            ? scope.notice
            : notice
      }
      retry={
        !session && scope.status === "unavailable"
          ? scope.reconnect
          : workspace === "unavailable"
            ? () => void inventory.refresh()
            : undefined
      }
      authConfig={authConfig}
      sessionOptions={sessionOptions}
      bootstrapToken={bootstrapToken}
      onBootstrapToken={setBootstrapToken}
      onBootstrapLogin={() => void bootstrapLogin()}
      loggingIn={loggingIn}
      authError={authError}
    />
  );
  const managementView =
    page === "assets" ? null : (
      <Suspense
        fallback={
          <section className="coming-soon" role="status">
            <h2>Loading management view</h2>
            <p>Preparing the governed workspace controls.</p>
          </section>
        }
      >
        {page === "activity" ? (
          <ActivityRoute />
        ) : page === "connections" ? (
          <ConnectionsRoute onDirtyChange={onDirtyChange} />
        ) : (
          <SettingsRoute onDirtyChange={onDirtyChange} />
        )}
      </Suspense>
    );
  const assetView = !requestedAssetId ? (
    <>
      <p className="inventory-intro">
        Browse governed data. Open an asset to manage its policy, access, and consumers.
      </p>
      {assetInventoryError && (
        <div role="alert" className="notice">
          {assetInventoryError}{" "}
          <Button variant="subtle" onClick={() => void inventory.refresh()}>
            Retry asset search
          </Button>
        </div>
      )}
      <AssetInventory
        assets={assets}
        search={assetSearch}
        loading={assetInventoryLoading}
        hasMore={assetHasMore}
        onSearch={inventory.searchAssets}
        onSelect={openAsset}
        onLoadMore={() => void inventory.loadMore()}
      />
    </>
  ) : (
    <Suspense
      fallback={
        <section className="coming-soon" role="status">
          <h2>Loading policy workspace</h2>
          <p>Preparing the nested policy editor.</p>
        </section>
      }
    >
      <AssetRoute
        key={requestedAssetId}
        assetId={requestedAssetId}
        initialTab={routeTab}
        onTabChange={navigation.updateTab}
        onBack={() => navigateTo("assets")}
        onContextChange={setAssetContext}
        onDirtyChange={onDirtyChange}
      />
    </Suspense>
  );
  const content = (
    <WorkspaceContent
      signedOut={signedOut}
      page={page}
      workspace={session ? workspace : scope.status}
      accessView={accessView}
      managementView={managementView}
      noAssetsView={
        <LoginPanel
          title={
            assets.length || locationFromUrl(window.location.hash, window.location.search).assetId
              ? "Asset unavailable"
              : "No governed assets"
          }
          message={notice}
          retry={() => void inventory.refresh()}
        />
      }
      assetView={assetView}
    />
  );

  return (
    <AppShell
      page={page}
      session={session}
      workspace={session ? workspace : scope.status}
      assetName={
        page === "assets" && requestedAssetId
          ? (assetContext?.name ?? "Asset workspace")
          : undefined
      }
      assetCatalog={page === "assets" && requestedAssetId ? assetContext?.catalog : undefined}
      mobileNavOpen={mobileNavOpen}
      mobileNavTrigger={mobileNavTrigger}
      theme={theme}
      logoutPending={logoutPending}
      onNavigate={navigateTo}
      onMobileNavOpen={() => setMobileNavOpen(true)}
      onMobileNavClose={() => setMobileNavOpen(false)}
      onThemeChange={setTheme}
      onLogout={() => void logout()}
      onSearch={() => {
        setPaletteQuery("");
        setPaletteOpen(true);
      }}
    >
      {content}
      <CommandPalette
        opened={paletteOpen}
        query={paletteQuery}
        commands={paletteCommands}
        assets={paletteAssets}
        onQueryChange={setPaletteQuery}
        onCommand={runPaletteCommand}
        onAsset={openPaletteAsset}
        onClose={closePalette}
      />
    </AppShell>
  );
}
