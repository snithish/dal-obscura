import { parseTestClaims } from "./policy_test";
import { discardChanges, useConfirmation } from "./components/ConfirmationProvider";
import { lazy, Suspense, useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient } from "@tanstack/react-query";
import { Button, useMantineColorScheme } from "@mantine/core";
import { AppProviders } from "./design/AppProviders";
import type { ApiFailure, Asset, PolicyRule, Preview, Session, SessionOptions, UiAuthConfig } from "./api";
import { controlPlane } from "./api";
import { isCurrentEpoch } from "./lifecycle";
import { locationFromUrl, pageFromHash, type UiPage } from "./navigation";
import { assetInventoryQueryKey, sessionQueryScope } from "./query_scope";
import { recoveryMessage } from "./recovery";
import { isAbortError } from "./async";
import { AppShell } from "./components/AppShell";
import { AssetInventory } from "./components/AssetInventory";
import { CommandPalette, type PaletteCommand } from "./components/CommandPalette";
import { LoginPanel } from "./components/LoginPanel";
import type { AuditFilters, ManagementData } from "./components/ManagementViews";
import { WorkspaceContent } from "./components/WorkspaceContent";
import "./styles.css";
import "./design/base.css";

const AssetWorkspace = lazy(async () => ({ default: (await import("./components/AssetWorkspace")).AssetWorkspace }));
const ManagementView = lazy(async () => ({ default: (await import("./components/ManagementViews")).ManagementView }));

type Page = UiPage;
type SaveState = "saved" | "saving" | "unsaved" | "failed";
type Theme = "system" | "light" | "dark";
type WorkspaceState = "loading" | "ready" | "unavailable";
const newRule = (ordinal: number): PolicyRule => ({ ordinal, effect: "allow", name: "", description: "", principals: [], columns: [], masks: {}, row_filter: null, when: {} });

function App() {
  const { confirm, cancelConfirmation } = useConfirmation();
  const historyNavigationPending = useRef(false);
  const { colorScheme, setColorScheme } = useMantineColorScheme();
  const theme: Theme = colorScheme === "auto" ? "system" : colorScheme;
  const setTheme = (next: Theme) => setColorScheme(next === "system" ? "auto" : next);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [paletteQuery, setPaletteQuery] = useState("");
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const mobileNavTrigger = useRef<HTMLButtonElement | null>(null);
  const [page, setPage] = useState<Page>(() => pageFromHash(window.location.hash));
  const [workspace, setWorkspace] = useState<WorkspaceState>("loading");
  const [assets, setAssets] = useState<Asset[]>([]);
  const [asset, setAsset] = useState<Asset | null>(null);
  const [requestedAssetId, setRequestedAssetId] = useState(() => locationFromUrl(window.location.hash, window.location.search).assetId);
  const [assetLoading, setAssetLoading] = useState(false);
  const [revokingTokens, setRevokingTokens] = useState(false);
  const revokePending = useRef(false);
  const [routeTab, setRouteTab] = useState(() => locationFromUrl(window.location.hash, window.location.search).tab);
  const [assetSearch, setAssetSearch] = useState("");
  const [assetCursor, setAssetCursor] = useState<string | null>(null);
  const [assetHasMore, setAssetHasMore] = useState(false);
  const [assetInventoryLoading, setAssetInventoryLoading] = useState(false);
  const [assetInventoryError, setAssetInventoryError] = useState("");
  const [rules, setRules] = useState<PolicyRule[]>([]);
  const rulesRef = useRef<PolicyRule[]>([]);
  const savedRules = useRef<PolicyRule[]>([]);
  const [policyRevision, setPolicyRevision] = useState(0);
  const [saveState, setSaveState] = useState<SaveState>("saved");
  const [previewBusy, setPreviewBusy] = useState(false);
  const [previewError, setPreviewError] = useState("");
  const previewInputEpoch = useRef(0);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [previewPrincipal, setPreviewPrincipal] = useState("analyst.alex");
  const [previewGroups, setPreviewGroups] = useState("us-analysts");
  const [previewClaims, setPreviewClaims] = useState("{}");
  const [notice, setNotice] = useState("Loading workspace…");
  const [fieldErrors, setFieldErrors] = useState<Array<{ field: string; message: string; type: string }>>([]);
  const [session, setSession] = useState<Session | null>(null);
  const [sessionGeneration, setSessionGeneration] = useState(0);
  const [authConfig, setAuthConfig] = useState<UiAuthConfig | null>(null);
  const [sessionOptions, setSessionOptions] = useState<SessionOptions | null>(null);
  const [bootstrapToken, setBootstrapToken] = useState("");
  const [authError, setAuthError] = useState("");
  const [loggingIn, setLoggingIn] = useState(false);
  const loginInFlight = useRef(false);
  const [managementData, setManagementData] = useState<ManagementData>({});
  const [managementLoading, setManagementLoading] = useState(false);
  const [managementError, setManagementError] = useState("");
  const [auditLoading, setAuditLoading] = useState(false);
  const [auditFilters, setAuditFilters] = useState<AuditFilters>({});
  const [managementDirty, setManagementDirty] = useState(false);
  const savePolicyPending = useRef(false);
  const previewPendingRef = useRef(false);
  const loadEpoch = useRef(0);
  const policyEditEpoch = useRef(0);
  const inventoryEpoch = useRef(0);
  const searchTimer = useRef<number | undefined>(undefined);
  const managementEpoch = useRef(0);
  const auditLoadingRef = useRef(false);
  const workspaceAbortController = useRef<AbortController | null>(null);
  const mutationControllers = useRef<Set<AbortController>>(new Set());
  const [queryClient] = useState(
    () => new QueryClient({
      defaultOptions: {
        queries: { retry: false, staleTime: 0 },
        mutations: { retry: false },
      },
    }),
  );
  const sessionCacheKey = sessionQueryScope(session, sessionGeneration);
  const previousSessionCacheKey = useRef(sessionCacheKey);
  const [logoutPending, setLogoutPending] = useState(false);
  const logoutInFlight = useRef(false);
  useEffect(() => {
    void loadInitialWorkspace();
  }, []);

  useEffect(() => {
    if (previousSessionCacheKey.current === sessionCacheKey) return;
    void queryClient.cancelQueries();
    queryClient.clear();
    previousSessionCacheKey.current = sessionCacheKey;
  }, [queryClient, sessionCacheKey]);

  useEffect(() => {
    rulesRef.current = rules;
  }, [rules]);

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

  useEffect(() => () => {
    loadEpoch.current += 1;
    inventoryEpoch.current += 1;
    managementEpoch.current += 1;
    if (searchTimer.current !== undefined) window.clearTimeout(searchTimer.current);
    workspaceAbortController.current?.abort();
    abortMutations();
    void queryClient.cancelQueries();
    queryClient.clear();
  }, []);

  useEffect(() => {
    const syncBrowserLocation = async () => {
      if (historyNavigationPending.current) return;
      const location = locationFromUrl(window.location.hash, window.location.search);
      const next = location.page;
      const leaving = next !== page || location.assetId !== requestedAssetId;
      const needsGuard = (leaving || location.tab !== routeTab) && (saveState === "unsaved" || saveState === "failed" || managementDirty);
      historyNavigationPending.current = true;
      const approved = !needsGuard || await confirmDiscardUnsaved();
      historyNavigationPending.current = false;
      if (!approved) {
        const params = new URLSearchParams();
        if (requestedAssetId) params.set("asset", requestedAssetId);
        if (routeTab) params.set("tab", routeTab);
        window.history.pushState(null, "", `${window.location.pathname}${params.size ? `?${params}` : ""}#${page}`);
        return;
      }
      setPage(next);
      if (leaving) window.scrollTo(0, 0);
      setRouteTab(location.tab);
      setRequestedAssetId(location.assetId);
      if (leaving) { setAsset(null); setSaveState("saved"); setManagementDirty(false); ++loadEpoch.current; }
      if (next === "assets" && session && location.assetId && leaving) {
        void loadAsset(location.assetId, undefined, sessionCacheKey);
      }
    };
    window.addEventListener("hashchange", syncBrowserLocation);
    window.addEventListener("popstate", syncBrowserLocation);
    return () => {
      window.removeEventListener("hashchange", syncBrowserLocation);
      window.removeEventListener("popstate", syncBrowserLocation);
    };
  }, [requestedAssetId, routeTab, assets, managementDirty, page, saveState, session, sessionCacheKey]);

  useEffect(() => {
    const handleAuthExpired = (event: Event) => {
      const detail = (event as CustomEvent<{ code?: string }>).detail;
      loadEpoch.current += 1;
      inventoryEpoch.current += 1;
      managementEpoch.current += 1;
      workspaceAbortController.current?.abort();
      abortMutations();
      clearPrivateState();
      setNotice(detail?.code === "auth_challenge"
        ? recoveryMessage(detail, "Your session expired or was revoked. Sign in again to continue.")
        : "Your session expired or was revoked. Sign in again to continue.");
    };
    window.addEventListener("dal-obscura-auth-expired", handleAuthExpired);
    return () => window.removeEventListener("dal-obscura-auth-expired", handleAuthExpired);
  }, []);

  useEffect(() => {
    if (saveState !== "unsaved" && !managementDirty) return;
    const warn = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [managementDirty, saveState]);

  useEffect(() => {
    if (page === "assets" || !session) return;
    void loadManagement(page);
  }, [page, session, auditFilters]);

  useEffect(() => {
    if (!session || (page !== "connections" && page !== "settings") || session.capabilities.includes("workspace:admin")) return;
    managementEpoch.current += 1;
    void queryClient.cancelQueries({ queryKey: ["management", sessionCacheKey] });
    setManagementData({});
    setManagementError("");
    setPage("assets");
    if (window.location.hash !== "#assets") window.history.replaceState(null, "", "#assets");
    setNotice("This workspace view requires platform administrator capability.");
  }, [page, queryClient, session, sessionCacheKey]);

  async function loadManagement(destination: Page) {
    const epoch = ++managementEpoch.current;
    await queryClient.cancelQueries({ queryKey: ["management", sessionCacheKey] });
    if ((destination === "connections" || destination === "settings") && !session?.capabilities.includes("workspace:admin")) {
      setManagementData({});
      setManagementError("");
      setManagementLoading(false);
      return;
    }
    setManagementLoading(true);
    setManagementError("");
    try {
      let next: ManagementData = {};
      if (destination === "activity") {
        const filtersKey = JSON.stringify(auditFilters);
        const [audit, summary, observations] = await Promise.all([
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "activity", "audit", filtersKey, 50],
            queryFn: ({ signal }) => controlPlane.listAuditEventsPage({ limit: 50, ...auditFilters, signal }),
          }),
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "activity", "summary"],
            queryFn: ({ signal }) => controlPlane.getSummary(signal),
          }),
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "activity", "observations"],
            queryFn: ({ signal }) => controlPlane.getObservations(signal),
          }),
        ]);
        next = { events: audit.items, eventsNextCursor: audit.next_cursor, summary, observations };
      }
      if (destination === "connections") {
        const [pluginData, catalogs] = await Promise.all([
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "connections", "plugins"],
            queryFn: ({ signal }) => controlPlane.listPlugins(signal),
          }),
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "connections", "catalogs"],
            queryFn: ({ signal }) => controlPlane.listCatalogs(signal),
          }),
        ]);
        next = { catalogs, plugins: pluginData.plugins, pluginStates: pluginData.states, pluginPairs: pluginData.pairs };
      }
      if (destination === "settings") {
        const [runtime, providers, revision] = await Promise.all([
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "settings", "runtime"],
            queryFn: ({ signal }) => controlPlane.getRuntimeSettings(signal),
          }),
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "settings", "providers"],
            queryFn: ({ signal }) => controlPlane.getAuthProviders(signal),
          }),
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "settings", "provider-revision"],
            queryFn: ({ signal }) => controlPlane.getAuthProviderRevision(signal),
          }),
        ]);
        next = { runtime, providers, providerRevision: revision.revision };
      }
      if (!isCurrentEpoch(epoch, managementEpoch.current)) return;
      // Keep asset-scoped access metadata while a management route is active.
      // The asset editor stays mounted behind navigation; replacing this object
      // would temporarily erase edit capability and make the editor read-only
      // when the user returns to Assets.
      setManagementData((current) => ({ ...current, ...next }));
    } catch (error) {
      if (!isCurrentEpoch(epoch, managementEpoch.current)) return;
      if ((error instanceof DOMException && error.name === "AbortError") || (error instanceof Error && error.name === "CancelledError")) return;
      const failure = error as ApiFailure;
      setManagementError(recoveryMessage(failure, "This management view could not be loaded. The server may be unavailable or the session may have expired."));
    } finally {
      if (epoch === managementEpoch.current) setManagementLoading(false);
    }
  }

  async function loadMoreAudit() {
    const cursor = managementData.eventsNextCursor;
    if (!cursor || auditLoading || auditLoadingRef.current) return;
    auditLoadingRef.current = true;
    const scope = managementEpoch.current;
    setAuditLoading(true);
    try {
      const pageResult = await queryClient.fetchQuery({
        queryKey: ["management", sessionCacheKey, "activity", "audit", JSON.stringify(auditFilters), 50, cursor],
        queryFn: ({ signal }) => controlPlane.listAuditEventsPage({ limit: 50, cursor, ...auditFilters, signal }),
      });
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setManagementData((current) => ({ ...current, events: [...(current.events ?? []), ...pageResult.items], eventsNextCursor: pageResult.next_cursor }));
    } catch (error) {
      if ((error instanceof DOMException && error.name === "AbortError") || (error instanceof Error && error.name === "CancelledError")) return;
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setNotice(recoveryMessage(error, "More activity could not be loaded. The entries already visible remain available."));
    } finally {
      auditLoadingRef.current = false;
      if (scope === managementEpoch.current) setAuditLoading(false);
    }
  }

  function updateAuditFilters(next: AuditFilters) {
    managementEpoch.current += 1;
    void queryClient.cancelQueries({ queryKey: ["management", sessionCacheKey] });
    setManagementData({});
    setAuditFilters(next);
  }

  async function loadInitialWorkspace() {
    const epoch = ++loadEpoch.current;
    workspaceAbortController.current?.abort();
    const controller = new AbortController();
    workspaceAbortController.current = controller;
    setWorkspace("loading");
    try {
      const loadedSession = await controlPlane.getSession(controller.signal);
      if (epoch !== loadEpoch.current) return;
      // The first authenticated page is fetched before React commits the
      // session state. Mark the cache scope now so the session transition
      // effect does not immediately discard that private result.
      const loadedSessionScope = sessionQueryScope(loadedSession, epoch);
      void queryClient.cancelQueries();
      queryClient.clear();
      previousSessionCacheKey.current = loadedSessionScope;
      setSessionGeneration(epoch);
      setSession(loadedSession);
      const loadedPage = await queryClient.fetchQuery({
        queryKey: assetInventoryQueryKey(loadedSessionScope, "", null),
        queryFn: ({ signal }) => controlPlane.listAssetPage({ limit: 50, signal }),
      });
      const loaded = loadedPage.items;
      if (epoch !== loadEpoch.current) return;
      setAssets(loaded);
      setAssetCursor(loadedPage.next_cursor);
      setAssetHasMore(Boolean(loadedPage.next_cursor));
      const location = locationFromUrl(window.location.hash, window.location.search);
      if (!loaded.length && !location.assetId) {
        setWorkspace("ready"); setNotice("No governed assets are available in this workspace.");
        restorePostLoginHash();
        return;
      }
      const requestedAssetId = location.assetId;
      if (requestedAssetId && location.page === "assets") await loadAsset(requestedAssetId, epoch, loadedSessionScope);
      if (epoch !== loadEpoch.current) return;
      setWorkspace("ready");
      restorePostLoginHash();
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      if (epoch !== loadEpoch.current) return;
      setWorkspace("unavailable");
      setSession(null);
      const options = await controlPlane.getSessionOptions(controller.signal).catch(() => null);
      if (epoch !== loadEpoch.current) return;
      setSessionOptions(options);
      setAuthConfig(options?.oidc ?? await controlPlane.getUiAuthConfig().catch(() => null));
      setNotice(recoveryMessage(error, "Workspace unavailable. Sign in or reconnect to the control plane; no demo data is shown automatically."));
    }
  }

  async function refreshAssetInventory(search: string, append = false) {
    const epoch = ++inventoryEpoch.current;
    const searchTerm = search.trim();
    await queryClient.cancelQueries({ queryKey: ["asset-inventory", sessionCacheKey] });
    setAssetInventoryLoading(true);
    setAssetInventoryError("");
    try {
      const pageResult = await queryClient.fetchQuery({
        queryKey: assetInventoryQueryKey(
          sessionCacheKey,
          searchTerm,
          append ? assetCursor : null,
        ),
        queryFn: ({ signal }) => controlPlane.listAssetPage({
          limit: 50,
          cursor: append ? assetCursor ?? undefined : undefined,
          search: searchTerm || undefined,
          signal,
        }),
      });
      if (epoch !== inventoryEpoch.current) return;
      setAssets((current) => append ? [...current, ...pageResult.items] : pageResult.items);
      setAssetCursor(pageResult.next_cursor);
      setAssetHasMore(Boolean(pageResult.next_cursor));
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      if (epoch !== inventoryEpoch.current) return;
      setAssetInventoryError(recoveryMessage(error, "Could not update the asset list. Showing the previously loaded results."));
    } finally {
      if (epoch === inventoryEpoch.current) setAssetInventoryLoading(false);
    }
  }

  function searchAssets(value: string) {
    setAssetSearch(value);
    if (searchTimer.current !== undefined) window.clearTimeout(searchTimer.current);
    searchTimer.current = window.setTimeout(() => void refreshAssetInventory(value), 250);
  }

  async function bootstrapLogin() {
    if (loginInFlight.current) return;
    const token = bootstrapToken.trim();
    if (!token) {
      setAuthError("Enter the local control-plane token to continue.");
      return;
    }
    loginInFlight.current = true;
    setLoggingIn(true);
    setAuthError("");
    const controller = beginMutation();
    try {
      await controlPlane.bootstrapLogin(token, controller.signal);
      setBootstrapToken("");
      setAuthError("");
      loadEpoch.current += 1;
      await loadInitialWorkspace();
    } catch (error) {
      if (isAbortError(error)) return;
      setAuthError((error as { status?: number })?.status === 429
        ? "Too many sign-in attempts. Wait a moment and try again."
        : "That local token was not accepted. Check the control-plane configuration and try again.");
      setWorkspace("unavailable");
      setNotice("Sign-in failed. No policy data was loaded.");
    } finally {
      finishMutation(controller);
      loginInFlight.current = false;
      setLoggingIn(false);
    }
  }

  async function logout() {
    if (logoutInFlight.current) return;
    logoutInFlight.current = true;
    loadEpoch.current += 1;
    inventoryEpoch.current += 1;
    managementEpoch.current += 1;
    workspaceAbortController.current?.abort();
    abortMutations();
    if (searchTimer.current !== undefined) {
      window.clearTimeout(searchTimer.current);
      searchTimer.current = undefined;
    }
    // Fence and clear private state before awaiting network revocation. A slow
    // server response must never leave policy data visible or let a late request
    // repopulate the previous session.
    clearPrivateState();
    const controller = beginMutation();
    try {
      const result = await controlPlane.logout(controller.signal);
      setLogoutPending(false);
      if (result.logout_url) {
        window.location.assign(result.logout_url);
        return;
      }
      setNotice("Signed out. No policy data remains loaded in this browser.");
    } catch (error) {
      if (isAbortError(error)) return;
      setLogoutPending(true);
      setNotice("Sign out could not be confirmed. Private data is hidden; retry sign out before closing this browser.");
    } finally {
      finishMutation(controller);
      logoutInFlight.current = false;
    }
  }

  function beginMutation(): AbortController {
    const controller = new AbortController();
    mutationControllers.current.add(controller);
    return controller;
  }

  function finishMutation(controller: AbortController): void {
    mutationControllers.current.delete(controller);
  }

  function abortMutations(): void {
    for (const controller of mutationControllers.current) controller.abort();
    mutationControllers.current.clear();
  }

  function clearPrivateState() {
    cancelConfirmation();
    void queryClient.cancelQueries();
    queryClient.clear();
    previousSessionCacheKey.current = "anonymous";
    policyEditEpoch.current += 1;
    setSession(null); setAsset(null); setAssets([]); setSavedRules([]); setPreview(null); setPreviewError("");
    setManagementData({}); setAssetCursor(null); setAssetHasMore(false); setAssetSearch(""); setAssetInventoryError("");
    setManagementDirty(false);
    setPolicyRevision(0); setSaveState("saved");
    setWorkspace("unavailable");
    void controlPlane.getSessionOptions().then((options) => {
      setSessionOptions(options);
      setAuthConfig(options.oidc);
    }).catch(() => {
      void controlPlane.getUiAuthConfig().then(setAuthConfig).catch(() => setAuthConfig(null));
    });
  }

  async function navigateTo(next: Page) {
    if (next === page && !requestedAssetId) return;
    if (!await confirmDiscardUnsaved()) return;
    setMobileNavOpen(false);
    window.scrollTo(0, 0);
    ++loadEpoch.current;
    setAsset(null); setRequestedAssetId(undefined); setRouteTab(undefined);
    setSaveState("saved"); setManagementDirty(false); setPage(next);
    window.history.pushState(null, "", `${window.location.pathname}#${next}`);
  }

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
    if (saveState !== "unsaved" && saveState !== "failed" && !managementDirty) return true;
    const confirmed = await confirm(discardChanges("editor"));
    if (confirmed) setManagementDirty(false);
    return confirmed;
  }

  function invalidateAssetQueries(assetId: string) {
    void queryClient.invalidateQueries({ queryKey: ["asset", sessionCacheKey, assetId] });
    void queryClient.invalidateQueries({ queryKey: ["asset-inventory", sessionCacheKey] });
    void queryClient.invalidateQueries({ queryKey: ["management", sessionCacheKey] });
  }

  async function openAsset(id: string) {
    if (page === "assets" && requestedAssetId === id) return;
    if (!await confirmDiscardUnsaved()) return;
    window.scrollTo(0, 0);
    setAsset(null); setRequestedAssetId(id); setRouteTab(undefined); setPage("assets");
    setSaveState("saved"); setManagementDirty(false);
    const params = new URLSearchParams({ asset: id });
    window.history.pushState(null, "", `${window.location.pathname}?${params}#assets`);
    void loadAsset(id);
  }

  async function loadAsset(
    assetId: string,
    inheritedEpoch?: number,
    inheritedSessionScope = sessionCacheKey,
  ) {
    setAssetLoading(true);
    const epoch = inheritedEpoch ?? ++loadEpoch.current;
    await queryClient.cancelQueries({ queryKey: ["asset", inheritedSessionScope] });
    if (epoch !== loadEpoch.current) return;
    try {
      const assetKey = ["asset", inheritedSessionScope, assetId] as const;
      const [fullAsset, schema, grants, access] = await Promise.all([
        queryClient.fetchQuery({ queryKey: [...assetKey, "detail"], queryFn: ({ signal }) => controlPlane.getAsset(assetId, signal) }),
        queryClient.fetchQuery({ queryKey: [...assetKey, "schema"], queryFn: ({ signal }) => controlPlane.getSchema(assetId, signal) }),
        queryClient.fetchQuery({ queryKey: [...assetKey, "grants"], queryFn: ({ signal }) => controlPlane.listGrants(assetId, signal) }),
        queryClient.fetchQuery({ queryKey: [...assetKey, "access"], queryFn: ({ signal }) => controlPlane.getAssetAccess(assetId, signal) }),
      ]);
      if (epoch !== loadEpoch.current) return;
      const hydratedAsset: Asset = { ...fullAsset, schema };
      const effectiveRules = fullAsset.policy_rules ?? [];
      setManagementData((current) => ({ ...current, grants, access }));
      setAsset(hydratedAsset); setSavedRules(effectiveRules); setPolicyRevision(fullAsset.policy_revision ?? 0);
      policyEditEpoch.current += 1;
      setPreview(null); setPreviewError(""); setSaveState("saved");
      setNotice(effectiveRules.length ? "Loaded the current live policy." : "No policy rules are configured. All columns return NULL until a rule overrides their masks.");
    } catch (error) {
      if ((error instanceof DOMException && error.name === "AbortError") || (error instanceof Error && error.name === "CancelledError")) return;
      if (epoch !== loadEpoch.current) return;
      setNotice(recoveryMessage(error, "Could not load this asset and its access metadata. Retry or return to assets."));
    } finally {
      if (epoch === loadEpoch.current) setAssetLoading(false);
    }
  }

  function replaceRules(next: PolicyRule[]) { rulesRef.current = next; setRules(next); }
  function setSavedRules(next: PolicyRule[]) { savedRules.current = structuredClone(next); replaceRules(next); }
  function discardPolicyChanges() {
    replaceRules(structuredClone(savedRules.current)); policyEditEpoch.current += 1;
    setManagementDirty(false); setFieldErrors([]); setSaveState("saved");
    setNotice("Discarded all local changes. Showing the last saved policy.");
  }
  function allowAllPolicy() {
    const ordinal = Math.max(0, ...rulesRef.current.map((rule) => rule.ordinal)) + 10;
    replaceRules([...rulesRef.current.filter((rule) => rule.effect !== "allow_all"), { ordinal, effect: "allow_all", name: "Allow all", description: "All authenticated readers receive all columns and rows without masks.", principals: ["*"], columns: ["*"], masks: {}, row_filter: null, when: {} }]);
    policyEditEpoch.current += 1; setSaveState("unsaved"); setPreview(null);
    setNotice("Allow all is staged. Save policy to bypass all masks and row filters for authenticated readers.");
  }
  function updateRule(change: (rule: PolicyRule) => PolicyRule, targetIndex: number) {
    if (!rulesRef.current[targetIndex]) return;
    replaceRules(rulesRef.current.map((rule, index) => index === targetIndex ? change(rule) : rule));
    policyEditEpoch.current += 1;
    setSaveState("unsaved"); setPreview(null); setNotice("Policy changed locally. Test saved policy to evaluate the last saved version.");
  }
  function addRule() {
    const current = rulesRef.current;
    const ordinal = Math.max(0, ...current.map((rule) => rule.ordinal)) + 10;
    replaceRules([...current, newRule(ordinal)]);
    policyEditEpoch.current += 1;
    setSaveState("unsaved"); setPreview(null);
    setNotice("New rule added locally. Add at least one principal before saving.");
  }
  function duplicateRule(index: number) {
    const source = rulesRef.current[index];
    if (!source) return;
    const ordinal = Math.max(0, ...rulesRef.current.map((rule) => rule.ordinal)) + 10;
    const duplicate: PolicyRule = {
      ...source,
      ordinal,
      principals: [...source.principals],
      columns: [...source.columns],
      masks: Object.fromEntries(Object.entries(source.masks).map(([field, mask]) => [field, { ...mask }])),
      when: source.when ? { ...source.when } : undefined,
    };
    replaceRules([...rulesRef.current, duplicate]);
    policyEditEpoch.current += 1;
    setSaveState("unsaved"); setPreview(null);
    setNotice("Rule duplicated locally. Check its principals and fields before saving.");
  }
  function removeRule(targetIndex: number) {
    if (!rulesRef.current[targetIndex]) return;
    replaceRules(rulesRef.current.filter((_, index) => index !== targetIndex));
    policyEditEpoch.current += 1;
    setSaveState("unsaved"); setPreview(null);
    setNotice("Rule removed locally. Save policy to apply the change.");
  }
  async function savePolicy(revokeExistingTokens = false) {
    if (!asset || savePolicyPending.current) return;
    savePolicyPending.current = true;
    const assetId = asset.id;
    const submittedRules = structuredClone(rules);
    const editEpoch = policyEditEpoch.current;
    const revision = policyRevision;
    const loadScope = loadEpoch.current;
    setSaveState("saving");
    const controller = beginMutation();
    try {
      // Ordinals define the server's display order. Preserve the local insertion
      // order without changing component identities while forms are being edited.
      const persistedRules = submittedRules.map((rule, index) => ({ ...rule, ordinal: (index + 1) * 10 }));
      const saved = await controlPlane.replaceAssetPolicy(assetId, revision, persistedRules, revokeExistingTokens, controller.signal);
      if (loadScope === loadEpoch.current) { savedRules.current = submittedRules; setPolicyRevision(saved.policy_revision); }
      if (loadScope !== loadEpoch.current || editEpoch !== policyEditEpoch.current) {
        if (loadScope === loadEpoch.current) setSaveState("unsaved");
        return;
      }
      setPolicyRevision(saved.policy_revision);
      if (loadScope !== loadEpoch.current || editEpoch !== policyEditEpoch.current) {
        if (loadScope === loadEpoch.current) setSaveState("unsaved");
        return;
      }
      setSaveState("saved");
      setAssets((current) => current.map((item) => item.id === assetId ? { ...item, policy_status: submittedRules.length ? "configured" : "missing", policy_revision: saved.policy_revision } : item));
      setNotice(revokeExistingTokens
        ? `Live policy saved; revoked ${saved.revoked_token_count} active token(s).`
        : "Live policy saved. Existing tokens remain valid until expiry unless an owner revokes them.");
      setFieldErrors([]);
      invalidateAssetQueries(assetId);
    } catch (error) {
      if (isAbortError(error)) return;
      if (loadScope !== loadEpoch.current || editEpoch !== policyEditEpoch.current) return;
      setSaveState("failed"); setFieldErrors((error as { fieldErrors?: Array<{ field: string; message: string; type: string }> }).fieldErrors ?? []); setNotice(recoveryMessage(error, "Save failed. Your unsaved changes remain in this browser."));
    } finally {
      finishMutation(controller);
      savePolicyPending.current = false;
    }
  }
  function changeTestPersona(setter: (value: string) => void, value: string) {
    previewInputEpoch.current += 1; setter(value); setPreview(null); setPreviewError("");
  }

  async function runPreview() {
    if (!asset) return;
    if (saveState === "saving") {
      setNotice("Policy save is in progress. Try the saved-policy test again when it completes.");
      return;
    }
    if (previewPendingRef.current) return;
    const parsed = parseTestClaims(previewClaims);
    if (parsed.error || !previewPrincipal.trim()) { setPreviewError(parsed.error ?? "Enter a principal to test."); return; }
    const personaScope = previewInputEpoch.current;
    setPreviewBusy(true); setPreviewError(""); setPreview(null);
    previewPendingRef.current = true;
    const loadScope = loadEpoch.current;
    const editScope = policyEditEpoch.current;
    const revision = policyRevision;
    const controller = beginMutation();
    try {
      const claims = parsed.claims;
      const result: Preview = await controlPlane.evaluate(asset.id, { principal: previewPrincipal.trim(), groups: previewGroups.split(",").map((value) => value.trim()).filter(Boolean), claims }, controller.signal);
      if (loadScope !== loadEpoch.current || editScope !== policyEditEpoch.current || revision !== policyRevision || personaScope !== previewInputEpoch.current) return;
      setPreview(result); setNotice(`Server-side evaluation completed: ${result.decision === "allow" ? "allowed" : "denied"}.`);
    } catch (error) {
      if (isAbortError(error)) return;
      if (loadScope !== loadEpoch.current || editScope !== policyEditEpoch.current || revision !== policyRevision || personaScope !== previewInputEpoch.current) return;
      setPreview(null); setPreviewError(recoveryMessage(error, "Policy test could not run. Try again."));
    } finally {
      finishMutation(controller);
      previewPendingRef.current = false; setPreviewBusy(false);
    }
  }

  async function revokeAssetTokens() {
    if (revokePending.current) return;
    if (!asset || !await confirm({ title: "Revoke all active tokens?", message: `Revoke every active token for ${asset.name}? Existing readers will be denied on their next token exchange and must request new access.`, confirmLabel: "Revoke tokens", destructive: true })) return;
    revokePending.current = true;
    setRevokingTokens(true);
    const scope = loadEpoch.current;
    const controller = beginMutation();
    try {
      const result = await controlPlane.revokeAssetTokens(asset.id, controller.signal);
      if (controller.signal.aborted || scope !== loadEpoch.current) return;
      setNotice(`Revoked ${result.revoked_token_count} active token(s).`);
      invalidateAssetQueries(asset.id);
    } catch (error) {
      if (!isAbortError(error) && scope === loadEpoch.current) setNotice(recoveryMessage(error, "Could not revoke this asset's tokens."));
    } finally {
      finishMutation(controller);
      revokePending.current = false;
      setRevokingTokens(false);
    }
  }

  const signedOut = workspace === "unavailable" || (workspace === "loading" && !session);
  const canManageWorkspace = Boolean(session?.capabilities.includes("workspace:admin"));
  const paletteCommands: PaletteCommand[] = ["assets", "activity", ...(canManageWorkspace ? ["connections", "settings"] as const : []), "help"];
  const paletteAssets = assets.filter((item) => `${item.catalog} ${item.name}`.toLowerCase().includes(paletteQuery.trim().toLowerCase())).slice(0, 8);
  const accessView = <LoginPanel showAuth={workspace === "unavailable"} title={workspace === "loading" ? "Loading governed workspace" : "Sign in to your workspace"} message={workspace === "loading" ? "Checking your workspace access and available assets." : notice} retry={workspace === "unavailable" ? loadInitialWorkspace : undefined} authConfig={authConfig} sessionOptions={sessionOptions} bootstrapToken={bootstrapToken} onBootstrapToken={setBootstrapToken} onBootstrapLogin={() => void bootstrapLogin()} loggingIn={loggingIn} authError={authError} />;
  const managementView = page === "assets" ? null : <Suspense fallback={<section className="coming-soon" role="status"><h2>Loading management view</h2><p>Preparing the governed workspace controls.</p></section>}><ManagementView page={page} data={managementData} loading={managementLoading} error={managementError} onReload={() => void loadManagement(page)} onLoadMore={page === "activity" ? () => void loadMoreAudit() : undefined} auditLoading={auditLoading} filters={auditFilters} onFiltersChange={updateAuditFilters} session={session} queryClient={queryClient} sessionScope={sessionCacheKey} onDirtyChange={setManagementDirty} /></Suspense>;
  const editorView = asset && asset.id === requestedAssetId ? <Suspense fallback={<section className="coming-soon" role="status"><h2>Loading policy workspace</h2><p>Preparing the nested policy editor.</p></section>}><AssetWorkspace initialTab={routeTab} onTabChange={setRouteTab} asset={asset} access={managementData.access} grants={managementData.grants ?? []} onBack={() => navigateTo("assets")} rules={rules} activeRevision={policyRevision} saveState={saveState} notice={notice} fieldErrors={fieldErrors} onUpdateRule={updateRule} onAddRule={addRule} onRemoveRule={removeRule} onDuplicateRule={duplicateRule} onDiscard={discardPolicyChanges} onAllowAll={allowAllPolicy} onSave={(revokeExistingTokens) => void savePolicy(revokeExistingTokens)} onPreview={() => void runPreview()} previewPrincipal={previewPrincipal} previewGroups={previewGroups} previewClaims={previewClaims} onPreviewPrincipal={(value) => changeTestPersona(setPreviewPrincipal, value)} onPreviewGroups={(value) => changeTestPersona(setPreviewGroups, value)} onPreviewClaims={(value) => changeTestPersona(setPreviewClaims, value)} preview={preview} previewBusy={previewBusy} previewError={previewError} session={session} onReloadAccess={() => void loadAsset(asset.id)} revokingTokens={revokingTokens} onRevokeTokens={() => void revokeAssetTokens()} onDirtyChange={setManagementDirty} queryClient={queryClient} sessionScope={sessionCacheKey} /></Suspense> : undefined;
  const assetView = !requestedAssetId ? <><p className="inventory-intro">Browse governed data. Open an asset to manage its policy, access, and consumers.</p>{assetInventoryError && <div role="alert" className="notice">{assetInventoryError} <Button variant="subtle" onClick={() => void refreshAssetInventory(assetSearch)}>Retry asset search</Button></div>}<AssetInventory assets={assets} search={assetSearch} loading={assetInventoryLoading} hasMore={assetHasMore} onSearch={searchAssets} onSelect={openAsset} onLoadMore={() => void refreshAssetInventory(assetSearch, true)} /></> : editorView ?? <section aria-label="Asset workspace"><Button variant="subtle" onClick={() => navigateTo("assets")}>Back to assets</Button>{assetLoading ? <p role="status">Loading asset workspace…</p> : <LoginPanel title="Asset unavailable" message={notice} retry={() => void loadAsset(requestedAssetId)} />}</section>;
  const content = <WorkspaceContent signedOut={signedOut} page={page} workspace={workspace} accessView={accessView} managementView={managementView} noAssetsView={<LoginPanel title={assets.length || locationFromUrl(window.location.hash, window.location.search).assetId ? "Asset unavailable" : "No governed assets"} message={notice} retry={loadInitialWorkspace} />} assetView={assetView} />;

  return (
    <AppShell
      page={page}
      session={session}
      workspace={workspace}
      assetName={page === "assets" && requestedAssetId ? asset?.name ?? "Asset workspace" : undefined}
      assetCatalog={page === "assets" && requestedAssetId ? asset?.catalog : undefined}
      mobileNavOpen={mobileNavOpen}
      mobileNavTrigger={mobileNavTrigger}
      theme={theme}
      logoutPending={logoutPending}
      onNavigate={navigateTo}
      onMobileNavOpen={() => setMobileNavOpen(true)}
      onMobileNavClose={() => setMobileNavOpen(false)}
      onThemeChange={setTheme}
      onLogout={() => void logout()}
      onSearch={() => { setPaletteQuery(""); setPaletteOpen(true); }}
    >
      {content}
      <CommandPalette opened={paletteOpen} query={paletteQuery} commands={paletteCommands} assets={paletteAssets} onQueryChange={setPaletteQuery} onCommand={runPaletteCommand} onAsset={openPaletteAsset} onClose={closePalette} />
    </AppShell>
  );
}



createRoot(document.getElementById("root")!).render(<AppProviders><App /></AppProviders>);
