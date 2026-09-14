import { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient } from "@tanstack/react-query";
import type { ApiFailure, Asset, Mask, PolicyRule, Preview, Session, SessionOptions, UiAuthConfig } from "./api";
import { controlPlane } from "./api";
import { isCurrentEpoch } from "./lifecycle";
import { locationFromUrl, pageFromHash, type UiPage } from "./navigation";
import { assetInventoryQueryKey, sessionQueryScope } from "./query_scope";
import { recoveryMessage } from "./recovery";
import { isAbortError } from "./async";
import { LoginPanel } from "./components/LoginPanel";
import { ConnectionsView } from "./components/ConnectionsView";
import { AssetWorkspace } from "./components/AssetWorkspace";
import { ManagementView, type AuditFilters, type ManagementData } from "./components/ManagementViews";
import { Icon, type IconName } from "./components/Icon";
import "./styles.css";

type Page = UiPage;
type SaveState = "saved" | "saving" | "unsaved" | "failed";
type Theme = "system" | "light" | "dark";
type WorkspaceState = "loading" | "ready" | "unavailable";
const newRule = (field: string, ordinal: number): PolicyRule => ({ ordinal, effect: "allow", principals: [], columns: field ? [field] : [], masks: {}, row_filter: null });

function App() {
  const [theme, setTheme] = useState<Theme>(() => {
    try {
      const value = window.localStorage.getItem("dal-obscura-theme");
      return value === "light" || value === "dark" ? value : "system";
    } catch {
      return "system";
    }
  });
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [paletteQuery, setPaletteQuery] = useState("");
  const paletteReturnFocus = useRef<HTMLElement | null>(null);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const mobileNavTrigger = useRef<HTMLButtonElement | null>(null);
  const [page, setPage] = useState<Page>(() => pageFromHash(window.location.hash));
  const [workspace, setWorkspace] = useState<WorkspaceState>("loading");
  const [assets, setAssets] = useState<Asset[]>([]);
  const [asset, setAsset] = useState<Asset | null>(null);
  const [assetSearch, setAssetSearch] = useState("");
  const [assetCursor, setAssetCursor] = useState<string | null>(null);
  const [assetHasMore, setAssetHasMore] = useState(false);
  const [assetInventoryLoading, setAssetInventoryLoading] = useState(false);
  const [rules, setRules] = useState<PolicyRule[]>([]);
  const rulesRef = useRef<PolicyRule[]>([]);
  const rulesUndoStack = useRef<PolicyRule[][]>([]);
  const rulesRedoStack = useRef<PolicyRule[][]>([]);
  const [draftRevision, setDraftRevision] = useState(0);
  const [draftId, setDraftId] = useState<string | null>(null);
  const [reviewOnly, setReviewOnly] = useState(false);
  const [selectedRule, setSelectedRule] = useState(0);
  const [selectedField, setSelectedField] = useState("");
  const [saveState, setSaveState] = useState<SaveState>("saved");
  const [preview, setPreview] = useState<Preview | null>(null);
  const [previewPrincipal, setPreviewPrincipal] = useState("analyst.alex");
  const [previewGroups, setPreviewGroups] = useState("us-analysts");
  const [previewClaims, setPreviewClaims] = useState("{}");
  const [reviewToken, setReviewToken] = useState<string | null>(null);
  const [notice, setNotice] = useState("Loading workspace…");
  const [fieldErrors, setFieldErrors] = useState<Array<{ field: string; message: string; type: string }>>([]);
  const [session, setSession] = useState<Session | null>(null);
  const [authConfig, setAuthConfig] = useState<UiAuthConfig | null>(null);
  const [sessionOptions, setSessionOptions] = useState<SessionOptions | null>(null);
  const [bootstrapToken, setBootstrapToken] = useState("");
  const [authError, setAuthError] = useState("");
  const [loggingIn, setLoggingIn] = useState(false);
  const loginInFlight = useRef(false);
  const [managementData, setManagementData] = useState<ManagementData>({});
  const [managementLoading, setManagementLoading] = useState(false);
  const [managementError, setManagementError] = useState("");
  const [historyLoading, setHistoryLoading] = useState(false);
  const [auditLoading, setAuditLoading] = useState(false);
  const [auditFilters, setAuditFilters] = useState<AuditFilters>({});
  const [managementDirty, setManagementDirty] = useState(false);
  const [publishPending, setPublishPending] = useState(false);
  const saveDraftPending = useRef(false);
  const publishPendingRef = useRef(false);
  const previewPendingRef = useRef(false);
  const reviewPendingRef = useRef(false);
  const restorePendingRef = useRef(false);
  const loadEpoch = useRef(0);
  const draftEditEpoch = useRef(0);
  const inventoryEpoch = useRef(0);
  const searchTimer = useRef<number | undefined>(undefined);
  const managementEpoch = useRef(0);
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
  const sessionCacheKey = sessionQueryScope(session);
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
    const onRuleShortcut = (event: KeyboardEvent) => {
      if (!(event.metaKey || event.ctrlKey) || event.key.toLowerCase() !== "z") return;
      event.preventDefault();
      if (event.shiftKey) redoRules();
      else undoRules();
    };
    window.addEventListener("keydown", onRuleShortcut);
    return () => window.removeEventListener("keydown", onRuleShortcut);
  }, []);

  useEffect(() => {
    document.documentElement.dataset.theme = theme === "system" ? "" : theme;
    try {
      if (theme === "system") window.localStorage.removeItem("dal-obscura-theme");
      else window.localStorage.setItem("dal-obscura-theme", theme);
    } catch {
      // Theme preference is optional and never blocks the workspace.
    }
  }, [theme]);

  useEffect(() => {
    const onShortcut = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        paletteReturnFocus.current = document.activeElement instanceof HTMLElement ? document.activeElement : null;
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
    const syncBrowserLocation = () => {
      const location = locationFromUrl(window.location.hash, window.location.search);
      const next = location.page;
      if (next !== page && !confirmDiscardUnsaved()) {
        window.history.replaceState(null, "", `#${page}`);
        return;
      }
      setPage(next);
      if (next === "assets" && session && location.assetId && location.assetId !== asset?.id) {
        const target = assets.find((item) => item.id === location.assetId);
        if (target && confirmDiscardUnsaved()) {
          setReviewOnly(Boolean(location.draftId));
          void loadAsset(target.id, assets, undefined, location.draftId);
        }
      }
    };
    window.addEventListener("hashchange", syncBrowserLocation);
    window.addEventListener("popstate", syncBrowserLocation);
    return () => {
      window.removeEventListener("hashchange", syncBrowserLocation);
      window.removeEventListener("popstate", syncBrowserLocation);
    };
  }, [asset?.id, assets, managementDirty, page, saveState, session]);

  useEffect(() => {
    const handleAuthExpired = () => {
      loadEpoch.current += 1;
      inventoryEpoch.current += 1;
      managementEpoch.current += 1;
      workspaceAbortController.current?.abort();
      abortMutations();
      clearPrivateState();
      setNotice("Your session expired or was revoked. Sign in again to continue.");
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

  async function loadManagement(destination: Page) {
    const epoch = ++managementEpoch.current;
    await queryClient.cancelQueries({ queryKey: ["management", sessionCacheKey] });
    setManagementLoading(true);
    setManagementError("");
    try {
      let next: ManagementData = {};
      if (destination === "changes") {
        const pageResult = await queryClient.fetchQuery({
          queryKey: ["management", sessionCacheKey, "changes", "history", 50],
          queryFn: ({ signal }) => controlPlane.listHistoryPage({ limit: 50, signal }),
        });
        next = { history: pageResult.items, historyNextCursor: pageResult.next_cursor };
      }
      if (destination === "activity") {
        const filtersKey = JSON.stringify(auditFilters);
        const [audit, history, summary, observations] = await Promise.all([
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "activity", "audit", filtersKey, 50],
            queryFn: ({ signal }) => controlPlane.listAuditEventsPage({ limit: 50, ...auditFilters, signal }),
          }),
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "activity", "history"],
            queryFn: ({ signal }) => controlPlane.listHistory(signal),
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
        next = { history, events: audit.items, eventsNextCursor: audit.next_cursor, summary, observations };
      }
      if (destination === "connections") {
        const [pluginData, catalogs, publications] = await Promise.all([
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "connections", "plugins"],
            queryFn: ({ signal }) => controlPlane.listPlugins(signal),
          }),
          queryClient.fetchQuery({
            queryKey: ["management", sessionCacheKey, "connections", "catalogs"],
            queryFn: ({ signal }) => controlPlane.listCatalogs(signal),
          }),
          session?.platform_admin
            ? queryClient.fetchQuery({
                queryKey: ["management", sessionCacheKey, "connections", "publications"],
                queryFn: ({ signal }) => controlPlane.listWorkspacePublications(signal),
              })
            : Promise.resolve([]),
        ]);
        next = { catalogs, publications, plugins: pluginData.plugins, pluginStates: pluginData.states, pluginPairs: pluginData.pairs };
      }
      if (destination === "settings") {
        const [runtime, providers, revision, publications] = await Promise.all([
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
          session?.platform_admin
            ? queryClient.fetchQuery({
                queryKey: ["management", sessionCacheKey, "settings", "publications"],
                queryFn: ({ signal }) => controlPlane.listWorkspacePublications(signal),
              })
            : Promise.resolve([]),
        ]);
        next = { runtime, providers, providerRevision: revision.revision, publications };
      }
      if (!isCurrentEpoch(epoch, managementEpoch.current)) return;
      setManagementData(next);
    } catch (error) {
      if (!isCurrentEpoch(epoch, managementEpoch.current)) return;
      if ((error instanceof DOMException && error.name === "AbortError") || (error instanceof Error && error.name === "CancelledError")) return;
      const failure = error as ApiFailure;
      setManagementError(recoveryMessage(failure, "This management view could not be loaded. The server may be unavailable or the session may have expired."));
    } finally {
      if (epoch === managementEpoch.current) setManagementLoading(false);
    }
  }

  async function loadMoreHistory() {
    const cursor = managementData.historyNextCursor;
    if (!cursor || historyLoading) return;
    const scope = managementEpoch.current;
    setHistoryLoading(true);
    try {
      const pageResult = await queryClient.fetchQuery({
        queryKey: ["management", sessionCacheKey, "changes", "history", 50, cursor],
        queryFn: ({ signal }) => controlPlane.listHistoryPage({ limit: 50, cursor, signal }),
      });
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setManagementData((current) => ({ ...current, history: [...(current.history ?? []), ...pageResult.items], historyNextCursor: pageResult.next_cursor }));
    } catch (error) {
      if ((error instanceof DOMException && error.name === "AbortError") || (error instanceof Error && error.name === "CancelledError")) return;
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setNotice(recoveryMessage(error, "More history could not be loaded. The entries already visible remain available."));
    } finally {
      if (scope === managementEpoch.current) setHistoryLoading(false);
    }
  }

  async function loadMoreAudit() {
    const cursor = managementData.eventsNextCursor;
    if (!cursor || auditLoading) return;
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
      previousSessionCacheKey.current = sessionQueryScope(loadedSession);
      setSession(loadedSession);
      const loadedPage = await queryClient.fetchQuery({
        queryKey: assetInventoryQueryKey(sessionQueryScope(loadedSession), "", null),
        queryFn: ({ signal }) => controlPlane.listAssetPage({ limit: 50, signal }),
      });
      const loaded = loadedPage.items;
      if (epoch !== loadEpoch.current) return;
      setAssets(loaded);
      setAssetCursor(loadedPage.next_cursor);
      setAssetHasMore(Boolean(loadedPage.next_cursor));
      if (!loaded.length) {
        setWorkspace("ready"); setNotice("No governed assets are available in this workspace.");
        restorePostLoginHash();
        return;
      }
      const location = locationFromUrl(window.location.hash, window.location.search);
      const requestedAssetId = location.assetId;
      const requestedDraftId = location.draftId;
      const selected = loaded.find((item) => item.id === requestedAssetId) ?? loaded[0];
      setReviewOnly(Boolean(requestedDraftId));
      await loadAsset(selected.id, loaded, epoch, requestedDraftId ?? undefined, sessionQueryScope(loadedSession));
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
      setNotice("Workspace unavailable. Sign in or reconnect to the control plane; no demo data is shown automatically.");
    }
  }

  async function refreshAssetInventory(search: string, append = false) {
    const epoch = ++inventoryEpoch.current;
    const searchTerm = search.trim();
    await queryClient.cancelQueries({ queryKey: ["asset-inventory", sessionCacheKey] });
    setAssetInventoryLoading(true);
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
      if (!append && !pageResult.items.length) setNotice("No governed assets match this search.");
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      if (epoch !== inventoryEpoch.current) return;
      setNotice(recoveryMessage(error, "Asset inventory could not be loaded. Your current editor state remains unchanged."));
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
      await controlPlane.logout(controller.signal);
      setLogoutPending(false);
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
    void queryClient.cancelQueries();
    queryClient.clear();
    previousSessionCacheKey.current = "anonymous";
    draftEditEpoch.current += 1;
    setSession(null); setAsset(null); setAssets([]); resetRuleHistory([]); setPreview(null);
    setManagementData({}); setAssetCursor(null); setAssetHasMore(false); setAssetSearch("");
    setManagementDirty(false);
      setDraftRevision(0); setDraftId(null); setReviewToken(null); setSaveState("saved");
    setPublishPending(false);
    setWorkspace("unavailable");
    void controlPlane.getSessionOptions().then((options) => {
      setSessionOptions(options);
      setAuthConfig(options.oidc);
    }).catch(() => {
      void controlPlane.getUiAuthConfig().then(setAuthConfig).catch(() => setAuthConfig(null));
    });
  }

  function navigateTo(next: Page) {
    if (!confirmDiscardUnsaved()) return;
    setMobileNavOpen(false);
    if (pageFromHash(window.location.hash) !== next) window.location.hash = next;
    else setPage(next);
  }

  useEffect(() => {
    if (mobileNavOpen) return;
    mobileNavTrigger.current?.focus();
  }, [mobileNavOpen]);

  function runPaletteCommand(command: Page | "help") {
    closePalette();
    if (command === "help") {
      setNotice("Use the navigation destinations to inspect governed assets, author policies, and review staged changes.");
      return;
    }
    navigateTo(command);
  }

  function openPaletteAsset(assetId: string) {
    closePalette();
    if (!confirmDiscardUnsaved()) return;
    const target = assets.find((item) => item.id === assetId);
    if (!target) return;
    setReviewOnly(false);
    if (page !== "assets") window.location.hash = "assets";
    void loadAsset(target.id, assets);
  }

  function closePalette() {
    setPaletteOpen(false);
    window.requestAnimationFrame(() => paletteReturnFocus.current?.focus());
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

  function confirmDiscardUnsaved() {
    if (saveState !== "unsaved" && !managementDirty) return true;
    const confirmed = window.confirm("You have unsaved changes. Leave this editor?");
    if (confirmed) setManagementDirty(false);
    return confirmed;
  }

  function invalidateAssetQueries(assetId: string) {
    void queryClient.invalidateQueries({ queryKey: ["asset", sessionCacheKey, assetId] });
    void queryClient.invalidateQueries({ queryKey: ["asset-inventory", sessionCacheKey] });
    void queryClient.invalidateQueries({ queryKey: ["management", sessionCacheKey] });
  }

  async function loadAsset(
    assetId: string,
    knownAssets = assets,
    inheritedEpoch?: number,
    selectedDraftId?: string,
    inheritedSessionScope = sessionCacheKey,
  ) {
    const epoch = inheritedEpoch ?? ++loadEpoch.current;
    await queryClient.cancelQueries({ queryKey: ["asset", inheritedSessionScope] });
    try {
      const assetKey = ["asset", inheritedSessionScope, assetId] as const;
      const [fullAsset, schema, history, grants, access] = await Promise.all([
        queryClient.fetchQuery({ queryKey: [...assetKey, "detail"], queryFn: ({ signal }) => controlPlane.getAsset(assetId, signal) }),
        queryClient.fetchQuery({ queryKey: [...assetKey, "schema"], queryFn: ({ signal }) => controlPlane.getSchema(assetId, signal) }),
        queryClient.fetchQuery({ queryKey: [...assetKey, "history"], queryFn: ({ signal }) => controlPlane.listAssetHistory(assetId, signal) }),
        queryClient.fetchQuery({ queryKey: [...assetKey, "grants"], queryFn: ({ signal }) => controlPlane.listGrants(assetId, signal) }),
        queryClient.fetchQuery({ queryKey: [...assetKey, "access"], queryFn: ({ signal }) => controlPlane.getAssetAccess(assetId, signal) }),
      ]);
      if (epoch !== loadEpoch.current) return;
      const draft = await queryClient.fetchQuery({
        queryKey: [...assetKey, "draft", selectedDraftId ?? "current"],
        queryFn: ({ signal }) => controlPlane.getDraft(assetId, selectedDraftId, signal),
      });
      if (epoch !== loadEpoch.current) return;
      const effectiveRules = draft?.rules ?? [];
      setManagementData((current) => ({ ...current, history, grants, access }));
      const hydratedAsset: Asset = { ...fullAsset, schema };
      setAssets(knownAssets); setAsset(hydratedAsset); resetRuleHistory(effectiveRules); setDraftRevision(draft?.revision ?? 0); setDraftId(draft?.id ?? null); setSelectedRule(0); setReviewToken(null);
      draftEditEpoch.current += 1;
      setSelectedField(schema.fields[0]?.human_path ?? hydratedAsset.schema_fields[0]?.name ?? ""); setPreview(null); setSaveState("saved");
      setNotice(selectedDraftId ? `Loaded saved draft ${draft?.revision ?? 0} for read-only review.` : effectiveRules.length ? "Loaded your policy draft." : "No policy draft exists yet. Add a rule to begin authoring.");
    } catch (error) {
      if ((error instanceof DOMException && error.name === "AbortError") || (error instanceof Error && error.name === "CancelledError")) return;
      if (inheritedEpoch !== undefined && epoch !== loadEpoch.current) return;
      setNotice(recoveryMessage(error, "Could not load this asset and its access metadata. Your previous editor state remains unchanged."));
    }
  }

  const activeRule = rules[selectedRule];
  const selectedMask = activeRule?.masks[selectedField];
  const effectiveFields = useMemo(() => new Set(rules.flatMap((rule) => rule.columns)), [rules]);
  function replaceRules(next: PolicyRule[], record = true) {
    if (record) {
      rulesUndoStack.current = [...rulesUndoStack.current, rulesRef.current].slice(-100);
      rulesRedoStack.current = [];
    }
    rulesRef.current = next;
    setRules(next);
  }
  function resetRuleHistory(next: PolicyRule[]) {
    rulesUndoStack.current = [];
    rulesRedoStack.current = [];
    rulesRef.current = next;
    setRules(next);
  }
  function undoRules() {
    const previous = rulesUndoStack.current.pop();
    if (!previous) return;
    rulesRedoStack.current.push(rulesRef.current);
    replaceRules(previous, false);
    draftEditEpoch.current += 1;
    setSaveState("unsaved"); setPreview(null); setReviewToken(null);
    setNotice("Undid the last local policy edit. Save the draft to persist this version.");
  }
  function redoRules() {
    const next = rulesRedoStack.current.pop();
    if (!next) return;
    rulesUndoStack.current.push(rulesRef.current);
    replaceRules(next, false);
    draftEditEpoch.current += 1;
    setSaveState("unsaved"); setPreview(null); setReviewToken(null);
    setNotice("Reapplied the local policy edit. Save the draft to persist this version.");
  }
  function updateRule(change: (rule: PolicyRule) => PolicyRule) {
    if (!activeRule) return;
    replaceRules(rulesRef.current.map((rule, index) => index === selectedRule ? change(rule) : rule));
    draftEditEpoch.current += 1;
    setSaveState("unsaved"); setPreview(null); setReviewToken(null); setNotice("Draft changed. Run a policy test before review.");
  }
  function addRule() {
    const ordinal = Math.max(0, ...rules.map((rule) => rule.ordinal)) + 10;
    replaceRules([...rulesRef.current, newRule(selectedField, ordinal)]);
    draftEditEpoch.current += 1;
    setSelectedRule(rules.length); setSaveState("unsaved"); setPreview(null);
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
    draftEditEpoch.current += 1;
    setSelectedRule(rulesRef.current.length - 1); setSaveState("unsaved"); setPreview(null); setReviewToken(null);
    setNotice("Rule duplicated locally. Review its principals and fields before saving.");
  }
  function removeRule() {
    if (!activeRule) return;
    replaceRules(rulesRef.current.filter((_, index) => index !== selectedRule));
    draftEditEpoch.current += 1;
    setSelectedRule(Math.max(0, selectedRule - 1)); setSaveState("unsaved"); setPreview(null);
    setNotice("Rule removed locally. Save the draft to persist the change.");
  }
  function moveRule(index: number, direction: -1 | 1) {
    const nextIndex = index + direction;
    if (!rules[index] || nextIndex < 0 || nextIndex >= rules.length) return;
    const next = [...rulesRef.current];
    [next[index], next[nextIndex]] = [next[nextIndex], next[index]];
    replaceRules(next.map((rule, index) => ({ ...rule, ordinal: (index + 1) * 10 })));
    draftEditEpoch.current += 1;
    setSelectedRule(nextIndex); setSaveState("unsaved"); setPreview(null); setReviewToken(null);
    setNotice("Rule order changed locally. Save the draft to persist precedence.");
  }
  function toggleField(name: string) {
    updateRule((rule) => ({ ...rule, columns: rule.columns.includes(name) ? rule.columns.filter((column) => column !== name) : [...rule.columns, name] }));
  }
  function setMask(mask: Mask | undefined) {
    updateRule((rule) => {
      const masks = { ...rule.masks };
      if (mask) masks[selectedField] = mask; else delete masks[selectedField];
      return { ...rule, masks };
    });
  }
  async function saveDraft() {
    if (!asset || saveDraftPending.current) return;
    saveDraftPending.current = true;
    const assetId = asset.id;
    const editEpoch = draftEditEpoch.current;
    const revision = draftRevision;
    const draftIdentity = draftId;
    const loadScope = loadEpoch.current;
    setSaveState("saving");
    const controller = beginMutation();
    try {
      const saved = await controlPlane.saveDraft(assetId, revision, rules, controller.signal);
      if (loadScope !== loadEpoch.current || editEpoch !== draftEditEpoch.current || draftIdentity !== draftId) {
        if (loadScope === loadEpoch.current) setSaveState("unsaved");
        return;
      }
      setDraftRevision(saved.revision);
      setDraftId(saved.id);
      if (loadScope !== loadEpoch.current || editEpoch !== draftEditEpoch.current || draftIdentity !== draftId) {
        if (loadScope === loadEpoch.current) setSaveState("unsaved");
        return;
      }
      setSaveState("saved"); setReviewToken(null); setNotice("Policy draft saved to the control plane.");
      setFieldErrors([]);
      invalidateAssetQueries(assetId);
    } catch (error) {
      if (isAbortError(error)) return;
      if (loadScope !== loadEpoch.current || editEpoch !== draftEditEpoch.current || draftIdentity !== draftId) return;
      setSaveState("failed"); setFieldErrors((error as { fieldErrors?: Array<{ field: string; message: string; type: string }> }).fieldErrors ?? []); setNotice(recoveryMessage(error, "Save failed. The unsaved draft remains in this browser."));
    } finally {
      finishMutation(controller);
      saveDraftPending.current = false;
    }
  }
  async function runPreview() {
    if (!asset) return;
    if (saveState !== "saved") {
      setNotice("Save the draft before running a server-side policy test.");
      return;
    }
    if (previewPendingRef.current) return;
    previewPendingRef.current = true;
    const loadScope = loadEpoch.current;
    const editScope = draftEditEpoch.current;
    const draftIdentity = { id: draftId, revision: draftRevision };
    const controller = beginMutation();
    try {
      let claims: Record<string, unknown> = {};
      if (previewClaims.trim()) {
        const parsed = JSON.parse(previewClaims);
        if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") throw new Error("Claims must be a JSON object");
        claims = parsed as Record<string, unknown>;
      }
      const result: Preview = await controlPlane.evaluate(asset.id, { principal: previewPrincipal.trim(), groups: previewGroups.split(",").map((value) => value.trim()).filter(Boolean), claims, draft_id: draftId ?? undefined, draft_revision: draftRevision }, controller.signal);
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current || draftIdentity.id !== draftId || draftIdentity.revision !== draftRevision) return;
      setPreview(result); setReviewToken(null); setNotice(`Server-side evaluation completed: ${result.decision === "allow" ? "allowed" : "denied"}.`);
    } catch (error) {
      if (isAbortError(error)) return;
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current || draftIdentity.id !== draftId || draftIdentity.revision !== draftRevision) return;
      setPreview(null); setReviewToken(null); setNotice(recoveryMessage(error, "Policy test could not run. This draft is not validated."));
    } finally {
      finishMutation(controller);
      previewPendingRef.current = false;
    }
  }

  async function requestReview() {
    if (!asset || saveState !== "saved" || publishPending || reviewPendingRef.current) return;
    reviewPendingRef.current = true;
    const loadScope = loadEpoch.current;
    const editScope = draftEditEpoch.current;
    const draftIdentity = { id: draftId, revision: draftRevision };
    const controller = beginMutation();
    try {
      const claims = JSON.parse(previewClaims || "{}") as Record<string, object>;
      if (!claims || Array.isArray(claims) || typeof claims !== "object") throw new Error("Claims must be a JSON object");
      const result = await controlPlane.review(asset.id, { principal: previewPrincipal.trim(), groups: previewGroups.split(",").map((value) => value.trim()).filter(Boolean), claims, draft_id: draftId ?? undefined, draft_revision: draftRevision }, controller.signal);
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current || draftIdentity.id !== draftId || draftIdentity.revision !== draftRevision) return;
      setPreview(result); setReviewToken(result.review_token ?? null); setNotice("Server review is current for this saved draft revision. You can publish it now.");
    } catch (error) {
      if (isAbortError(error)) return;
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current || draftIdentity.id !== draftId || draftIdentity.revision !== draftRevision) return;
      setReviewToken(null); setNotice(recoveryMessage(error, "Review was rejected. Run a successful test against the saved draft and resolve any policy or schema errors."));
    } finally {
      finishMutation(controller);
      reviewPendingRef.current = false;
    }
  }

  async function publishAsset() {
    if (!asset || !reviewToken || publishPending || publishPendingRef.current) return;
    publishPendingRef.current = true;
    const loadScope = loadEpoch.current;
    const editScope = draftEditEpoch.current;
    const draftIdentity = { id: draftId, revision: draftRevision };
    const idempotencyKey = crypto.randomUUID();
    setPublishPending(true);
    const controller = beginMutation();
    try {
      await controlPlane.publishAsset(asset.id, draftRevision, reviewToken, idempotencyKey, draftId ?? undefined, controller.signal);
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current || draftIdentity.id !== draftId || draftIdentity.revision !== draftRevision) return;
      setReviewToken(null);
      setNotice("Published the saved draft.");
      invalidateAssetQueries(asset.id);
      void refreshAssetInventory(assetSearch);
    } catch (error) {
      if (isAbortError(error)) return;
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current || draftIdentity.id !== draftId || draftIdentity.revision !== draftRevision) return;
      try {
        const operation = await controlPlane.getPublicationOperation(asset.id, idempotencyKey, controller.signal);
        if (operation.status === "committed") {
          setReviewToken(null);
          setNotice(`Publish committed as policy version ${operation.result.policy_version}.`);
          invalidateAssetQueries(asset.id);
          void refreshAssetInventory(assetSearch);
        } else {
          setNotice("Publish outcome is still pending. Refresh Activity before retrying.");
        }
      } catch (error) {
        if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current || draftIdentity.id !== draftId || draftIdentity.revision !== draftRevision) return;
        setNotice(recoveryMessage(error, "Publish failed. Review the saved draft and active generation."));
      }
    } finally {
      finishMutation(controller);
      publishPendingRef.current = false;
      if (loadScope === loadEpoch.current) {
        setPublishPending(false);
      }
    }
  }

  async function restorePolicyVersion(policyVersion: number) {
    if (!asset) return;
    if (restorePendingRef.current) return;
    if (saveState === "unsaved" && !window.confirm("You have unsaved policy changes. Restore this published version over them?")) return;
    restorePendingRef.current = true;
    const loadScope = loadEpoch.current;
    const editScope = draftEditEpoch.current;
    const draftIdentity = { id: draftId, revision: draftRevision };
    const controller = beginMutation();
    try {
      const restored = await controlPlane.restorePolicyVersion(asset.id, policyVersion, draftRevision, controller.signal);
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current || draftIdentity.id !== draftId || draftIdentity.revision !== draftRevision) return;
      draftEditEpoch.current += 1;
      replaceRules(restored.rules);
      setDraftRevision(restored.revision);
      setSaveState("saved");
      setPreview(null); setReviewToken(null);
      setNotice(`Version ${policyVersion} restored as draft revision ${restored.revision}. Review and publish it when ready.`);
      invalidateAssetQueries(asset.id);
    } catch (error) {
      if (isAbortError(error)) return;
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current || draftIdentity.id !== draftId || draftIdentity.revision !== draftRevision) return;
      setNotice(recoveryMessage(error, "Restore failed. The draft may have changed; reload the asset before trying again."));
    } finally {
      finishMutation(controller);
      restorePendingRef.current = false;
    }
  }

  const signedOut = workspace === "unavailable" || (workspace === "loading" && !session);
  const canManageWorkspace = Boolean(session?.capabilities.includes("workspace:admin"));
  const paletteCommands: Array<Page | "help"> = ["assets", "changes", "activity", ...(canManageWorkspace ? ["connections", "settings"] as const : []), "help"];
  const paletteAssets = assets.filter((item) => `${item.catalog} ${item.name}`.toLowerCase().includes(paletteQuery.trim().toLowerCase())).slice(0, 8);
  const accessView = <LoginPanel showAuth={workspace === "unavailable"} title={workspace === "loading" ? "Loading governed workspace" : "Sign in to your workspace"} message={workspace === "loading" ? "Checking your workspace access and available assets." : notice} retry={workspace === "unavailable" ? loadInitialWorkspace : undefined} authConfig={authConfig} sessionOptions={sessionOptions} bootstrapToken={bootstrapToken} onBootstrapToken={setBootstrapToken} onBootstrapLogin={() => void bootstrapLogin()} loggingIn={loggingIn} authError={authError} />;
  return <div className="app-shell">
    {mobileNavOpen && <button className="mobile-nav-backdrop" aria-label="Close navigation menu" onClick={() => setMobileNavOpen(false)} />}
    <aside id="primary-navigation" className={mobileNavOpen ? "sidebar open" : "sidebar"} aria-label="Primary navigation">
      <a className="brand" href="#assets" onClick={() => navigateTo("assets")}>DAL OBSCURA<span>GOVERNANCE</span></a>
      <nav>{(["assets", "changes", "activity", "connections", "settings"] as Page[]).map((item) => {
        const requiresWorkspaceAdmin = item === "connections" || item === "settings";
        const disabled = !session || (requiresWorkspaceAdmin && !canManageWorkspace);
        const reason = !session ? "Sign in to open this workspace view" : requiresWorkspaceAdmin && !canManageWorkspace ? "Platform administrator capability required" : undefined;
        const icon: IconName = item === "assets" ? "database" : item === "changes" ? "history" : item === "activity" ? "activity" : item === "connections" ? "plug" : "settings";
        return <button key={item} className={page === item ? "nav-item active" : "nav-item"} onClick={() => navigateTo(item)} disabled={disabled} title={reason} aria-current={page === item ? "page" : undefined}><Icon name={icon} /><span>{item}</span></button>;
      })}</nav>
      <div className="sidebar-foot" role="status" aria-live="polite"><span className={"status-dot " + workspace} /> Workspace: {workspace === "ready" ? "connected" : "unavailable"}<br /><small>{workspaceLabel(workspace)}{asset?.catalog ? ` · catalog ${asset.catalog}` : ""}</small></div>
    </aside>
    <main>
      <header className="topbar"><div className="topbar-title"><button ref={mobileNavTrigger} className="mobile-menu-toggle" type="button" aria-label="Open navigation menu" aria-expanded={mobileNavOpen} aria-controls="primary-navigation" onClick={() => setMobileNavOpen(true)}><Icon name="menu" /></button><div><span className="eyebrow">{page === "assets" ? "ASSET WORKSPACE" : page.toUpperCase()}</span><h1>{page === "assets" ? asset?.name ?? "Assets" : titleFor(page)}</h1></div></div><div className="actor"><span className="avatar">{session?.principal.slice(0, 1).toUpperCase() ?? "?"}</span><div><strong>{session?.principal ?? "Not signed in"}</strong><small>{session?.platform_admin ? "Platform admin" : "Authenticated user"}{session?.issuer ? ` · ${session.issuer}` : ""}</small></div><label className="theme-control"><span className="sr-only">Color theme</span><select aria-label="Color theme" value={theme} onChange={(event) => setTheme(event.target.value as Theme)}><option value="system">System theme</option><option value="light">Light theme</option><option value="dark">Dark theme</option></select></label>{session && <button className="text-button" onClick={() => void logout()}>Sign out</button>}{logoutPending && <button className="text-button" onClick={() => void logout()}>Retry sign out</button>}</div></header>
      {signedOut ? accessView : page !== "assets" ? <ManagementView page={page} data={managementData} loading={managementLoading} error={managementError} onReload={() => void loadManagement(page)} onLoadMore={page === "changes" ? () => void loadMoreHistory() : page === "activity" ? () => void loadMoreAudit() : undefined} historyLoading={historyLoading} auditLoading={auditLoading} filters={auditFilters} onFiltersChange={updateAuditFilters} session={session} queryClient={queryClient} sessionScope={sessionCacheKey} onDirtyChange={setManagementDirty} /> : workspace === "loading" ? accessView : !asset ? <LoginPanel title="No governed assets" message={notice} /> : <AssetWorkspace initialTab={locationFromUrl(window.location.hash, window.location.search).tab} initialVersion={locationFromUrl(window.location.hash, window.location.search).version} assets={assets} asset={asset} access={managementData.access} history={managementData.history ?? []} grants={managementData.grants ?? []} onAsset={(id) => { if (confirmDiscardUnsaved()) { setReviewOnly(false); void loadAsset(id); } }} assetSearch={assetSearch} assetHasMore={assetHasMore} assetInventoryLoading={assetInventoryLoading} onSearch={searchAssets} onLoadMore={() => void refreshAssetInventory(assetSearch, true)} rules={rules} activeRule={activeRule} activeRevision={draftRevision} selectedRule={selectedRule} onRule={setSelectedRule} onMoveRule={moveRule} selectedField={selectedField} onField={setSelectedField} selectedMask={selectedMask} effectiveFields={effectiveFields} saveState={saveState} notice={notice} fieldErrors={fieldErrors} onToggleField={toggleField} onMask={setMask} onUpdateRule={updateRule} onAddRule={addRule} onRemoveRule={removeRule} onDuplicateRule={duplicateRule} onUndo={undoRules} onRedo={redoRules} canUndo={rulesUndoStack.current.length > 0} canRedo={rulesRedoStack.current.length > 0} onSave={() => void saveDraft()} onPreview={() => void runPreview()} onReview={() => void requestReview()} previewPrincipal={previewPrincipal} previewGroups={previewGroups} previewClaims={previewClaims} onPreviewPrincipal={setPreviewPrincipal} onPreviewGroups={setPreviewGroups} onPreviewClaims={setPreviewClaims} onPublish={() => void publishAsset()} publishing={publishPending} onRestore={(version) => void restorePolicyVersion(version)} reviewToken={reviewToken ?? undefined} preview={preview} session={session} onReloadAccess={() => void loadAsset(asset.id, assets, undefined, reviewOnly ? draftId ?? undefined : undefined)} onDirtyChange={setManagementDirty} reviewOnly={reviewOnly} draftId={draftId} queryClient={queryClient} sessionScope={sessionCacheKey} />}
      {paletteOpen && <div className="palette-backdrop" role="presentation" onMouseDown={closePalette}><section className="command-palette" role="dialog" aria-modal="true" aria-label="Command palette" onMouseDown={(event) => event.stopPropagation()}><input autoFocus value={paletteQuery} onChange={(event) => setPaletteQuery(event.target.value)} placeholder="Jump to a destination or search an asset" aria-label="Command search" /><div role="listbox">{paletteCommands.filter((command) => command.includes(paletteQuery.toLowerCase())).map((command) => <button key={command} role="option" onClick={() => runPaletteCommand(command)}>{command === "help" ? "Keyboard and workflow help" : `Open ${titleFor(command)}`}</button>)}{paletteAssets.map((item) => <button key={item.id} role="option" onClick={() => openPaletteAsset(item.id)}><strong>{item.name}</strong><small>{item.catalog} · {item.backend}</small></button>)}{paletteQuery && !paletteCommands.some((command) => command.includes(paletteQuery.toLowerCase())) && !paletteAssets.length && <p className="help">No authorized destination or asset matches that search.</p>}</div><p className="help">Press Escape to close. Publishing, deletion, and revocation are never palette commands.</p></section></div>}
    </main>
  </div>;
}



function titleFor(page: Page) { return ({ changes: "Changes", activity: "Activity", connections: "Connections", settings: "Settings", assets: "Assets" })[page]; }
function saveLabel(state: SaveState) { return ({ saved: "Saved draft", saving: "Saving draft", unsaved: "Unsaved changes", failed: "Save failed" })[state]; }
function workspaceLabel(state: WorkspaceState) { return ({ loading: "Checking access", ready: "Connected", unavailable: "Unavailable" })[state]; }
createRoot(document.getElementById("root")!).render(<App />);
