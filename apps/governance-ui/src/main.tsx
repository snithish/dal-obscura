import { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import type { ApiFailure, Asset, Mask, PolicyRule, Preview, Session, SessionOptions, UiAuthConfig } from "./api";
import { controlPlane } from "./api";
import { isCurrentEpoch } from "./lifecycle";
import { locationFromUrl, pageFromHash, type UiPage } from "./navigation";
import { recoveryMessage } from "./recovery";
import { LoginPanel } from "./components/LoginPanel";
import { SettingsView } from "./components/SettingsView";
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
  const [page, setPage] = useState<Page>(() => pageFromHash(window.location.hash));
  const [workspace, setWorkspace] = useState<WorkspaceState>("loading");
  const [assets, setAssets] = useState<Asset[]>([]);
  const [asset, setAsset] = useState<Asset | null>(null);
  const [assetSearch, setAssetSearch] = useState("");
  const [assetCursor, setAssetCursor] = useState<string | null>(null);
  const [assetHasMore, setAssetHasMore] = useState(false);
  const [assetInventoryLoading, setAssetInventoryLoading] = useState(false);
  const [rules, setRules] = useState<PolicyRule[]>([]);
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
  const [session, setSession] = useState<Session | null>(null);
  const [authConfig, setAuthConfig] = useState<UiAuthConfig | null>(null);
  const [sessionOptions, setSessionOptions] = useState<SessionOptions | null>(null);
  const [bootstrapToken, setBootstrapToken] = useState("");
  const [authError, setAuthError] = useState("");
  const [loggingIn, setLoggingIn] = useState(false);
  const [managementData, setManagementData] = useState<ManagementData>({});
  const [managementLoading, setManagementLoading] = useState(false);
  const [managementError, setManagementError] = useState("");
  const [historyLoading, setHistoryLoading] = useState(false);
  const [auditLoading, setAuditLoading] = useState(false);
  const [auditFilters, setAuditFilters] = useState<AuditFilters>({});
  const [publishPending, setPublishPending] = useState(false);
  const loadEpoch = useRef(0);
  const draftEditEpoch = useRef(0);
  const inventoryEpoch = useRef(0);
  const searchTimer = useRef<number | undefined>(undefined);
  const managementEpoch = useRef(0);
  const assetAbortController = useRef<AbortController | null>(null);
  const managementAbortController = useRef<AbortController | null>(null);
  const workspaceAbortController = useRef<AbortController | null>(null);
  const inventoryAbortController = useRef<AbortController | null>(null);
  const historyAbortController = useRef<AbortController | null>(null);
  const auditAbortController = useRef<AbortController | null>(null);
  const [logoutPending, setLogoutPending] = useState(false);
  useEffect(() => {
    void loadInitialWorkspace();
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
    assetAbortController.current?.abort();
    inventoryAbortController.current?.abort();
    managementAbortController.current?.abort();
    workspaceAbortController.current?.abort();
    historyAbortController.current?.abort();
    auditAbortController.current?.abort();
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
  }, [asset?.id, assets, page, saveState, session]);

  useEffect(() => {
    const handleAuthExpired = () => {
      loadEpoch.current += 1;
      inventoryEpoch.current += 1;
      managementEpoch.current += 1;
      assetAbortController.current?.abort();
      inventoryAbortController.current?.abort();
      managementAbortController.current?.abort();
      workspaceAbortController.current?.abort();
      historyAbortController.current?.abort();
      auditAbortController.current?.abort();
      clearPrivateState();
      setNotice("Your session expired or was revoked. Sign in again to continue.");
    };
    window.addEventListener("dal-obscura-auth-expired", handleAuthExpired);
    return () => window.removeEventListener("dal-obscura-auth-expired", handleAuthExpired);
  }, []);

  useEffect(() => {
    if (saveState !== "unsaved") return;
    const warn = (event: BeforeUnloadEvent) => {
      event.preventDefault();
      event.returnValue = "";
    };
    window.addEventListener("beforeunload", warn);
    return () => window.removeEventListener("beforeunload", warn);
  }, [saveState]);

  useEffect(() => {
    if (page === "assets" || !session) return;
    void loadManagement(page);
  }, [page, session, auditFilters]);

  async function loadManagement(destination: Page) {
    const epoch = ++managementEpoch.current;
    managementAbortController.current?.abort();
    const controller = new AbortController();
    managementAbortController.current = controller;
    setManagementLoading(true);
    setManagementError("");
    try {
      let next: ManagementData = {};
      if (destination === "changes") { const pageResult = await controlPlane.listHistoryPage({ limit: 50, signal: controller.signal }); next = { history: pageResult.items, historyNextCursor: pageResult.next_cursor }; }
      if (destination === "activity") { const audit = await controlPlane.listAuditEventsPage({ limit: 50, ...auditFilters, signal: controller.signal }); next = { history: await controlPlane.listHistory(controller.signal), events: audit.items, eventsNextCursor: audit.next_cursor, summary: await controlPlane.getSummary(controller.signal), observations: await controlPlane.getObservations(controller.signal) }; }
      if (destination === "connections") { const pluginData = await controlPlane.listPlugins(controller.signal); next = { catalogs: await controlPlane.listCatalogs(controller.signal), publications: session?.platform_admin ? await controlPlane.listWorkspacePublications(controller.signal) : [], plugins: pluginData.plugins, pluginStates: pluginData.states, pluginPairs: pluginData.pairs }; }
      if (destination === "settings") { const [runtime, providers, revision] = await Promise.all([controlPlane.getRuntimeSettings(controller.signal), controlPlane.getAuthProviders(controller.signal), controlPlane.getAuthProviderRevision(controller.signal)]); next = { runtime, providers, providerRevision: revision.revision, publications: session?.platform_admin ? await controlPlane.listWorkspacePublications(controller.signal) : [] }; }
      if (!isCurrentEpoch(epoch, managementEpoch.current)) return;
      setManagementData(next);
    } catch (error) {
      if (!isCurrentEpoch(epoch, managementEpoch.current)) return;
      if (error instanceof DOMException && error.name === "AbortError") return;
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
    historyAbortController.current?.abort();
    const controller = new AbortController();
    historyAbortController.current = controller;
    setHistoryLoading(true);
    try {
      const pageResult = await controlPlane.listHistoryPage({ limit: 50, cursor, signal: controller.signal });
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setManagementData((current) => ({ ...current, history: [...(current.history ?? []), ...pageResult.items], historyNextCursor: pageResult.next_cursor }));
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setNotice(recoveryMessage(error, "More history could not be loaded. The entries already visible remain available."));
    } finally {
      if (scope === managementEpoch.current) setHistoryLoading(false);
      if (controller === historyAbortController.current) historyAbortController.current = null;
    }
  }

  async function loadMoreAudit() {
    const cursor = managementData.eventsNextCursor;
    if (!cursor || auditLoading) return;
    const scope = managementEpoch.current;
    auditAbortController.current?.abort();
    const controller = new AbortController();
    auditAbortController.current = controller;
    setAuditLoading(true);
    try {
      const pageResult = await controlPlane.listAuditEventsPage({ limit: 50, cursor, ...auditFilters, signal: controller.signal });
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setManagementData((current) => ({ ...current, events: [...(current.events ?? []), ...pageResult.items], eventsNextCursor: pageResult.next_cursor }));
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setNotice(recoveryMessage(error, "More activity could not be loaded. The entries already visible remain available."));
    } finally {
      if (scope === managementEpoch.current) setAuditLoading(false);
      if (controller === auditAbortController.current) auditAbortController.current = null;
    }
  }

  function updateAuditFilters(next: AuditFilters) {
    managementEpoch.current += 1;
    historyAbortController.current?.abort();
    auditAbortController.current?.abort();
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
      setSession(loadedSession);
      const loadedPage = await controlPlane.listAssetPage({ limit: 50, signal: controller.signal });
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
      await loadAsset(selected.id, loaded, epoch, requestedDraftId ?? undefined);
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
    inventoryAbortController.current?.abort();
    const controller = new AbortController();
    inventoryAbortController.current = controller;
    setAssetInventoryLoading(true);
    try {
      const pageResult = await controlPlane.listAssetPage({
        limit: 50,
        cursor: append ? assetCursor ?? undefined : undefined,
        search: search.trim() || undefined,
        signal: controller.signal,
      });
      if (epoch !== inventoryEpoch.current) return;
      setAssets((current) => append ? [...current, ...pageResult.items] : pageResult.items);
      setAssetCursor(pageResult.next_cursor);
      setAssetHasMore(Boolean(pageResult.next_cursor));
      if (!append && !pageResult.items.length) setNotice("No governed assets match this search.");
    } catch (error) {
      if (controller.signal.aborted) return;
      if (epoch !== inventoryEpoch.current) return;
      setNotice(recoveryMessage(error, "Asset inventory could not be loaded. Your current editor state remains unchanged."));
    } finally {
      if (epoch === inventoryEpoch.current) setAssetInventoryLoading(false);
      if (controller === inventoryAbortController.current) inventoryAbortController.current = null;
    }
  }

  function searchAssets(value: string) {
    setAssetSearch(value);
    if (searchTimer.current !== undefined) window.clearTimeout(searchTimer.current);
    searchTimer.current = window.setTimeout(() => void refreshAssetInventory(value), 250);
  }

  async function bootstrapLogin() {
    const token = bootstrapToken.trim();
    if (!token) {
      setAuthError("Enter the local control-plane token to continue.");
      return;
    }
    setLoggingIn(true);
    setAuthError("");
    try {
      await controlPlane.bootstrapLogin(token);
      setBootstrapToken("");
      setAuthError("");
      loadEpoch.current += 1;
      await loadInitialWorkspace();
    } catch (error) {
      setAuthError((error as { status?: number })?.status === 429
        ? "Too many sign-in attempts. Wait a moment and try again."
        : "That local token was not accepted. Check the control-plane configuration and try again.");
      setWorkspace("unavailable");
      setNotice("Sign-in failed. No policy data was loaded.");
    } finally {
      setLoggingIn(false);
    }
  }

  async function logout() {
    loadEpoch.current += 1;
    inventoryEpoch.current += 1;
    managementEpoch.current += 1;
    assetAbortController.current?.abort();
    inventoryAbortController.current?.abort();
    managementAbortController.current?.abort();
    workspaceAbortController.current?.abort();
    historyAbortController.current?.abort();
    auditAbortController.current?.abort();
    if (searchTimer.current !== undefined) {
      window.clearTimeout(searchTimer.current);
      searchTimer.current = undefined;
    }
    // Fence and clear private state before awaiting network revocation. A slow
    // server response must never leave policy data visible or let a late request
    // repopulate the previous session.
    clearPrivateState();
    try {
      await controlPlane.logout();
      setLogoutPending(false);
      setNotice("Signed out. No policy data remains loaded in this browser.");
    } catch {
      setLogoutPending(true);
      setNotice("Sign out could not be confirmed. Private data is hidden; retry sign out before closing this browser.");
    }
  }

  function clearPrivateState() {
    draftEditEpoch.current += 1;
    setSession(null); setAsset(null); setAssets([]); setRules([]); setPreview(null);
    setManagementData({}); setAssetCursor(null); setAssetHasMore(false); setAssetSearch("");
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
    if (pageFromHash(window.location.hash) !== next) window.location.hash = next;
    else setPage(next);
  }

  function runPaletteCommand(command: Page | "help") {
    closePalette();
    if (command === "help") {
      setNotice("Use the navigation destinations to inspect governed assets, author policies, and review staged changes.");
      return;
    }
    navigateTo(command);
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
    return saveState !== "unsaved" || window.confirm("You have unsaved policy changes. Leave this editor?");
  }

  async function loadAsset(
    assetId: string,
    knownAssets = assets,
    inheritedEpoch?: number,
    selectedDraftId?: string,
  ) {
    const epoch = inheritedEpoch ?? ++loadEpoch.current;
    assetAbortController.current?.abort();
    const controller = new AbortController();
    assetAbortController.current = controller;
    try {
      const [fullAsset, schema, history, grants, access] = await Promise.all([controlPlane.getAsset(assetId, controller.signal), controlPlane.getSchema(assetId, controller.signal), controlPlane.listAssetHistory(assetId, controller.signal), controlPlane.listGrants(assetId, controller.signal), controlPlane.getAssetAccess(assetId, controller.signal)]);
      if (epoch !== loadEpoch.current) return;
      fullAsset.schema = schema;
      const draft = await controlPlane.getDraft(assetId, selectedDraftId, controller.signal);
      if (epoch !== loadEpoch.current) return;
      const effectiveRules = draft?.rules ?? [];
      setManagementData((current) => ({ ...current, history, grants, access }));
      setAssets(knownAssets); setAsset(fullAsset); setRules(effectiveRules); setDraftRevision(draft?.revision ?? 0); setDraftId(draft?.id ?? null); setSelectedRule(0); setReviewToken(null);
      draftEditEpoch.current += 1;
      setSelectedField(schema.fields[0]?.human_path ?? fullAsset.schema_fields[0]?.name ?? ""); setPreview(null); setSaveState("saved");
      setNotice(selectedDraftId ? `Loaded saved draft ${draft?.revision ?? 0} for read-only review.` : effectiveRules.length ? "Loaded your policy draft." : "No policy draft exists yet. Add a rule to begin authoring.");
    } catch (error) {
      if (error instanceof DOMException && error.name === "AbortError") return;
      if (inheritedEpoch !== undefined && epoch !== loadEpoch.current) return;
      setNotice(recoveryMessage(error, "Could not load this asset and its access metadata. Your previous editor state remains unchanged."));
    }
  }

  const activeRule = rules[selectedRule];
  const selectedMask = activeRule?.masks[selectedField];
  const effectiveFields = useMemo(() => new Set(rules.flatMap((rule) => rule.columns)), [rules]);
  function updateRule(change: (rule: PolicyRule) => PolicyRule) {
    if (!activeRule) return;
    setRules((current) => current.map((rule, index) => index === selectedRule ? change(rule) : rule));
    draftEditEpoch.current += 1;
    setSaveState("unsaved"); setPreview(null); setReviewToken(null); setNotice("Draft changed. Run a policy test before review.");
  }
  function addRule() {
    const ordinal = Math.max(0, ...rules.map((rule) => rule.ordinal)) + 10;
    setRules((current) => [...current, newRule(selectedField, ordinal)]);
    draftEditEpoch.current += 1;
    setSelectedRule(rules.length); setSaveState("unsaved"); setPreview(null);
    setNotice("New rule added locally. Add at least one principal before saving.");
  }
  function removeRule() {
    if (!activeRule) return;
    setRules((current) => current.filter((_, index) => index !== selectedRule));
    draftEditEpoch.current += 1;
    setSelectedRule(Math.max(0, selectedRule - 1)); setSaveState("unsaved"); setPreview(null);
    setNotice("Rule removed locally. Save the draft to persist the change.");
  }
  function moveRule(index: number, direction: -1 | 1) {
    const nextIndex = index + direction;
    if (!rules[index] || nextIndex < 0 || nextIndex >= rules.length) return;
    setRules((current) => {
      const next = [...current];
      [next[index], next[nextIndex]] = [next[nextIndex], next[index]];
      return next.map((rule, index) => ({ ...rule, ordinal: (index + 1) * 10 }));
    });
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
    if (!asset) return;
    const assetId = asset.id;
    const editEpoch = draftEditEpoch.current;
    const revision = draftRevision;
    const loadScope = loadEpoch.current;
    setSaveState("saving");
    try {
      const saved = await controlPlane.saveDraft(assetId, revision, rules);
      if (loadScope !== loadEpoch.current || editEpoch !== draftEditEpoch.current) {
        if (loadScope === loadEpoch.current) setSaveState("unsaved");
        return;
      }
      setDraftRevision(saved.revision);
      setDraftId(saved.id);
      if (loadScope !== loadEpoch.current || editEpoch !== draftEditEpoch.current) {
        if (loadScope === loadEpoch.current) setSaveState("unsaved");
        return;
      }
      setSaveState("saved"); setReviewToken(null); setNotice("Policy draft saved to the control plane.");
    } catch (error) {
      if (loadScope !== loadEpoch.current || editEpoch !== draftEditEpoch.current) return;
      setSaveState("failed"); setNotice(recoveryMessage(error, "Save failed. The unsaved draft remains in this browser."));
    }
  }
  async function runPreview() {
    if (!asset) return;
    if (saveState !== "saved") {
      setNotice("Save the draft before running a server-side policy test.");
      return;
    }
    const loadScope = loadEpoch.current;
    const editScope = draftEditEpoch.current;
    try {
      let claims: Record<string, unknown> = {};
      if (previewClaims.trim()) {
        const parsed = JSON.parse(previewClaims);
        if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") throw new Error("Claims must be a JSON object");
        claims = parsed as Record<string, unknown>;
      }
      const result: Preview = await controlPlane.evaluate(asset.id, { principal: previewPrincipal.trim(), groups: previewGroups.split(",").map((value) => value.trim()).filter(Boolean), claims, draft_id: draftId ?? undefined, draft_revision: draftRevision });
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setPreview(result); setReviewToken(null); setNotice(`Server-side evaluation completed: ${result.decision === "allow" ? "allowed" : "denied"}.`);
    } catch (error) {
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setPreview(null); setReviewToken(null); setNotice(recoveryMessage(error, "Policy test could not run. This draft is not validated."));
    }
  }

  async function requestReview() {
    if (!asset || saveState !== "saved" || publishPending) return;
    const loadScope = loadEpoch.current;
    const editScope = draftEditEpoch.current;
    try {
      const claims = JSON.parse(previewClaims || "{}") as Record<string, object>;
      if (!claims || Array.isArray(claims) || typeof claims !== "object") throw new Error("Claims must be a JSON object");
      const result = await controlPlane.review(asset.id, { principal: previewPrincipal.trim(), groups: previewGroups.split(",").map((value) => value.trim()).filter(Boolean), claims, draft_id: draftId ?? undefined, draft_revision: draftRevision });
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setPreview(result); setReviewToken(result.review_token ?? null); setNotice("Server review is current for this saved draft revision. You can publish it now.");
    } catch (error) {
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setReviewToken(null); setNotice(recoveryMessage(error, "Review was rejected. Run a successful test against the saved draft and resolve any policy or schema errors."));
    }
  }

  async function publishAsset() {
    if (!asset || !reviewToken || publishPending) return;
    const loadScope = loadEpoch.current;
    const editScope = draftEditEpoch.current;
    const idempotencyKey = crypto.randomUUID();
    setPublishPending(true);
    try {
      await controlPlane.publishAsset(asset.id, draftRevision, reviewToken, idempotencyKey, draftId ?? undefined);
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setReviewToken(null);
      setNotice("Published the saved draft.");
    } catch {
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      try {
        const operation = await controlPlane.getPublicationOperation(asset.id, idempotencyKey);
        if (operation.status === "committed") {
          setReviewToken(null);
          setNotice(`Publish committed as policy version ${operation.result.policy_version}.`);
        } else {
          setNotice("Publish outcome is still pending. Refresh Activity before retrying.");
        }
      } catch (error) {
        if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
        setNotice(recoveryMessage(error, "Publish failed. Review the saved draft and active generation."));
      }
    } finally {
      if (loadScope === loadEpoch.current) {
        setPublishPending(false);
      }
    }
  }

  async function restorePolicyVersion(policyVersion: number) {
    if (!asset) return;
    if (saveState === "unsaved" && !window.confirm("You have unsaved policy changes. Restore this published version over them?")) return;
    const loadScope = loadEpoch.current;
    const editScope = draftEditEpoch.current;
    try {
      const restored = await controlPlane.restorePolicyVersion(asset.id, policyVersion, draftRevision);
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      draftEditEpoch.current += 1;
      setRules(restored.rules);
      setDraftRevision(restored.revision);
      setSaveState("saved");
      setPreview(null); setReviewToken(null);
      setNotice(`Version ${policyVersion} restored as draft revision ${restored.revision}. Review and publish it when ready.`);
    } catch (error) {
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setNotice(recoveryMessage(error, "Restore failed. The draft may have changed; reload the asset before trying again."));
    }
  }

  const signedOut = workspace === "unavailable" || (workspace === "loading" && !session);
  const canManageWorkspace = Boolean(session?.capabilities.includes("workspace:admin"));
  const accessView = <LoginPanel showAuth={workspace === "unavailable"} title={workspace === "loading" ? "Loading governed workspace" : "Sign in to your workspace"} message={workspace === "loading" ? "Checking your workspace access and available assets." : notice} retry={workspace === "unavailable" ? loadInitialWorkspace : undefined} authConfig={authConfig} sessionOptions={sessionOptions} bootstrapToken={bootstrapToken} onBootstrapToken={setBootstrapToken} onBootstrapLogin={() => void bootstrapLogin()} loggingIn={loggingIn} authError={authError} />;
  return <div className="app-shell">
    <aside className="sidebar" aria-label="Primary navigation">
      <a className="brand" href="#assets" onClick={() => navigateTo("assets")}>DAL OBSCURA<span>GOVERNANCE</span></a>
      <nav>{(["assets", "changes", "activity", "connections", "settings"] as Page[]).map((item) => {
        const requiresWorkspaceAdmin = item === "connections" || item === "settings";
        const disabled = !session || (requiresWorkspaceAdmin && !canManageWorkspace);
        const reason = !session ? "Sign in to open this workspace view" : requiresWorkspaceAdmin && !canManageWorkspace ? "Platform administrator capability required" : undefined;
        const icon: IconName = item === "assets" ? "database" : item === "changes" ? "history" : item === "activity" ? "activity" : item === "connections" ? "plug" : "settings";
        return <button key={item} className={page === item ? "nav-item active" : "nav-item"} onClick={() => navigateTo(item)} disabled={disabled} title={reason}><Icon name={icon} /><span>{item}</span></button>;
      })}</nav>
      <div className="sidebar-foot"><span className={"status-dot " + workspace} /> Workspace: {workspace === "ready" ? "connected" : "unavailable"}<br /><small>{workspaceLabel(workspace)}{asset?.catalog ? ` · catalog ${asset.catalog}` : ""}</small></div>
    </aside>
    <main>
      <header className="topbar"><div><span className="eyebrow">{page === "assets" ? "ASSET WORKSPACE" : page.toUpperCase()}</span><h1>{page === "assets" ? asset?.name ?? "Assets" : titleFor(page)}</h1></div><div className="actor"><span className="avatar">{session?.principal.slice(0, 1).toUpperCase() ?? "?"}</span><div><strong>{session?.principal ?? "Not signed in"}</strong><small>{session?.platform_admin ? "Platform admin" : "Authenticated user"}{session?.issuer ? ` · ${session.issuer}` : ""}</small></div><label className="theme-control"><span className="sr-only">Color theme</span><select aria-label="Color theme" value={theme} onChange={(event) => setTheme(event.target.value as Theme)}><option value="system">System theme</option><option value="light">Light theme</option><option value="dark">Dark theme</option></select></label>{session && <button className="text-button" onClick={() => void logout()}>Sign out</button>}{logoutPending && <button className="text-button" onClick={() => void logout()}>Retry sign out</button>}</div></header>
      {signedOut ? accessView : page !== "assets" ? <ManagementView page={page} data={managementData} loading={managementLoading} error={managementError} onReload={() => void loadManagement(page)} onLoadMore={page === "changes" ? () => void loadMoreHistory() : page === "activity" ? () => void loadMoreAudit() : undefined} historyLoading={historyLoading} auditLoading={auditLoading} filters={auditFilters} onFiltersChange={updateAuditFilters} session={session} /> : workspace === "loading" ? accessView : !asset ? <LoginPanel title="No governed assets" message={notice} /> : <AssetWorkspace initialTab={locationFromUrl(window.location.hash, window.location.search).tab} initialVersion={locationFromUrl(window.location.hash, window.location.search).version} assets={assets} asset={asset} access={managementData.access} history={managementData.history ?? []} grants={managementData.grants ?? []} onAsset={(id) => { if (confirmDiscardUnsaved()) { setReviewOnly(false); void loadAsset(id); } }} assetSearch={assetSearch} assetHasMore={assetHasMore} assetInventoryLoading={assetInventoryLoading} onSearch={searchAssets} onLoadMore={() => void refreshAssetInventory(assetSearch, true)} rules={rules} activeRule={activeRule} activeRevision={draftRevision} selectedRule={selectedRule} onRule={setSelectedRule} onMoveRule={moveRule} selectedField={selectedField} onField={setSelectedField} selectedMask={selectedMask} effectiveFields={effectiveFields} saveState={saveState} notice={notice} onToggleField={toggleField} onMask={setMask} onUpdateRule={updateRule} onAddRule={addRule} onRemoveRule={removeRule} onSave={() => void saveDraft()} onPreview={() => void runPreview()} onReview={() => void requestReview()} previewPrincipal={previewPrincipal} previewGroups={previewGroups} previewClaims={previewClaims} onPreviewPrincipal={setPreviewPrincipal} onPreviewGroups={setPreviewGroups} onPreviewClaims={setPreviewClaims} onPublish={() => void publishAsset()} publishing={publishPending} onRestore={(version) => void restorePolicyVersion(version)} reviewToken={reviewToken ?? undefined} preview={preview} session={session} onReloadAccess={() => void loadAsset(asset.id, assets, undefined, reviewOnly ? draftId ?? undefined : undefined)} reviewOnly={reviewOnly} draftId={draftId} />}
      {paletteOpen && <div className="palette-backdrop" role="presentation" onMouseDown={closePalette}><section className="command-palette" role="dialog" aria-modal="true" aria-label="Command palette" onMouseDown={(event) => event.stopPropagation()}><input autoFocus value={paletteQuery} onChange={(event) => setPaletteQuery(event.target.value)} placeholder="Jump to a destination or search help" aria-label="Command search" /><div role="listbox">{(["assets", "connections", "activity", "settings", "changes", "help"] as const).filter((command) => command.includes(paletteQuery.toLowerCase())).map((command) => <button key={command} role="option" onClick={() => runPaletteCommand(command)}>{command === "help" ? "Keyboard and workflow help" : `Open ${titleFor(command)}`}</button>)}</div><p className="help">Press Escape to close. Publishing, deletion, and revocation are never palette commands.</p></section></div>}
    </main>
  </div>;
}



function titleFor(page: Page) { return ({ changes: "Changes", activity: "Activity", connections: "Connections", settings: "Settings", assets: "Assets" })[page]; }
function saveLabel(state: SaveState) { return ({ saved: "Saved draft", saving: "Saving draft", unsaved: "Unsaved changes", failed: "Save failed" })[state]; }
function workspaceLabel(state: WorkspaceState) { return ({ loading: "Checking access", ready: "Connected", unavailable: "Unavailable" })[state]; }
createRoot(document.getElementById("root")!).render(<App />);
