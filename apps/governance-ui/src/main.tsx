import { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import type { ApiFailure, Asset, AssetAccess, AssetGrant, AuditEvent, AuthProvider, Catalog, Mask, PluginDescriptor, PluginPair, PluginState, PolicyRule, PolicyVersion, Preview, RuntimeSettings, Session, SessionOptions, UiAuthConfig, WorkspaceObservations, WorkspacePublication, WorkspaceSummary } from "./api";
import { controlPlane } from "./api";
import { isCurrentEpoch } from "./lifecycle";
import { pageFromHash, type UiPage } from "./navigation";
import { LoginPanel } from "./components/LoginPanel";
import { SettingsView } from "./components/SettingsView";
import { ConnectionsView } from "./components/ConnectionsView";
import { AssetWorkspace } from "./components/AssetWorkspace";
import "./styles.css";

type Page = UiPage;
type SaveState = "saved" | "saving" | "unsaved" | "failed";
type Theme = "system" | "light" | "dark";
type WorkspaceState = "loading" | "ready" | "unavailable";
type ManagementData = { history?: PolicyVersion[]; historyNextCursor?: string | null; events?: AuditEvent[]; eventsNextCursor?: string | null; catalogs?: Catalog[]; tables?: Array<Record<string, unknown>>; runtime?: RuntimeSettings | null; providers?: AuthProvider[]; providerRevision?: number; publications?: WorkspacePublication[]; summary?: WorkspaceSummary; observations?: WorkspaceObservations; grants?: AssetGrant[]; access?: AssetAccess; plugins?: PluginDescriptor[]; pluginStates?: PluginState[]; pluginPairs?: PluginPair[] };
type AuditFilters = { actor?: string; action?: string; resourceType?: string; outcome?: string; correlationId?: string; createdAfter?: string; createdBefore?: string };

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
    managementAbortController.current?.abort();
    workspaceAbortController.current?.abort();
    historyAbortController.current?.abort();
    auditAbortController.current?.abort();
  }, []);

  useEffect(() => {
    const handleHashChange = () => {
      const next = pageFromHash(window.location.hash);
      if (next !== page && !confirmDiscardUnsaved()) {
        window.history.replaceState(null, "", `#${page}`);
        return;
      }
      setPage(next);
    };
    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, [page, saveState]);

  useEffect(() => {
    const handleAuthExpired = () => {
      loadEpoch.current += 1;
      inventoryEpoch.current += 1;
      managementEpoch.current += 1;
      assetAbortController.current?.abort();
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
      const message = failure.status === 403 ? "Your account can view the workspace, but it does not have permission to open this management view." : "This management view could not be loaded. The server may be unavailable or the session may have expired.";
      setManagementError(`${message}${failure.requestId ? ` Request ID: ${failure.requestId}` : ""}`);
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
      setNotice("More history could not be loaded. The entries already visible remain available.");
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
      setNotice("More activity could not be loaded. The entries already visible remain available.");
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
      const params = new URLSearchParams(window.location.search);
      const requestedAssetId = params.get("asset");
      const requestedDraftId = params.get("draft");
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
    setAssetInventoryLoading(true);
    try {
      const pageResult = await controlPlane.listAssetPage({
        limit: 50,
        cursor: append ? assetCursor ?? undefined : undefined,
        search: search.trim() || undefined,
      });
      if (epoch !== inventoryEpoch.current) return;
      setAssets((current) => append ? [...current, ...pageResult.items] : pageResult.items);
      setAssetCursor(pageResult.next_cursor);
      setAssetHasMore(Boolean(pageResult.next_cursor));
      if (!append && !pageResult.items.length) setNotice("No governed assets match this search.");
    } catch {
      if (epoch !== inventoryEpoch.current) return;
      setNotice("Asset inventory could not be loaded. Your current editor state remains unchanged.");
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
      setNotice("Could not load this asset and its access metadata. Your previous editor state remains unchanged.");
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
    } catch {
      if (loadScope !== loadEpoch.current || editEpoch !== draftEditEpoch.current) return;
      setSaveState("failed"); setNotice("Save failed. The unsaved draft remains in this browser.");
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
    } catch {
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setPreview(null); setReviewToken(null); setNotice("Policy test could not run. This draft is not validated.");
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
    } catch {
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setReviewToken(null); setNotice("Review was rejected. Run a successful test against the saved draft and resolve any policy or schema errors.");
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
      } catch {
        if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
        setNotice("Publish failed. Review the saved draft and active generation.");
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
    } catch {
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setNotice("Restore failed. The draft may have changed; reload the asset before trying again.");
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
        return <button key={item} className={page === item ? "nav-item active" : "nav-item"} onClick={() => navigateTo(item)} disabled={disabled} title={reason}>{item}</button>;
      })}</nav>
      <div className="sidebar-foot"><span className={"status-dot " + workspace} /> Workspace: {workspace === "ready" ? "connected" : "unavailable"}<br /><small>{workspaceLabel(workspace)}{asset?.catalog ? ` · catalog ${asset.catalog}` : ""}</small></div>
    </aside>
    <main>
      <header className="topbar"><div><span className="eyebrow">{page === "assets" ? "ASSET WORKSPACE" : page.toUpperCase()}</span><h1>{page === "assets" ? asset?.name ?? "Assets" : titleFor(page)}</h1></div><div className="actor"><span className="avatar">{session?.principal.slice(0, 1).toUpperCase() ?? "?"}</span><div><strong>{session?.principal ?? "Not signed in"}</strong><small>{session?.platform_admin ? "Platform admin" : "Authenticated user"}{session?.issuer ? ` · ${session.issuer}` : ""}</small></div><label className="theme-control"><span className="sr-only">Color theme</span><select aria-label="Color theme" value={theme} onChange={(event) => setTheme(event.target.value as Theme)}><option value="system">System theme</option><option value="light">Light theme</option><option value="dark">Dark theme</option></select></label>{session && <button className="text-button" onClick={() => void logout()}>Sign out</button>}{logoutPending && <button className="text-button" onClick={() => void logout()}>Retry sign out</button>}</div></header>
      {signedOut ? accessView : page !== "assets" ? <ManagementView page={page} data={managementData} loading={managementLoading} error={managementError} onReload={() => void loadManagement(page)} onLoadMore={page === "changes" ? () => void loadMoreHistory() : page === "activity" ? () => void loadMoreAudit() : undefined} historyLoading={historyLoading} auditLoading={auditLoading} filters={auditFilters} onFiltersChange={updateAuditFilters} session={session} /> : workspace === "loading" ? accessView : !asset ? <LoginPanel title="No governed assets" message={notice} /> : <AssetWorkspace assets={assets} asset={asset} access={managementData.access} history={managementData.history ?? []} grants={managementData.grants ?? []} onAsset={(id) => { if (confirmDiscardUnsaved()) { setReviewOnly(false); void loadAsset(id); } }} assetSearch={assetSearch} assetHasMore={assetHasMore} assetInventoryLoading={assetInventoryLoading} onSearch={searchAssets} onLoadMore={() => void refreshAssetInventory(assetSearch, true)} rules={rules} activeRule={activeRule} activeRevision={draftRevision} selectedRule={selectedRule} onRule={setSelectedRule} onMoveRule={moveRule} selectedField={selectedField} onField={setSelectedField} selectedMask={selectedMask} effectiveFields={effectiveFields} saveState={saveState} notice={notice} onToggleField={toggleField} onMask={setMask} onUpdateRule={updateRule} onAddRule={addRule} onRemoveRule={removeRule} onSave={() => void saveDraft()} onPreview={() => void runPreview()} onReview={() => void requestReview()} previewPrincipal={previewPrincipal} previewGroups={previewGroups} previewClaims={previewClaims} onPreviewPrincipal={setPreviewPrincipal} onPreviewGroups={setPreviewGroups} onPreviewClaims={setPreviewClaims} onPublish={() => void publishAsset()} publishing={publishPending} onRestore={(version) => void restorePolicyVersion(version)} reviewToken={reviewToken ?? undefined} preview={preview} session={session} onReloadAccess={() => void loadAsset(asset.id, assets, undefined, reviewOnly ? draftId ?? undefined : undefined)} reviewOnly={reviewOnly} draftId={draftId} />}
      {paletteOpen && <div className="palette-backdrop" role="presentation" onMouseDown={closePalette}><section className="command-palette" role="dialog" aria-modal="true" aria-label="Command palette" onMouseDown={(event) => event.stopPropagation()}><input autoFocus value={paletteQuery} onChange={(event) => setPaletteQuery(event.target.value)} placeholder="Jump to a destination or search help" aria-label="Command search" /><div role="listbox">{(["assets", "connections", "activity", "settings", "changes", "help"] as const).filter((command) => command.includes(paletteQuery.toLowerCase())).map((command) => <button key={command} role="option" onClick={() => runPaletteCommand(command)}>{command === "help" ? "Keyboard and workflow help" : `Open ${titleFor(command)}`}</button>)}</div><p className="help">Press Escape to close. Publishing, deletion, and revocation are never palette commands.</p></section></div>}
    </main>
  </div>;
}

function ManagementView({ page, data, loading, error, onReload, onLoadMore, historyLoading, auditLoading, filters, onFiltersChange, session }: { page: Exclude<Page, "assets">; data: ManagementData; loading: boolean; error: string; onReload: () => void; onLoadMore?: () => void; historyLoading?: boolean; auditLoading?: boolean; filters: AuditFilters; onFiltersChange: (filters: AuditFilters) => void; session: Session | null }) {
  if (loading) return <section className="coming-soon"><span className="eyebrow">{page.toUpperCase()}</span><h2>Loading {page}</h2><p>Checking the current workspace state and your capabilities.</p></section>;
  if (error) return <section className="coming-soon" role="alert"><span className="eyebrow">{page.toUpperCase()}</span><h2>Management view unavailable</h2><p>{error}</p><button className="secondary" onClick={onReload}>Retry</button></section>;
  if (page === "changes") return <ChangesView history={data.history ?? []} nextCursor={data.historyNextCursor} onReload={onReload} onLoadMore={onLoadMore} loading={historyLoading ?? false} />;
  if (page === "activity") return <ActivityView history={data.history ?? []} events={data.events ?? []} nextCursor={data.eventsNextCursor} onLoadMore={onLoadMore} loading={auditLoading ?? false} filters={filters} onFiltersChange={onFiltersChange} summary={data.summary} observations={data.observations} />;
  if (page === "connections") return <ConnectionsView catalogs={data.catalogs ?? []} publications={data.publications ?? []} plugins={data.plugins ?? []} pluginStates={data.pluginStates ?? []} pluginPairs={data.pluginPairs ?? []} canActivate={Boolean(session?.platform_admin)} onReload={onReload} />;
  return <SettingsView runtime={data.runtime} providers={data.providers ?? []} providerRevision={data.providerRevision} publications={data.publications ?? []} onReload={onReload} />;
}

function ChangesView({ history, nextCursor, onReload, onLoadMore, loading }: { history: PolicyVersion[]; nextCursor?: string | null; onReload: () => void; onLoadMore?: () => void; loading: boolean }) {
  return <section className="management-view"><div className="management-head"><div><span className="eyebrow">CHANGES</span><h2>Published policy history</h2><p className="muted">Immutable versions returned by the control plane. A publication is active only when the server says so.</p></div><button className="secondary" onClick={onReload}>Refresh</button></div>{history.length ? <div className="table-wrap"><table><thead><tr><th>Asset</th><th>Version</th><th>State</th><th>Created</th></tr></thead><tbody>{history.map((item, index) => <tr key={`${item.asset_id}-${item.policy_version}-${item.created_at}-${index}`}><td><strong>{item.asset_name}</strong><small>{item.catalog} / {item.target}</small></td><td><code>{item.policy_version}</code></td><td><span className={item.active ? "result-state allowed" : "pill"}>{item.active ? "Active" : "Published"}</span></td><td>{new Date(item.created_at).toLocaleString()}</td></tr>)}</tbody></table>{nextCursor && onLoadMore && <button className="secondary load-more" disabled={loading} onClick={onLoadMore}>{loading ? "Loading…" : "Load more history"}</button>}</div> : <div className="empty-result"><strong>No published changes</strong><p>Save and publish an asset draft to create an immutable version.</p></div>}</section>;
}

function ActivityView({ history, events, nextCursor, onLoadMore, loading, filters, onFiltersChange, summary, observations }: { history: PolicyVersion[]; events: AuditEvent[]; nextCursor?: string | null; onLoadMore?: () => void; loading: boolean; filters: AuditFilters; onFiltersChange: (filters: AuditFilters) => void; summary?: WorkspaceSummary; observations?: WorkspaceObservations }) {
  const [draftFilters, setDraftFilters] = useState<AuditFilters>(filters);
  useEffect(() => setDraftFilters(filters), [filters]);
  function setFilter(key: keyof AuditFilters, value: string) {
    setDraftFilters((current) => ({ ...current, [key]: value || undefined }));
  }
  function applyFilters() { onFiltersChange({ ...draftFilters }); }
  function clearFilters() { setDraftFilters({}); onFiltersChange({}); }
  return <section className="management-view"><span className="eyebrow">ACTIVITY</span><h2>Workspace status</h2><p className="muted">Live counts and redacted audit observations from the control plane. Data is limited to the assets your session can see.</p><div className="form-card audit-filters"><h3>Filter activity</h3><div className="form-grid three"><label>Actor<input value={draftFilters.actor ?? ""} onChange={(event) => setFilter("actor", event.target.value)} placeholder="platform:admin" /></label><label>Action<input value={draftFilters.action ?? ""} onChange={(event) => setFilter("action", event.target.value)} placeholder="policy.draft.save" /></label><label>Request ID<input value={draftFilters.correlationId ?? ""} onChange={(event) => setFilter("correlationId", event.target.value)} placeholder="Correlation ID" /></label><label>Resource type<select value={draftFilters.resourceType ?? ""} onChange={(event) => setFilter("resourceType", event.target.value)}><option value="">All resources</option><option value="asset">Asset</option><option value="workspace">Workspace</option></select></label><label>Outcome<select value={draftFilters.outcome ?? ""} onChange={(event) => setFilter("outcome", event.target.value)}><option value="">All outcomes</option><option value="success">Success</option><option value="failure">Failure</option></select></label><label>Created after<input type="datetime-local" value={draftFilters.createdAfter?.slice(0, 16) ?? ""} onChange={(event) => setFilter("createdAfter", event.target.value ? new Date(event.target.value).toISOString() : "")} /></label><label>Created before<input type="datetime-local" value={draftFilters.createdBefore?.slice(0, 16) ?? ""} onChange={(event) => setFilter("createdBefore", event.target.value ? new Date(event.target.value).toISOString() : "")} /></label></div><div className="editor-actions"><button className="secondary" onClick={clearFilters}>Clear</button><button className="primary" onClick={applyFilters}>Apply filters</button></div></div>{summary ? <div className="metric-grid">{[["Assets", summary.asset_count], ["Catalogs", summary.catalog_count], ["Draft changes", summary.draft_change_count], ["Missing policy", summary.missing_policy_count], ["Enabled auth", summary.enabled_auth_provider_count]].map(([label, value]) => <div className="metric-card" key={String(label)}><strong>{String(value)}</strong><span>{label}</span></div>)}</div> : <div className="empty-result"><strong>Status unavailable</strong><p>Reconnect with a session that can read workspace observations.</p></div>}{observations && <div className="form-card observation-card"><h3>Runtime observation</h3><p><span className={"status-dot " + (observations.available ? "ready" : "unavailable")} /> {observations.available ? "Control plane connected" : "Workspace unavailable"}</p>{observations.generation ? <p><strong>Active generation:</strong> <code>{observations.generation.publication_id.slice(0, 12)}</code> · {observations.generation.status}</p> : <p>No active publication is recorded.</p>}<p className="help">Data-plane health: <strong>{observations.data_plane.status}</strong> ({observations.data_plane.reason}). Observed {new Date(observations.observed_at).toLocaleString()} from {observations.source}.</p></div>}<h3 className="activity-title">Recent governed actions</h3>{events.length ? <><ul className="activity-list">{events.slice(0, 8).map((event) => <li key={event.id}><span className={"status-dot " + (event.outcome === "success" ? "ready" : "unavailable")} /><div><strong>{event.action}</strong><small>{event.actor} · {event.resource_type} {event.resource_id.slice(0, 8)}</small></div><time>{new Date(event.created_at).toLocaleString()}</time></li>)}</ul>{nextCursor && onLoadMore && <button className="secondary load-more" disabled={loading} onClick={onLoadMore}>{loading ? "Loading…" : "Load more activity"}</button>}</> : history.length ? <ul className="activity-list">{history.slice(-8).reverse().map((item) => <li key={`${item.asset_id}-${item.policy_version}`}><span className="status-dot ready" /><div><strong>{item.asset_name}</strong><small>Policy version {item.policy_version} · {item.active ? "active" : "published"}</small></div><time>{new Date(item.created_at).toLocaleString()}</time></li>)}</ul> : <div className="empty-result"><strong>No activity yet</strong><p>Draft, restore, and publication actions will appear here after the first governed change.</p></div>}</section>;
}


function titleFor(page: Page) { return ({ changes: "Changes", activity: "Activity", connections: "Connections", settings: "Settings", assets: "Assets" })[page]; }
function saveLabel(state: SaveState) { return ({ saved: "Saved draft", saving: "Saving draft", unsaved: "Unsaved changes", failed: "Save failed" })[state]; }
function workspaceLabel(state: WorkspaceState) { return ({ loading: "Checking access", ready: "Connected", unavailable: "Unavailable" })[state]; }
createRoot(document.getElementById("root")!).render(<App />);
