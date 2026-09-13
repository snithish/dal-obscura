import { useEffect, useMemo, useRef, useState } from "react";
import type { ReactNode } from "react";
import { createRoot } from "react-dom/client";
import type { Asset, AssetGrant, AuditEvent, AuthProvider, Catalog, CatalogDiagnostic, Mask, PluginDescriptor, PluginPair, PluginState, PolicyRule, PolicyVersion, Preview, RuntimeSettings, SchemaNode, Session, SessionOptions, UiAuthConfig, WorkspaceObservations, WorkspacePublication, WorkspaceSummary } from "./api";
import { controlPlane } from "./api";
import { demoAsset, demoRules } from "./fixtures";
import { isCurrentEpoch } from "./lifecycle";
import { pageFromHash, type UiPage } from "./navigation";
import { flattenSchemaTree } from "./schema_tree";
import "./styles.css";

type Page = UiPage;
type SaveState = "saved" | "saving" | "unsaved" | "failed";
type WorkspaceState = "loading" | "ready" | "demo" | "unavailable";
type ManagementData = { history?: PolicyVersion[]; historyNextCursor?: string | null; events?: AuditEvent[]; eventsNextCursor?: string | null; catalogs?: Catalog[]; tables?: Array<Record<string, unknown>>; runtime?: RuntimeSettings | null; providers?: AuthProvider[]; publications?: WorkspacePublication[]; summary?: WorkspaceSummary; observations?: WorkspaceObservations; grants?: AssetGrant[]; plugins?: PluginDescriptor[]; pluginStates?: PluginState[]; pluginPairs?: PluginPair[] };
type AuditFilters = { actor?: string; action?: string; resourceType?: string; outcome?: string; correlationId?: string; createdAfter?: string; createdBefore?: string };

const maskOptions: Array<{ type: Mask["type"]; label: string; needsValue?: boolean }> = [
  { type: "null", label: "Null" }, { type: "redact", label: "Redact", needsValue: true },
  { type: "hash", label: "Hash" }, { type: "email", label: "Email" },
  { type: "keep_last", label: "Keep last", needsValue: true }, { type: "default", label: "Default", needsValue: true },
];
type PluginConfigField = { name: string; type: string; required: boolean; secret: boolean; options?: string[] };
const defaultCatalogFields: PluginConfigField[] = [
  { name: "uri", type: "string", required: true, secret: false },
  { name: "warehouse", type: "string", required: false, secret: false },
  { name: "user", type: "string", required: false, secret: false },
  { name: "password", type: "secret_reference", required: false, secret: true },
];

function configFields(plugin?: PluginDescriptor): PluginConfigField[] {
  const raw = plugin?.config_schema?.fields;
  if (!Array.isArray(raw)) return [];
  return raw.flatMap((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return [];
    const value = item as Record<string, unknown>;
    const name = typeof value.name === "string" ? value.name.trim() : "";
    const type = typeof value.type === "string" ? value.type : "string";
    if (!name || name.length > 128 || !/^[A-Za-z][A-Za-z0-9_.-]*$/.test(name)) return [];
    const options = Array.isArray(value.options) ? value.options.filter((option): option is string => typeof option === "string" && option.length <= 128) : undefined;
    return [{ name, type, required: value.required === true, secret: value.secret === true || type === "secret_reference", options }];
  });
}

function configFieldLabel(name: string): string {
  if (name === "uri") return "SQL catalog URI";
  if (name === "user") return "Database username";
  if (name === "password") return "Password secret reference";
  return name.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

const newRule = (field: string, ordinal: number): PolicyRule => ({ ordinal, effect: "allow", principals: [], columns: field ? [field] : [], masks: {}, row_filter: null });

function App() {
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
  const [historyLoading, setHistoryLoading] = useState(false);
  const [auditLoading, setAuditLoading] = useState(false);
  const [auditFilters, setAuditFilters] = useState<AuditFilters>({});
  const [publishPending, setPublishPending] = useState(false);
  const loadEpoch = useRef(0);
  const draftEditEpoch = useRef(0);
  const inventoryEpoch = useRef(0);
  const searchTimer = useRef<number | undefined>(undefined);
  const managementEpoch = useRef(0);
  const [logoutPending, setLogoutPending] = useState(false);
  const isDemo = workspace === "demo";

  useEffect(() => {
    if (import.meta.env.DEV && new URLSearchParams(window.location.search).has("demo")) {
      setAssets([demoAsset]); setAsset(demoAsset); setRules(demoRules); setSelectedField(demoAsset.schema_fields[0].name);
      setWorkspace("demo"); setNotice("Demo workspace. It never writes to the control plane.");
      return;
    }
    void loadInitialWorkspace();
  }, []);

  useEffect(() => () => {
    if (searchTimer.current !== undefined) window.clearTimeout(searchTimer.current);
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
    if (page === "assets" || !session || isDemo) return;
    void loadManagement(page);
  }, [page, session, isDemo, auditFilters]);

  async function loadManagement(destination: Page) {
    const epoch = ++managementEpoch.current;
    setManagementLoading(true);
    try {
      let next: ManagementData = {};
      if (destination === "changes") { const pageResult = await controlPlane.listHistoryPage({ limit: 50 }); next = { history: pageResult.items, historyNextCursor: pageResult.next_cursor }; }
      if (destination === "activity") { const audit = await controlPlane.listAuditEventsPage({ limit: 50, ...auditFilters }); next = { history: await controlPlane.listHistory(), events: audit.items, eventsNextCursor: audit.next_cursor, summary: await controlPlane.getSummary(), observations: await controlPlane.getObservations() }; }
      if (destination === "connections") { const pluginData = await controlPlane.listPlugins(); next = { catalogs: await controlPlane.listCatalogs(), publications: session?.platform_admin ? await controlPlane.listWorkspacePublications() : [], plugins: pluginData.plugins, pluginStates: pluginData.states, pluginPairs: pluginData.pairs }; }
      if (destination === "settings") next = { runtime: await controlPlane.getRuntimeSettings(), providers: await controlPlane.getAuthProviders(), publications: session?.platform_admin ? await controlPlane.listWorkspacePublications() : [] };
      if (!isCurrentEpoch(epoch, managementEpoch.current)) return;
      setManagementData(next);
    } catch {
      if (!isCurrentEpoch(epoch, managementEpoch.current)) return;
      setManagementData({});
      setNotice("This management view is unavailable for your current session or workspace.");
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
      const pageResult = await controlPlane.listHistoryPage({ limit: 50, cursor });
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setManagementData((current) => ({ ...current, history: [...(current.history ?? []), ...pageResult.items], historyNextCursor: pageResult.next_cursor }));
    } catch {
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setNotice("More history could not be loaded. The entries already visible remain available.");
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
      const pageResult = await controlPlane.listAuditEventsPage({ limit: 50, cursor, ...auditFilters });
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setManagementData((current) => ({ ...current, events: [...(current.events ?? []), ...pageResult.items], eventsNextCursor: pageResult.next_cursor }));
    } catch {
      if (!isCurrentEpoch(scope, managementEpoch.current)) return;
      setNotice("More activity could not be loaded. The entries already visible remain available.");
    } finally {
      if (scope === managementEpoch.current) setAuditLoading(false);
    }
  }

  function updateAuditFilters(next: AuditFilters) {
    managementEpoch.current += 1;
    setManagementData({});
    setAuditFilters(next);
  }

  async function loadInitialWorkspace() {
    const epoch = ++loadEpoch.current;
    setWorkspace("loading");
    try {
      const loadedSession = await controlPlane.getSession();
      if (epoch !== loadEpoch.current) return;
      setSession(loadedSession);
      const loadedPage = await controlPlane.listAssetPage({ limit: 50 });
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
      await loadAsset(loaded[0].id, loaded, epoch);
      if (epoch !== loadEpoch.current) return;
      setWorkspace("ready");
      restorePostLoginHash();
    } catch {
      if (epoch !== loadEpoch.current) return;
      setWorkspace("unavailable");
      setSession(null);
      const options = await controlPlane.getSessionOptions().catch(() => null);
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

  async function demoLogin(loginHint: string) {
    setLoggingIn(true);
    try {
      await controlPlane.demoLogin(loginHint);
      loadEpoch.current += 1;
      await loadInitialWorkspace();
    } catch {
      setWorkspace("unavailable");
      setNotice("Sign-in failed. No policy data was loaded.");
    } finally {
      setLoggingIn(false);
    }
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
    setDraftRevision(0); setReviewToken(null); setSaveState("saved");
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
  ) {
    const epoch = inheritedEpoch ?? ++loadEpoch.current;
    try {
      const [fullAsset, loadedRules, schema, history, grants] = await Promise.all([controlPlane.getAsset(assetId), controlPlane.listRules(assetId), controlPlane.getSchema(assetId), controlPlane.listAssetHistory(assetId).catch(() => []), controlPlane.listGrants(assetId).catch(() => [])]);
      if (epoch !== loadEpoch.current) return;
      fullAsset.schema = schema;
      const draft = isDemo ? null : await controlPlane.getDraft(assetId);
      if (epoch !== loadEpoch.current) return;
      const effectiveRules = draft?.rules ?? loadedRules;
      setManagementData((current) => ({ ...current, history, grants }));
      setAssets(knownAssets); setAsset(fullAsset); setRules(effectiveRules); setDraftRevision(draft?.revision ?? 0); setSelectedRule(0); setReviewToken(null);
      draftEditEpoch.current += 1;
      setSelectedField(schema.fields[0]?.human_path ?? fullAsset.schema_fields[0]?.name ?? ""); setPreview(null); setSaveState("saved");
      setNotice(effectiveRules.length ? "Loaded your policy draft." : "No policy draft exists yet. Add a rule to begin authoring.");
    } catch {
      if (inheritedEpoch !== undefined && epoch !== loadEpoch.current) return;
      setNotice("Could not load this asset. Your previous editor state remains unchanged.");
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
      if (!isDemo) {
        const saved = await controlPlane.saveDraft(assetId, revision, rules);
        if (loadScope !== loadEpoch.current || editEpoch !== draftEditEpoch.current) {
          if (loadScope === loadEpoch.current) setSaveState("unsaved");
          return;
        }
        setDraftRevision(saved.revision);
      }
      if (loadScope !== loadEpoch.current || editEpoch !== draftEditEpoch.current) {
        if (loadScope === loadEpoch.current) setSaveState("unsaved");
        return;
      }
      setSaveState("saved"); setReviewToken(null); setNotice(isDemo ? "Demo draft resets when this page closes." : "Policy draft saved to the control plane.");
    } catch {
      if (loadScope !== loadEpoch.current || editEpoch !== draftEditEpoch.current) return;
      setSaveState("failed"); setNotice("Save failed. The unsaved draft remains in this browser.");
    }
  }
  async function runPreview() {
    if (!asset) return;
    if (!isDemo && saveState !== "saved") {
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
      const result: Preview = isDemo ? { decision: "allow", allowed_columns: activeRule?.columns ?? [], masks: activeRule?.masks ?? {}, row_filter: activeRule?.row_filter ?? null, policy_version: 1 } : await controlPlane.evaluate(asset.id, { principal: previewPrincipal.trim(), groups: previewGroups.split(",").map((value) => value.trim()).filter(Boolean), claims });
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setPreview(result); setReviewToken(null); setNotice(isDemo ? "Demo evaluation is local and cannot be published." : `Server-side evaluation completed: ${result.decision === "allow" ? "allowed" : "denied"}.`);
    } catch {
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setPreview(null); setReviewToken(null); setNotice("Policy test could not run. This draft is not validated.");
    }
  }

  async function requestReview() {
    if (!asset || isDemo || saveState !== "saved" || publishPending) return;
    const loadScope = loadEpoch.current;
    const editScope = draftEditEpoch.current;
    try {
      const claims = JSON.parse(previewClaims || "{}") as Record<string, object>;
      if (!claims || Array.isArray(claims) || typeof claims !== "object") throw new Error("Claims must be a JSON object");
      const result = await controlPlane.review(asset.id, { principal: previewPrincipal.trim(), groups: previewGroups.split(",").map((value) => value.trim()).filter(Boolean), claims });
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setPreview(result); setReviewToken(result.review_token ?? null); setNotice("Server review is current for this saved draft revision. You can publish it now.");
    } catch {
      if (loadScope !== loadEpoch.current || editScope !== draftEditEpoch.current) return;
      setReviewToken(null); setNotice("Review was rejected. Run a successful test against the saved draft and resolve any policy or schema errors.");
    }
  }

  async function publishAsset() {
    if (!asset || isDemo || !reviewToken || publishPending) return;
    const loadScope = loadEpoch.current;
    const editScope = draftEditEpoch.current;
    const idempotencyKey = crypto.randomUUID();
    setPublishPending(true);
    try {
      await controlPlane.publishAsset(asset.id, draftRevision, reviewToken, idempotencyKey);
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
      setPublishPending(false);
    }
  }

  async function restorePolicyVersion(policyVersion: number) {
    if (!asset || isDemo) return;
    if (saveState === "unsaved" && !window.confirm("You have unsaved policy changes. Restore this published version over them?")) return;
    try {
      const restored = await controlPlane.restorePolicyVersion(asset.id, policyVersion, draftRevision);
      setRules(restored.rules);
      setDraftRevision(restored.revision);
      setSaveState("saved");
      setPreview(null); setReviewToken(null);
      setNotice(`Version ${policyVersion} restored as draft revision ${restored.revision}. Review and publish it when ready.`);
    } catch {
      setNotice("Restore failed. The draft may have changed; reload the asset before trying again.");
    }
  }

  const signedOut = !isDemo && (workspace === "unavailable" || (workspace === "loading" && !session));
  const accessView = <WorkspaceMessage showAuth={workspace === "unavailable"} title={workspace === "loading" ? "Loading governed workspace" : "Sign in to your workspace"} message={workspace === "loading" ? "Checking your workspace access and available assets." : notice} retry={workspace === "unavailable" ? loadInitialWorkspace : undefined} authConfig={authConfig} sessionOptions={sessionOptions} bootstrapToken={bootstrapToken} onBootstrapToken={setBootstrapToken} onBootstrapLogin={() => void bootstrapLogin()} onLogin={demoLogin} loggingIn={loggingIn} authError={authError} />;
  return <div className="app-shell">
    <aside className="sidebar" aria-label="Primary navigation">
      <a className="brand" href="#assets" onClick={() => navigateTo("assets")}>DAL OBSCURA<span>GOVERNANCE</span></a>
      <nav>{(["assets", "changes", "activity", "connections", "settings"] as Page[]).map((item) => <button key={item} className={page === item ? "nav-item active" : "nav-item"} onClick={() => navigateTo(item)}>{item}</button>)}</nav>
      <div className="sidebar-foot"><span className={"status-dot " + workspace} /> Workspace: {workspace === "ready" || isDemo ? "connected" : "unavailable"}<br /><small>{workspaceLabel(workspace)}{asset?.catalog ? ` · catalog ${asset.catalog}` : ""}</small></div>
    </aside>
    <main>
      <header className="topbar"><div><span className="eyebrow">{page === "assets" ? "ASSET WORKSPACE" : page.toUpperCase()}</span><h1>{page === "assets" ? asset?.name ?? "Assets" : titleFor(page)}</h1></div><div className="actor"><span className="avatar">{session?.principal.slice(0, 1).toUpperCase() ?? "?"}</span><div><strong>{session?.principal ?? "Not signed in"}</strong><small>{session?.platform_admin ? "Platform admin" : "Authenticated user"}{session?.issuer ? ` · ${session.issuer}` : ""}</small></div>{session && <button className="text-button" onClick={() => void logout()}>Sign out</button>}{logoutPending && <button className="text-button" onClick={() => void logout()}>Retry sign out</button>}</div></header>
      {signedOut ? accessView : page !== "assets" ? <ManagementView page={page} data={managementData} loading={managementLoading} onReload={() => void loadManagement(page)} onLoadMore={page === "changes" ? () => void loadMoreHistory() : page === "activity" ? () => void loadMoreAudit() : undefined} historyLoading={historyLoading} auditLoading={auditLoading} filters={auditFilters} onFiltersChange={updateAuditFilters} session={session} /> : workspace === "loading" ? accessView : !asset ? <WorkspaceMessage title="No governed assets" message={notice} /> : <AssetWorkspace assets={assets} asset={asset} history={managementData.history ?? []} grants={managementData.grants ?? []} onAsset={(id) => { if (confirmDiscardUnsaved()) void loadAsset(id); }} assetSearch={assetSearch} assetHasMore={assetHasMore} assetInventoryLoading={assetInventoryLoading} onSearch={searchAssets} onLoadMore={() => void refreshAssetInventory(assetSearch, true)} rules={rules} activeRule={activeRule} activeRevision={draftRevision} selectedRule={selectedRule} onRule={setSelectedRule} onMoveRule={moveRule} selectedField={selectedField} onField={setSelectedField} selectedMask={selectedMask} effectiveFields={effectiveFields} saveState={saveState} notice={notice} onToggleField={toggleField} onMask={setMask} onUpdateRule={updateRule} onAddRule={addRule} onRemoveRule={removeRule} onSave={() => void saveDraft()} onPreview={() => void runPreview()} onReview={() => void requestReview()} previewPrincipal={previewPrincipal} previewGroups={previewGroups} previewClaims={previewClaims} onPreviewPrincipal={setPreviewPrincipal} onPreviewGroups={setPreviewGroups} onPreviewClaims={setPreviewClaims} onPublish={() => void publishAsset()} publishing={publishPending} onRestore={(version) => void restorePolicyVersion(version)} reviewToken={reviewToken ?? undefined} preview={preview} session={session} onReloadAccess={() => void loadAsset(asset.id)} />}
    </main>
  </div>;
}

function AssetWorkspace(props: { assets: Asset[]; asset: Asset; history: PolicyVersion[]; grants: AssetGrant[]; onAsset: (id: string) => void; assetSearch: string; assetHasMore: boolean; assetInventoryLoading: boolean; onSearch: (value: string) => void; onLoadMore: () => void; rules: PolicyRule[]; activeRule?: PolicyRule; activeRevision: number; selectedRule: number; onRule: (index: number) => void; onMoveRule: (index: number, direction: -1 | 1) => void; selectedField: string; onField: (name: string) => void; selectedMask?: Mask; effectiveFields: Set<string>; saveState: SaveState; notice: string; onToggleField: (name: string) => void; onMask: (mask?: Mask) => void; onUpdateRule: (change: (rule: PolicyRule) => PolicyRule) => void; onAddRule: () => void; onRemoveRule: () => void; onSave: () => void; onPreview: () => void; onReview: () => void; previewPrincipal: string; previewGroups: string; previewClaims: string; onPreviewPrincipal: (value: string) => void; onPreviewGroups: (value: string) => void; onPreviewClaims: (value: string) => void; onPublish: () => void; publishing: boolean; onRestore: (version: number) => void; reviewToken?: string; preview: Preview | null; session: Session | null; onReloadAccess: () => void }) {
  const [tab, setTab] = useState<"policy" | "tests" | "history" | "access" | "consumers">("policy");
  const [schemaSearch, setSchemaSearch] = useState("");
  const [conditionsText, setConditionsText] = useState("{}");
  const [conditionsError, setConditionsError] = useState(false);
  useEffect(() => {
    setConditionsText(JSON.stringify(props.activeRule?.when ?? {}, null, 2));
    setConditionsError(false);
  }, [props.asset.id, props.selectedRule, props.activeRule?.when]);
  const fields = props.asset.schema?.fields ?? props.asset.schema_fields.map((field, index) => ({ field_id: index, name: field.name, path: { version: 1, segments: [{ kind: "field" as const, name: field.name, field_id: index }] }, human_path: field.name, type: field.type, nullable: field.nullable, kind: "scalar" as const }));
  const visibleFields = useMemo(() => filterSchemaNodes(fields, schemaSearch), [fields, schemaSearch]);
  const currentPreview = props.preview;
  const listedAssets = props.assets.some((item) => item.id === props.asset.id) ? props.assets : [props.asset, ...props.assets];
  return <><div className="asset-summary"><div className="asset-picker"><label htmlFor="asset-search">Find governed asset<input id="asset-search" type="search" value={props.assetSearch} onChange={(event) => props.onSearch(event.target.value)} placeholder="Search catalog or asset" /></label><label htmlFor="asset-select">Selected asset<select id="asset-select" value={props.asset.id} onChange={(event) => props.onAsset(event.target.value)}>{listedAssets.map((item) => <option key={item.id} value={item.id}>{item.catalog} / {item.name}</option>)}</select></label><div className="asset-page-actions"><small>{props.assets.length} loaded</small>{props.assetHasMore && <button className="secondary compact" disabled={props.assetInventoryLoading} onClick={props.onLoadMore}>{props.assetInventoryLoading ? "Loading…" : "Load more"}</button>}{props.assetInventoryLoading && <span className="muted" role="status">Updating inventory…</span>}</div></div><div className="save-status" aria-live="polite"><span className={"save-dot " + props.saveState} /> {saveLabel(props.saveState)}</div></div><div className="notice" role="status">{props.notice}</div><div className="asset-tabs" role="tablist" aria-label="Asset views">{(["policy", "tests", "history", "access", "consumers"] as const).map((item) => <button key={item} role="tab" aria-selected={tab === item} className={tab === item ? "selected" : ""} onClick={() => setTab(item)}>{item === "policy" ? "Policy" : item === "tests" ? "Tests" : item === "history" ? "History" : item === "access" ? "Access" : "Consumers"}</button>)}</div>{tab === "policy" ? <div className="studio">
    <section className="schema-panel" aria-label="Schema and field selection"><div className="panel-head"><div><span className="eyebrow">SCHEMA</span><h2>Fields & access</h2></div></div><p className="help">Fields retain server-defined paths. A checked field is visible through the selected rule.</p>{props.asset.schema?.stable_field_ids === false && <p className="schema-drift-warning" role="status"><strong>Schema identity requires reapproval</strong><br />This adapter does not provide stable field IDs. Any schema change must be reviewed again before access is served.</p>}<label className="schema-search">Search fields<input type="search" value={schemaSearch} onChange={(event) => setSchemaSearch(event.target.value)} placeholder="name or nested path" /></label>{visibleFields.length ? <VirtualSchemaTree nodes={visibleFields} selectedField={props.selectedField} effectiveFields={props.effectiveFields} onField={props.onField} forceExpanded={Boolean(schemaSearch)} /> : <div className="empty-result"><strong>No matching fields</strong><p>Clear the search to browse the authoritative Iceberg schema.</p></div>}<div className="schema-note"><strong>Nested fields</strong><p>Struct, list, and map paths come from the control plane. Collection nodes expose explicit <code>$element</code>, <code>$key</code>, and <code>$value</code> segments.</p></div></section>
    <section className="editor-panel" aria-label="Policy rule editor"><div className="panel-head"><div><span className="eyebrow">POLICY RULES</span><h2>{props.activeRule ? "Rule " + (props.selectedRule + 1) : "No rule selected"}</h2></div><button className="text-button" onClick={props.onAddRule}>Add rule</button></div>{props.rules.length ? <><div className="rule-list" aria-label="Policy rule list">{props.rules.map((rule, index) => <div className="rule-row" key={index}><button className={index === props.selectedRule ? "selected" : ""} onClick={() => props.onRule(index)}>Rule {index + 1}<small>{rule.principals.join(", ") || "No principal"}</small></button><div className="rule-order"><button className="secondary compact" type="button" aria-label={`Move rule ${index + 1} up`} disabled={index === 0} onClick={() => props.onMoveRule(index, -1)}>↑</button><button className="secondary compact" type="button" aria-label={`Move rule ${index + 1} down`} disabled={index === props.rules.length - 1} onClick={() => props.onMoveRule(index, 1)}>↓</button></div></div>)}</div><EditorSection label="Who"><input value={props.activeRule?.principals.join(", ") ?? ""} onChange={(event) => props.onUpdateRule((rule) => ({ ...rule, principals: event.target.value.split(",").map((value) => value.trim()).filter(Boolean) }))} aria-label="Principals or groups" placeholder="group:us-analysts" /></EditorSection><EditorSection label="Conditions"><textarea value={conditionsText} onChange={(event) => { const text = event.target.value; setConditionsText(text); try { const parsed = JSON.parse(text); if (!parsed || Array.isArray(parsed) || typeof parsed !== "object") throw new Error("object required"); setConditionsError(false); props.onUpdateRule((rule) => ({ ...rule, when: parsed as Record<string, string | string[]> })); } catch { setConditionsError(true); } }} aria-invalid={conditionsError} aria-label="Principal conditions JSON" placeholder='{"region":"us"}' />{conditionsError && <p className="auth-error" role="alert">Conditions must be a JSON object before saving.</p>}<p className="help">Conditions are evaluated by the control plane against authenticated claims.</p></EditorSection><EditorSection label="Which fields"><label className="check-line"><input type="checkbox" checked={props.activeRule?.columns.includes(props.selectedField) ?? false} disabled={!props.selectedField} onChange={() => props.onToggleField(props.selectedField)} /> Include <strong>{props.selectedField || "a schema field"}</strong></label><p className="help">Parent/child conflicts and invalid nested paths are rejected by the control plane.</p></EditorSection><EditorSection label="Which rows"><textarea value={props.activeRule?.row_filter ?? ""} onChange={(event) => props.onUpdateRule((rule) => ({ ...rule, row_filter: event.target.value || null }))} aria-label="DuckDB row restriction" placeholder="region = 'US'" /><p className="help">Matching rules combine row restrictions with AND.</p></EditorSection><EditorSection label="How values appear"><MaskEditor mask={props.selectedMask} onChange={props.onMask} field={props.selectedField} /></EditorSection><div className="editor-actions"><button className="danger" onClick={props.onRemoveRule}>Remove rule</button><button className="secondary" onClick={props.onPreview}>Run policy test</button><button className="secondary" onClick={props.onReview} disabled={props.saveState !== "saved" || props.publishing}>{props.reviewToken ? "Review current" : "Review for publish"}</button><button className="primary" onClick={props.onSave} disabled={props.saveState === "saving" || conditionsError}>{props.saveState === "saving" ? "Saving…" : "Save draft"}</button><button className="secondary" onClick={props.onPublish} disabled={props.saveState !== "saved" || !props.reviewToken || props.publishing}>{props.publishing ? "Publishing…" : "Publish reviewed draft"}</button></div></> : <div className="empty-result"><strong>No draft rules</strong><p>This is an intentional deny-all policy. Save it, test it, and complete server review before publishing.</p><div className="editor-actions"><button className="secondary" onClick={props.onPreview}>Run policy test</button><button className="secondary" onClick={props.onReview} disabled={props.saveState !== "saved" || props.publishing}>Review for publish</button><button className="primary" onClick={props.onSave} disabled={props.saveState === "saving"}>{props.saveState === "saving" ? "Saving…" : "Save deny-all draft"}</button><button className="secondary" onClick={props.onPublish} disabled={props.saveState !== "saved" || !props.reviewToken || props.publishing}>{props.publishing ? "Publishing…" : "Publish reviewed deny-all"}</button></div><button className="primary" onClick={props.onAddRule}>Add first rule</button></div>}</section>
    <section className="result-panel" aria-label="Effective access inspector"><span className="eyebrow">EFFECTIVE ACCESS</span><h2>{props.previewPrincipal || "Synthetic persona"}</h2><p className="muted">{props.previewGroups ? `Groups: ${props.previewGroups}` : "No groups"} · not reader authentication</p>{currentPreview ? <><div className={"result-state " + (currentPreview.decision === "deny" ? "denied" : "allowed")}>Test {currentPreview.decision === "deny" ? "denied" : "allowed"}</div><h3>Visible output</h3><ul>{currentPreview.allowed_columns.map((field) => <li key={field}>{field}{currentPreview.masks[field] && <small> · {currentPreview.masks[field].type} mask</small>}</li>)}</ul><h3>Row restriction</h3><code>{currentPreview.row_filter ?? "No matching row restriction"}</code></> : <div className="empty-result"><strong>Run a policy test</strong><p>See authorized output schema, masks, and row restriction for this draft.</p></div>}<div className="result-warning"><strong>Before publishing</strong><p>Review uses the exact saved draft. Changing fields, masks, or row restrictions makes this result stale.</p></div></section>
  </div> : tab === "tests" ? <TestsView onPreview={props.onPreview} preview={props.preview} principal={props.previewPrincipal} groups={props.previewGroups} claims={props.previewClaims} onPrincipal={props.onPreviewPrincipal} onGroups={props.onPreviewGroups} onClaims={props.onPreviewClaims} /> : tab === "history" ? <HistoryView history={props.history.filter((item) => item.asset_id === props.asset.id)} onRestore={props.onRestore} disabled={props.saveState === "saving"} /> : tab === "access" ? <AccessView asset={props.asset} grants={props.grants} session={props.session} onReload={props.onReloadAccess} /> : <ConsumerView asset={props.asset} />}</>;
}

const TREE_ROW_HEIGHT = 42;
const TREE_VIEWPORT_HEIGHT = 504;
const TREE_OVERSCAN = 8;

function VirtualSchemaTree({ nodes, selectedField, effectiveFields, onField, forceExpanded = false }: { nodes: SchemaNode[]; selectedField: string; effectiveFields: Set<string>; onField: (name: string) => void; forceExpanded?: boolean }) {
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set(nodes.filter((node) => node.children?.length).map((node) => node.human_path)));
  const [scrollTop, setScrollTop] = useState(0);
  const viewportRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    setExpanded(new Set(nodes.filter((node) => node.children?.length).map((node) => node.human_path)));
    setScrollTop(0);
  }, [nodes]);
  const flattened = useMemo(() => flattenSchemaTree(nodes, expanded, forceExpanded), [expanded, forceExpanded, nodes]);
  const first = Math.max(0, Math.floor(scrollTop / TREE_ROW_HEIGHT) - TREE_OVERSCAN);
  const last = Math.min(flattened.length, Math.ceil((scrollTop + TREE_VIEWPORT_HEIGHT) / TREE_ROW_HEIGHT) + TREE_OVERSCAN);
  const windowed = flattened.slice(first, last);
  const toggle = (path: string) => setExpanded((current) => {
    const next = new Set(current);
    if (next.has(path)) next.delete(path); else next.add(path);
    return next;
  });
  const focusIndex = (index: number) => {
    const viewport = viewportRef.current;
    if (!viewport) return;
    const targetTop = index * TREE_ROW_HEIGHT;
    const visibleTop = viewport.scrollTop;
    const visibleBottom = visibleTop + TREE_VIEWPORT_HEIGHT;
    if (targetTop < visibleTop || targetTop + TREE_ROW_HEIGHT > visibleBottom) {
      viewport.scrollTo({ top: Math.max(0, targetTop - (TREE_VIEWPORT_HEIGHT - TREE_ROW_HEIGHT) / 2) });
    }
    window.requestAnimationFrame(() => document.querySelector<HTMLElement>(`[data-schema-index="${index}"]`)?.focus());
  };
  return <div ref={viewportRef} className="field-tree-viewport" role="tree" aria-label="Schema fields" aria-setsize={flattened.length} style={{ maxHeight: TREE_VIEWPORT_HEIGHT }} onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}><div className="field-tree-window" style={{ height: flattened.length * TREE_ROW_HEIGHT }}>{windowed.map(({ node, depth, index }) => {
    const hasChildren = Boolean(node.children?.length);
    const isExpanded = hasChildren && (forceExpanded || expanded.has(node.human_path));
    return <div className="field-tree-item" key={node.human_path} data-schema-index={index} role="treeitem" aria-level={depth + 1} aria-posinset={index + 1} aria-setsize={flattened.length} aria-expanded={hasChildren ? isExpanded : undefined} style={{ top: index * TREE_ROW_HEIGHT }} tabIndex={selectedField === node.human_path ? 0 : -1} onKeyDown={(event) => {
      if (event.key === "ArrowRight" && hasChildren && !isExpanded) { event.preventDefault(); toggle(node.human_path); }
      else if (event.key === "ArrowLeft" && hasChildren && isExpanded && !forceExpanded) { event.preventDefault(); toggle(node.human_path); }
      else if (event.key === "ArrowDown" && index < flattened.length - 1) { event.preventDefault(); focusIndex(index + 1); }
      else if (event.key === "ArrowUp" && index > 0) { event.preventDefault(); focusIndex(index - 1); }
      else if (event.key === "Enter" || event.key === " ") { event.preventDefault(); onField(node.human_path); }
    }}><div className="field-row-wrap">{hasChildren ? <button className="tree-toggle" type="button" aria-label={`${isExpanded ? "Collapse" : "Expand"} ${node.human_path}`} aria-expanded={isExpanded} onClick={() => toggle(node.human_path)}>{isExpanded ? "▾" : "▸"}</button> : <span className="tree-toggle spacer" aria-hidden="true" /> }<button className={selectedField === node.human_path ? "field-row selected" : "field-row"} style={{ paddingLeft: `${8 + depth * 16}px` }} onClick={() => onField(node.human_path)} aria-label={`Select ${node.human_path}`}><span className="field-name">{node.name}</span><span className="field-type">{node.type}{node.nullable ? " · nullable" : ""}</span>{effectiveFields.has(node.human_path) && <span className="grant">Granted</span>}</button></div></div>;
  })}</div></div>;
}

function filterSchemaNodes(nodes: SchemaNode[], search: string): SchemaNode[] {
  const normalized = search.trim().toLowerCase();
  if (!normalized) return nodes;
  const visit = (node: SchemaNode): SchemaNode | null => {
    const children = (node.children ?? []).map(visit).filter((child): child is SchemaNode => child !== null);
    if (node.human_path.toLowerCase().includes(normalized) || node.name.toLowerCase().includes(normalized)) {
      return { ...node, children: node.children };
    }
    return children.length ? { ...node, children } : null;
  };
  return nodes.map(visit).filter((node): node is SchemaNode => node !== null);
}

function EditorSection({ label, children }: { label: string; children: ReactNode }) { return <section className="editor-section"><h3>{label}</h3>{children}</section>; }
function MaskEditor({ mask, field, onChange }: { mask?: Mask; field: string; onChange: (mask?: Mask) => void }) { const option = maskOptions.find((candidate) => candidate.type === mask?.type); return <div className="mask-editor"><label htmlFor="mask-type">Mask for <strong>{field || "selected field"}</strong></label><select id="mask-type" value={mask?.type ?? ""} disabled={!field} onChange={(event) => { const type = event.target.value as Mask["type"] | ""; onChange(type ? { type } : undefined); }}><option value="">No mask</option>{maskOptions.map((candidate) => <option key={candidate.type} value={candidate.type}>{candidate.label}</option>)}</select>{option?.needsValue && <input aria-label="Mask value" type={mask?.type === "keep_last" ? "number" : "text"} min={mask?.type === "keep_last" ? 0 : undefined} step={mask?.type === "keep_last" ? 1 : undefined} inputMode={mask?.type === "keep_last" ? "numeric" : undefined} value={mask?.value === undefined || mask.value === null ? "" : String(mask.value)} placeholder={mask?.type === "keep_last" ? "Characters to retain" : "Text or JSON scalar"} onChange={(event) => { if (mask?.type === "keep_last") { const value = Number(event.target.value); if (Number.isInteger(value) && value >= 0) onChange({ ...mask, value }); return; } const raw = event.target.value; try { const parsed = JSON.parse(raw); onChange({ ...mask!, value: parsed === null || ["string", "number", "boolean"].includes(typeof parsed) ? parsed : raw }); } catch { onChange({ ...mask!, value: raw }); } }} />}<p className="help">Mask behavior is validated by the control plane before publication.</p></div>; }
function TestsView({ onPreview, preview, principal, groups, claims, onPrincipal, onGroups, onClaims }: { onPreview: () => void; preview: Preview | null; principal: string; groups: string; claims: string; onPrincipal: (value: string) => void; onGroups: (value: string) => void; onClaims: (value: string) => void }) { return <section className="tests-view"><span className="eyebrow">POLICY TESTS</span><h2>Test before review</h2><p>Simulate a representative persona. This is a policy evaluation, not an impersonated read or data preview.</p><div className="test-card persona-form"><label>Principal<input value={principal} onChange={(event) => onPrincipal(event.target.value)} placeholder="user:analyst@example.com" /></label><label>Groups<input value={groups} onChange={(event) => onGroups(event.target.value)} placeholder="us-analysts, finance" /></label><label>Claims (JSON)<textarea value={claims} onChange={(event) => onClaims(event.target.value)} aria-label="Synthetic persona claims" placeholder='{"region":"us"}' /></label><button className="primary" onClick={onPreview}>Run test</button></div>{preview && <div className={"result-state " + (preview.decision === "deny" ? "denied" : "allowed")}>Current test: {preview.decision === "deny" ? "denied" : `${preview.allowed_columns.length} fields visible`}</div>}</section>; }
function HistoryView({ history, onRestore, disabled }: { history: PolicyVersion[]; onRestore: (version: number) => void; disabled: boolean }) { return <section className="tests-view"><span className="eyebrow">HISTORY</span><h2>Published policy revisions</h2><p>Immutable versions returned by the control plane. Restore creates a new editable draft and never changes published history.</p>{history.length ? <div className="table-wrap"><table><thead><tr><th>Version</th><th>State</th><th>Created</th><th>Action</th></tr></thead><tbody>{history.map((item) => <tr key={item.policy_version}><td><code>{item.policy_version}</code></td><td>{item.active ? "Active" : "Published"}</td><td>{new Date(item.created_at).toLocaleString()}</td><td><button className="secondary compact" disabled={disabled} onClick={() => onRestore(item.policy_version)}>Restore to draft</button></td></tr>)}</tbody></table></div> : <div className="empty-result"><strong>No historical revision loaded</strong><p>Publish a reviewed draft to create the first immutable revision.</p></div>}</section>; }
function ConsumerView({ asset }: { asset: Asset }) {
  const catalog = JSON.stringify(asset.catalog);
  const target = JSON.stringify(asset.name);
  const python = `import os\nfrom dal_obscura.connectors import DalObscuraClient\n\nwith DalObscuraClient(os.environ["DAL_OBSCURA_FLIGHT_URI"],\n                     auth_token=lambda: os.environ["DAL_OBSCURA_TOKEN"]) as client:\n    table = client.read_table(\n        catalog=${catalog},\n        target=${target},\n        columns=["*"],\n    )`;
  const duckdb = `import os\nfrom dal_obscura.connectors import DalObscuraClient, DuckDBDalObscuraReader\n\nwith DalObscuraClient(os.environ["DAL_OBSCURA_FLIGHT_URI"],\n                     auth_token=lambda: os.environ["DAL_OBSCURA_TOKEN"]) as client:\n    reader = DuckDBDalObscuraReader(client)\n    relation = reader.relation(\n        catalog=${catalog},\n        target=${target},\n        columns=["*"],\n    )\n    relation.show()`;
  const spark = `import os\n\nspark.read.format("dal_obscura") \\\n    .option("dal.uri", os.environ["DAL_OBSCURA_FLIGHT_URI"]) \\\n    .option("dal.catalog", ${catalog}) \\\n    .option("dal.target", ${target}) \\\n    .option("dal.auth.token-env", "DAL_OBSCURA_TOKEN") \\\n    .option("dal.executor.auth.token-env", "DAL_OBSCURA_TOKEN") \\\n    .load()`;
  const arrow = `import os\nimport pyarrow.flight as flight\nfrom dal_obscura.connectors import DalObscuraClient\n\nwith DalObscuraClient(os.environ["DAL_OBSCURA_FLIGHT_URI"],\n                     auth_token=lambda: os.environ["DAL_OBSCURA_TOKEN"]) as client:\n    for batch in client.read_batches(\n        catalog=${catalog}, target=${target}, columns=["*"]\n    ):\n        consume(batch)`;
  return <section className="consumer-view"><div className="management-head"><div><span className="eyebrow">CONSUMERS</span><h2>Read this governed asset</h2><p className="muted">Use the same Arrow Flight contract from Python, DuckDB, Spark, or another Arrow consumer. Credentials stay in environment or workload identity configuration.</p></div></div><div className="consumer-notice"><strong>Use a TLS Flight URI in production.</strong><span>Set <code>DAL_OBSCURA_FLIGHT_URI</code> and <code>DAL_OBSCURA_TOKEN</code> outside source control. The snippets request the governed logical target and receive only authorized nested fields and masks.</span></div><div className="consumer-grid"><CopyableCode title="Python / PyArrow" code={python} /><CopyableCode title="DuckDB relation" code={duckdb} /><CopyableCode title="Spark 3.x" code={spark} /><CopyableCode title="Raw Arrow batches" code={arrow} /></div></section>;
}

function CopyableCode({ title, code }: { title: string; code: string }) {
  const [copied, setCopied] = useState(false);
  async function copy() {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1_500);
    } catch {
      setCopied(false);
    }
  }
  return <article className="consumer-card"><div className="consumer-card-head"><h3>{title}</h3><button className="secondary compact" onClick={() => void copy()}>{copied ? "Copied" : "Copy"}</button></div><pre><code>{code}</code></pre></article>;
}

function AccessView({ asset, grants, session, onReload }: { asset: Asset; grants: AssetGrant[]; session: Session | null; onReload: () => void }) {
  const [owners, setOwners] = useState(asset.owners.join(", "));
  const [rows, setRows] = useState<AssetGrant[]>(grants);
  const [message, setMessage] = useState("");
  useEffect(() => { setOwners(asset.owners.join(", ")); setRows(grants); }, [asset, grants]);
  const canManageOwners = Boolean(session?.platform_admin);
  async function saveOwners() {
    if (!canManageOwners) return setMessage("Only a platform administrator can change owners.");
    try { await controlPlane.saveOwners(asset.id, owners.split(",").map((value) => value.trim()).filter(Boolean), asset.revision); setMessage("Owners updated. Existing drafts and publications are unchanged."); onReload(); } catch { setMessage("Owner update was rejected; refresh before retrying."); }
  }
  async function saveGrants() {
    const normalized = rows.filter((grant) => grant.principal.trim()).map((grant) => ({ ...grant, principal: grant.principal.trim() }));
    try { await controlPlane.saveGrants(asset.id, normalized, asset.revision); setRows(normalized); setMessage("Delegated capabilities updated. Changes take effect on the next authorized request."); onReload(); } catch { setMessage("Capability update was rejected; refresh before retrying."); }
  }
  const identityHint = session?.issuer ? `Federated identities use the exact issuer ${session.issuer}|subject and ${session.issuer}|group:name.` : "Federated identities should use the exact issuer|subject form so identical subjects from different providers stay isolated.";
  return <section className="management-view access-view"><div className="management-head"><div><span className="eyebrow">ACCESS</span><h2>Owners and delegated capabilities</h2><p className="muted">Owners receive read and edit scope. Publication and grant management are explicit capabilities enforced by the control plane.</p></div><button className="secondary" onClick={onReload}>Refresh</button></div><div className="form-card"><h3>Owners</h3><label className="form-label">Owner principals<input value={owners} onChange={(event) => setOwners(event.target.value)} placeholder="user:owner@example.com, group:data-stewards" disabled={!canManageOwners} /></label><p className="help">Comma-separated user or group principals. Removing the last owner is blocked while the asset is not safely reassigned. {identityHint}</p><button className="primary" onClick={() => void saveOwners()} disabled={!canManageOwners}>Save owners</button></div><div className="form-card"><h3>Delegated capabilities</h3>{rows.length ? <div className="grant-editor">{rows.map((grant, index) => <div className="grant-row" key={`${grant.principal}-${grant.capability}-${index}`}><input aria-label={`Grant principal ${index + 1}`} value={grant.principal} onChange={(event) => setRows((current) => current.map((item, row) => row === index ? { ...item, principal: event.target.value } : item))} placeholder="user:analyst@example.com" /><select aria-label={`Grant capability ${index + 1}`} value={grant.capability} onChange={(event) => setRows((current) => current.map((item, row) => row === index ? { ...item, capability: event.target.value as AssetGrant["capability"] } : item))}><option value="read">Read</option><option value="edit">Edit</option><option value="publish">Publish</option><option value="grant">Grant management</option></select><button className="danger" onClick={() => setRows((current) => current.filter((_, row) => row !== index))}>Remove</button></div>)}</div> : <p className="muted">No explicit delegated capabilities. Owners need explicit publish or grant-management assignments for those actions.</p>}<div className="editor-actions"><button className="secondary" onClick={() => setRows((current) => [...current, { principal: "", capability: "read" }])}>Add capability</button><button className="primary" onClick={() => void saveGrants()}>Save capabilities</button></div>{message && <p className="notice" role="status">{message}</p>}</div></section>;
}
function ManagementView({ page, data, loading, onReload, onLoadMore, historyLoading, auditLoading, filters, onFiltersChange, session }: { page: Exclude<Page, "assets">; data: ManagementData; loading: boolean; onReload: () => void; onLoadMore?: () => void; historyLoading?: boolean; auditLoading?: boolean; filters: AuditFilters; onFiltersChange: (filters: AuditFilters) => void; session: Session | null }) {
  if (loading) return <section className="coming-soon"><span className="eyebrow">{page.toUpperCase()}</span><h2>Loading {page}</h2><p>Checking the current workspace state and your capabilities.</p></section>;
  if (page === "changes") return <ChangesView history={data.history ?? []} nextCursor={data.historyNextCursor} onReload={onReload} onLoadMore={onLoadMore} loading={historyLoading ?? false} />;
  if (page === "activity") return <ActivityView history={data.history ?? []} events={data.events ?? []} nextCursor={data.eventsNextCursor} onLoadMore={onLoadMore} loading={auditLoading ?? false} filters={filters} onFiltersChange={onFiltersChange} summary={data.summary} observations={data.observations} />;
  if (page === "connections") return <ConnectionsView catalogs={data.catalogs ?? []} publications={data.publications ?? []} plugins={data.plugins ?? []} pluginStates={data.pluginStates ?? []} pluginPairs={data.pluginPairs ?? []} canActivate={Boolean(session?.platform_admin)} onReload={onReload} />;
  return <SettingsView runtime={data.runtime} providers={data.providers ?? []} publications={data.publications ?? []} onReload={onReload} />;
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

function ConnectionsView({ catalogs, publications, plugins, pluginStates, pluginPairs, canActivate, onReload }: { catalogs: Catalog[]; publications: WorkspacePublication[]; plugins: PluginDescriptor[]; pluginStates: PluginState[]; pluginPairs: PluginPair[]; canActivate: boolean; onReload: () => void }) {
  const [name, setName] = useState("");
  const catalogPlugins = plugins.filter((plugin) => plugin.kind === "catalog");
  const [pluginId, setPluginId] = useState(catalogPlugins[0]?.plugin_id ?? "iceberg.sql");
  const selectedPlugin = catalogPlugins.find((plugin) => plugin.plugin_id === pluginId);
  const fields = configFields(selectedPlugin);
  const effectiveFields = fields.length ? fields : (pluginId === "iceberg.sql" ? defaultCatalogFields : []);
  const [config, setConfig] = useState<Record<string, string>>({});
  const [message, setMessage] = useState("");
  const [tables, setTables] = useState<Array<Record<string, unknown>>>([]);
  const [discoveredCatalog, setDiscoveredCatalog] = useState("");
  const [selectedFormatId, setSelectedFormatId] = useState("");
  const [diagnostics, setDiagnostics] = useState<Record<string, CatalogDiagnostic>>({});
  const [diagnosing, setDiagnosing] = useState("");
  const [publishing, setPublishing] = useState(false);
  useEffect(() => {
    setPluginId((current) => catalogPlugins.some((plugin) => plugin.plugin_id === current) ? current : (catalogPlugins[0]?.plugin_id ?? "iceberg.sql"));
  }, [plugins]);
  useEffect(() => {
    setConfig((current) => Object.fromEntries(effectiveFields.map((field) => [field.name, current[field.name] ?? ""])));
  }, [pluginId]);
  async function save() {
    if (!name.trim()) return setMessage("Connection name is required.");
    const missing = effectiveFields.filter((field) => field.required && !config[field.name]?.trim());
    if (missing.length) return setMessage(`Required configuration missing: ${missing.map((field) => field.name).join(", ")}.`);
    const options: Record<string, unknown> = pluginId === "iceberg.sql" ? { type: "sql" } : {};
    for (const field of effectiveFields) {
      const value = config[field.name]?.trim();
      if (!value) continue;
      options[field.name] = field.secret
        ? { secret: value, scope: `catalog:${name.trim()}` }
        : value;
    }
    const existing = catalogs.find((catalog) => catalog.name === name.trim());
    try { await controlPlane.saveCatalog(name.trim(), pluginId === "iceberg.sql" ? "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog" : pluginId, options, existing?.revision); setMessage("Connection saved. Discovery remains bounded to this configured catalog."); setName(""); setConfig({}); onReload(); } catch (error) { setMessage((error as { status?: number })?.status === 409 ? "Connection changed elsewhere. Refresh before saving again." : "Connection was rejected by the control plane."); }
  }
  async function discover(catalog: string) {
    try {
      setDiscoveredCatalog(catalog);
      const catalogRow = catalogs.find((item) => item.name === catalog);
      const catalogPluginId = catalogRow?.module === "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog" ? "iceberg.sql" : catalogRow?.module;
      const choices = pluginPairs.filter((pair) => pair.catalog_plugin_id === catalogPluginId && pair.status === "admitted");
      setSelectedFormatId(choices.length === 1 ? choices[0].format_plugin_id : "");
      setTables((await controlPlane.discoverCatalogTables(catalog)).tables);
      setMessage(`Loaded table inventory for ${catalog}.`);
    } catch { setTables([]); setMessage("Discovery failed; source credentials and endpoint policy were not changed."); }
  }
  async function diagnose(catalog: string) {
    setDiagnosing(catalog);
    try {
      const result = await controlPlane.diagnoseCatalog(catalog);
      setDiagnostics((current) => ({ ...current, [catalog]: result }));
    } catch {
      setDiagnostics((current) => ({ ...current, [catalog]: { catalog, status: "unavailable", message: "Diagnostic request failed", checked_at: new Date().toISOString() } }));
    } finally {
      setDiagnosing("");
    }
  }
  async function govern(catalog: string, table: Record<string, unknown>) {
    const target = String(table.target ?? table.name ?? "").trim();
    const identifier = String(table.table_identifier ?? target).trim();
    if (!target || !identifier) return setMessage("The discovered table has no safe identifier.");
    const catalogRow = catalogs.find((item) => item.name === catalog);
    const catalogPluginId = catalogRow?.module === "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog" ? "iceberg.sql" : catalogRow?.module;
    const choices = pluginPairs.filter((pair) => pair.catalog_plugin_id === catalogPluginId && pair.status === "admitted");
    const formatId = selectedFormatId || (choices.length === 1 ? choices[0].format_plugin_id : undefined);
    if (!formatId) return setMessage("Select the table format explicitly before governing a discovered table.");
    const formatPlugin = plugins.find((plugin) => plugin.kind === "table_format" && plugin.plugin_id === formatId);
    if (!formatPlugin) return setMessage("No admitted table-format adapter is available for this catalog.");
    try { await controlPlane.saveAsset(catalog, target, formatPlugin.plugin_id, identifier); setMessage(`Governed asset ${target} registered. Assign owners and author a policy in Assets.`); await discover(catalog); } catch { setMessage("Asset registration was rejected; the source table was not changed."); }
  }
  async function createPublication() {
    if (!canActivate || publishing) return;
    setPublishing(true);
    try { await controlPlane.createWorkspacePublication(); setMessage("Configuration snapshot created. Activate it when ready."); onReload(); } catch { setMessage("Snapshot could not be created; resolve readiness errors before retrying."); } finally { setPublishing(false); }
  }
  async function activatePublication(id: string) {
    if (!canActivate || publishing) return;
    setPublishing(true);
    const current = publications.find((publication) => publication.active)?.id;
    try { await controlPlane.activateWorkspacePublication(id, current); setMessage("Configuration snapshot activated for new data-plane requests."); onReload(); } catch (error) { setMessage((error as { status?: number })?.status === 409 ? "Activation conflict: another administrator changed the serving generation. Refresh to compare impact before retrying." : "Activation was rejected; the current generation remains active. Refresh before retrying."); } finally { setPublishing(false); }
  }
  const pluginCards = plugins.length > 0 && <div className="form-card"><h3>Admitted adapters</h3><div className="plugin-list">{plugins.map((plugin) => <article className="plugin-card" key={`${plugin.kind}:${plugin.plugin_id}`}><div><strong>{plugin.display_name}</strong><small>{plugin.kind === "catalog" ? "Catalog" : "Table format"} · {plugin.plugin_id} · v{plugin.version}</small></div><div className="capability-list">{plugin.capabilities.map((capability) => <span className="pill" key={capability}>{capability.replaceAll("_", " ")}</span>)}</div></article>)}</div></div>;
  const discoveredChoices = pluginPairs.filter((pair) => pair.catalog_plugin_id === (catalogs.find((item) => item.name === discoveredCatalog)?.module === "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog" ? "iceberg.sql" : catalogs.find((item) => item.name === discoveredCatalog)?.module) && pair.status === "admitted");
  return <section className="management-view"><div className="management-head"><div><span className="eyebrow">CONNECTIONS</span><h2>Catalog connections</h2><p className="muted">Choose from adapters admitted by the control plane. Credentials stay in server configuration and are never rendered.</p></div><button className="secondary" onClick={onReload}>Refresh</button></div>{pluginStates.length > 0 && <p className="help">Adapter status: {pluginStates.map((state) => state.plugin_id + " " + (state.lifecycle ?? state.status)).join(", ")}</p>}{pluginCards}<div className="form-card"><h3>Add or update catalog</h3><div className="form-grid"><label>Name<input value={name} onChange={(event) => setName(event.target.value)} placeholder="analytics" /></label><>{catalogPlugins.length > 1 && <label>Catalog adapter<select value={pluginId} onChange={(event) => setPluginId(event.target.value)}>{catalogPlugins.map((plugin) => <option key={plugin.plugin_id} value={plugin.plugin_id}>{plugin.display_name || plugin.plugin_id}</option>)}</select></label>}{effectiveFields.map((field) => <label key={field.name}>{configFieldLabel(field.name)}{field.required ? "" : <span className="muted"> (optional)</span>}<input value={config[field.name] ?? ""} onChange={(event) => setConfig((current) => ({ ...current, [field.name]: event.target.value }))} placeholder={field.name === "uri" ? "postgresql+psycopg://catalog.internal:5432/catalog" : field.name === "password" ? "prod/catalog/password" : undefined} type={field.secret ? "password" : field.type === "number" ? "number" : "text"} /></label>)}</></div><p className="help connection-note">Use a URI without userinfo or query credentials. The password field is a name resolved by the deployment secret provider; never paste a password or token here.</p><button className="primary" onClick={() => void save()}>Save connection</button>{message && <p className="notice">{message}</p>}</div>{canActivate && <div className="form-card"><div className="management-head"><div><h3>Configuration generations</h3><p className="help">Catalog, runtime, identity, and asset drafts become data-plane state only after an explicit snapshot activation.</p></div><button className="primary" disabled={publishing} onClick={() => void createPublication()}>{publishing ? "Working…" : "Create snapshot"}</button></div>{publications.length ? <div className="table-wrap"><table><thead><tr><th>Generation</th><th>State</th><th>Impact</th><th>Manifest</th><th>Created</th><th>Action</th></tr></thead><tbody>{publications.map((publication) => <tr key={publication.id}><td><code>{publication.id.slice(0, 12)}</code></td><td>{publication.active ? "Active" : "Staged"}</td><td>{publication.asset_count} assets · {publication.catalog_count} catalogs</td><td><code>{publication.manifest_hash.slice(0, 12)}</code></td><td>{new Date(publication.created_at).toLocaleString()}</td><td>{publication.active ? <span className="pill">Serving</span> : <button className="secondary compact" disabled={publishing} onClick={() => void activatePublication(publication.id)}>Activate</button>}</td></tr>)}</tbody></table></div> : <p className="muted">No staged generations exist yet.</p>}</div>}{discoveredChoices.length > 1 && <div className="form-card"><h3>Select table format</h3><p className="help">This catalog advertises multiple output formats. Choose one before governing a discovered table.</p><select aria-label="Selected table format" value={selectedFormatId} onChange={(event) => setSelectedFormatId(event.target.value)}><option value="">Choose a format…</option>{discoveredChoices.map((pair) => <option key={pair.format_plugin_id} value={pair.format_plugin_id}>{pair.format_plugin_id} · handle v{pair.handle_versions.join(", ")}</option>)}</select></div>}{catalogs.length ? <div className="card-list">{catalogs.map((catalog) => { const diagnostic = diagnostics[catalog.name]; return <article className="management-card" key={catalog.id}><div><h3>{catalog.name}</h3><p className="muted">{catalogDisplayName(catalog)}</p>{diagnostic && <p className={diagnostic.status === "ready" ? "diagnostic ready" : "diagnostic unavailable"} role="status">{diagnostic.message}{diagnostic.table_count !== undefined ? ` · ${diagnostic.table_count} tables` : ""}</p>}</div><div className="card-actions"><button className="secondary" disabled={diagnosing === catalog.name} onClick={() => void diagnose(catalog.name)}>{diagnosing === catalog.name ? "Checking…" : "Check connection"}</button><button className="secondary" onClick={() => void discover(catalog.name)}>Discover tables</button></div></article>; })}</div> : <div className="empty-result"><strong>No catalogs configured</strong><p>Connect an admitted catalog adapter to begin asset onboarding.</p></div>}{tables.length > 0 && <div className="table-wrap"><table><thead><tr><th>Table</th><th>Backend</th><th>Governed</th><th>Action</th></tr></thead><tbody>{tables.map((table, index) => <tr key={String(table.name ?? index)}><td>{String(table.name ?? "Unknown")}</td><td>{String(table.backend ?? "iceberg")}</td><td>{table.governed ? "Yes" : "No"}</td><td>{table.governed ? <span className="pill">Registered</span> : <button className="secondary compact" onClick={() => void govern(discoveredCatalog, table)}>Govern table</button>}</td></tr>)}</tbody></table></div>}</section>;
}

function catalogDisplayName(catalog: Catalog): string {
  return catalog.module === "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
    ? "Iceberg SQL catalog"
    : `Catalog adapter · ${catalog.module}`;
}

function SettingsView({ runtime, providers, publications, onReload }: { runtime?: RuntimeSettings | null; providers: AuthProvider[]; publications: WorkspacePublication[]; onReload: () => void }) {
  const [form, setForm] = useState<RuntimeSettings>(runtime ?? { ticket_ttl_seconds: 900, max_tickets: 64, max_ticket_exchanges: 2 });
  const [message, setMessage] = useState("");
  useEffect(() => setForm(runtime ?? { ticket_ttl_seconds: 900, max_tickets: 64, max_ticket_exchanges: 2 }), [runtime]);
  async function save() { try { await controlPlane.saveRuntimeSettings(form); setMessage("Runtime settings saved as draft configuration. Publish to make worker behavior change."); onReload(); } catch { setMessage("Settings update was rejected; the previous values remain active."); } }
  const active = publications.find((publication) => publication.active);
  const stagedCount = publications.filter((publication) => !publication.active).length;
  return <section className="management-view"><div className="management-head"><div><span className="eyebrow">SETTINGS</span><h2>Runtime and identity</h2><p className="muted">These controls affect ticket fan-out and authentication. Changes are server-validated and do not expose secrets.</p></div><button className="secondary" onClick={onReload}>Refresh</button></div><div className="form-card"><h3>Runtime limits</h3><div className="form-grid three"><label>Ticket TTL (seconds)<input type="number" min="1" value={form.ticket_ttl_seconds} onChange={(event) => setForm({ ...form, ticket_ttl_seconds: Number(event.target.value) })} /></label><label>Max tickets<input type="number" min="1" value={form.max_tickets} onChange={(event) => setForm({ ...form, max_tickets: Number(event.target.value) })} /></label><label>Ticket exchanges<input type="number" min="1" value={form.max_ticket_exchanges} onChange={(event) => setForm({ ...form, max_ticket_exchanges: Number(event.target.value) })} /></label></div><button className="primary" onClick={() => void save()}>Save runtime settings</button>{message && <p className="notice">{message}</p>}</div><div className="form-card"><h3>Configuration state</h3>{active ? <p><span className="status-dot ready" /> Serving generation <code>{active.id.slice(0, 12)}</code> · {active.asset_count} assets · {active.catalog_count} catalogs</p> : <p><span className="status-dot unavailable" /> No generation is serving yet.</p>}<p className="help">{stagedCount ? `${stagedCount} staged generation${stagedCount === 1 ? " is" : "s are"} waiting for explicit administrator activation.` : "Save settings creates draft configuration; create and activate a snapshot from Connections when ready."}</p></div><div className="form-card"><h3>Authentication providers</h3>{providers.length ? <ul className="provider-list">{providers.map((provider) => <li key={provider.id}><span className={provider.enabled ? "status-dot ready" : "status-dot unavailable"} /><div><strong>{provider.module.split(".").at(-1)}</strong><small>Order {provider.ordinal} · {provider.enabled ? "Enabled" : "Disabled"}</small></div></li>)}</ul> : <div className="empty-result"><strong>No provider configured</strong><p>Production startup must fail closed until an approved identity provider is enabled.</p></div>}</div></section>;
}
function WorkspaceMessage({ showAuth = false, title, message, retry, authConfig, sessionOptions, bootstrapToken, onBootstrapToken, onBootstrapLogin, onLogin, loggingIn, authError }: { showAuth?: boolean; title: string; message: string; retry?: () => void; authConfig?: UiAuthConfig | null; sessionOptions?: SessionOptions | null; bootstrapToken?: string; onBootstrapToken?: (value: string) => void; onBootstrapLogin?: () => void; onLogin?: (loginHint: string) => void; loggingIn?: boolean; authError?: string }) {
  const hasLoginMethod = Boolean(authConfig?.authority || sessionOptions?.bootstrap_enabled || authConfig?.login_shortcuts?.some((shortcut) => shortcut.demo_login_path));
  return <section className={showAuth ? "coming-soon auth-panel" : "coming-soon"}><span className="eyebrow">{showAuth ? "WORKSPACE ACCESS" : "WORKSPACE"}</span><h2>{title}</h2><p>{message}</p>{showAuth && authConfig?.authority && <button className="primary login-shortcut" onClick={controlPlane.startLogin}>Sign in with SSO</button>}{showAuth && sessionOptions?.bootstrap_enabled && onBootstrapToken && onBootstrapLogin && <form className="bootstrap-login" onSubmit={(event) => { event.preventDefault(); onBootstrapLogin(); }}><label>Local control-plane token<input type="password" autoComplete="current-password" value={bootstrapToken ?? ""} onChange={(event) => onBootstrapToken(event.target.value)} placeholder="Paste the configured local token" /></label><button className="secondary" type="submit" disabled={loggingIn}>{loggingIn ? "Signing in…" : "Sign in locally"}</button></form>}{showAuth && authConfig?.login_shortcuts?.map((shortcut) => shortcut.demo_login_path && onLogin ? <button className="secondary login-shortcut" disabled={loggingIn} key={shortcut.login_hint} onClick={() => onLogin(shortcut.login_hint)}>{loggingIn ? "Signing in…" : `Use demo persona · ${shortcut.label}`}</button> : null)}{showAuth && authError && <p className="auth-error" role="alert">{authError}</p>}{showAuth && !hasLoginMethod && <p className="help">No browser identity provider is configured. Ask an operator to configure OIDC before signing in.</p>}{retry && <button className="secondary" onClick={() => void retry()}>Retry connection</button>}</section>; }
function titleFor(page: Page) { return ({ changes: "Changes", activity: "Activity", connections: "Connections", settings: "Settings", assets: "Assets" })[page]; }
function saveLabel(state: SaveState) { return ({ saved: "Saved draft", saving: "Saving draft", unsaved: "Unsaved changes", failed: "Save failed" })[state]; }
function workspaceLabel(state: WorkspaceState) { return ({ loading: "Checking access", ready: "Connected", demo: "Explicit demo", unavailable: "Unavailable" })[state]; }
createRoot(document.getElementById("root")!).render(<App />);
