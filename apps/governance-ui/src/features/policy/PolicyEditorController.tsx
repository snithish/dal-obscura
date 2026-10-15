import { useLayoutEffect, useEffect, useRef, useState } from "react";
import { AssetWorkspace } from "../assets/AssetWorkspace";
import { useSessionScope } from "../identity/SessionBoundary";
import { sessionQueryScope } from "../../data/query_scope";
import {
  controlPlane,
  type ApiFailure,
  type Asset,
  type AssetAccess,
  type AssetGrant,
  type AssetPage,
  type PolicyRule,
  type Preview,
} from "../../data/api";
import type { InfiniteData, QueryClient } from "@tanstack/react-query";
import type { AssetTab } from "../../app/navigation/routes";
import { useConfirmation } from "../../shared/ConfirmationProvider";
import { recoveryMessage } from "../../data/recovery";
import { isAbortError } from "../../data/async";
import { usePolicyDraft } from "./usePolicyDraft";
import { parseTestClaims } from "./policy_test";

export function PolicyEditorController({
  asset,
  access,
  grants,
  initialTab,
  onTabChange,
  onBack,
  onReloadAccess,
  onDirtyChange,
}: {
  asset: Asset;
  access: AssetAccess;
  grants: AssetGrant[];
  initialTab?: AssetTab;
  onTabChange: (tab: AssetTab) => void;
  onBack: () => void;
  onReloadAccess: () => void;
  onDirtyChange: (dirty: boolean) => void;
}) {
  const { confirm } = useConfirmation();
  const {
    session,
    generation: sessionGeneration,
    queryClient,
    registerRequest,
    isCurrent: sessionIsCurrent,
  } = useSessionScope();
  const active = useRef(true);
  const isCurrent = (captured: number) => active.current && sessionIsCurrent(captured);
  const sessionCacheKey = sessionQueryScope(session, sessionGeneration);
  const previewPendingRef = useRef(false);
  const [formDirty, setFormDirty] = useState(false);
  const [revokingTokens, setRevokingTokens] = useState(false);
  const revokePending = useRef(false);
  const draft = usePolicyDraft({
    rules: asset.policy_rules ?? [],
    revision: asset.policy_revision ?? 0,
  });
  const { rules, revision: policyRevision, status: saveState, errors: fieldErrors } = draft;
  const [previewBusy, setPreviewBusy] = useState(false);
  const [previewError, setPreviewError] = useState("");
  const previewInputEpoch = useRef(0);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [previewPrincipal, setPreviewPrincipal] = useState("analyst.alex");
  const [previewGroups, setPreviewGroups] = useState("us-analysts");
  const [previewProvider, setPreviewProvider] = useState("");
  const [previewClaims, setPreviewClaims] = useState("{}");
  const [notice, setNotice] = useState(
    asset.policy_rules?.length
      ? "Loaded the current live policy."
      : "No policy rules are configured. All columns return NULL until a rule overrides their masks.",
  );

  useLayoutEffect(() => {
    onDirtyChange(formDirty || saveState === "unsaved" || saveState === "failed");
  }, [formDirty, saveState, onDirtyChange]);
  useEffect(() => {
    active.current = true;
    return () => {
      active.current = false;
      onDirtyChange(false);
    };
  }, [onDirtyChange]);
  function reportFormDirty(value: boolean) {
    setFormDirty(value);
    const status = draft.current().status;
    onDirtyChange(value || status === "unsaved" || status === "failed");
  }
  function invalidateAssetQueries(assetId: string) {
    void queryClient.invalidateQueries({
      queryKey: ["asset", sessionCacheKey, assetId],
    });
    void queryClient.invalidateQueries({
      queryKey: ["asset-inventory", sessionCacheKey],
    });
    void queryClient.invalidateQueries({
      queryKey: ["management", sessionCacheKey],
    });
  }

  function discardPolicyChanges() {
    draft.dispatch({ type: "discard" });
    setFormDirty(false);
    setPreview(null);
    setPreviewError("");
    setNotice("Discarded all local changes. Showing the last saved policy.");
  }
  function allowAllPolicy() {
    draft.dispatch({ type: "allowAll" });
    setPreview(null);
    setNotice(
      "Allow all is staged. Save policy to bypass all masks and row filters for authenticated readers.",
    );
  }
  function updateRule(change: (rule: PolicyRule) => PolicyRule, targetIndex: number) {
    draft.update(targetIndex, change);
    setPreview(null);
    setNotice("Policy changed locally. Test saved policy to evaluate the last saved version.");
  }
  function addRule() {
    draft.dispatch({ type: "add" });
    setPreview(null);
    setNotice("New rule added locally. Add at least one principal before saving.");
  }
  function duplicateRule(index: number) {
    draft.dispatch({ type: "duplicate", index });
    setPreview(null);
    setNotice("Rule duplicated locally. Check its principals and fields before saving.");
  }
  function removeRule(index: number) {
    draft.dispatch({ type: "remove", index });
    setPreview(null);
    setNotice("Rule removed locally. Save policy to apply the change.");
  }
  async function savePolicy(revokeExistingTokens = false) {
    const submission = draft.beginSave();
    if (!submission) return;
    const assetId = asset.id;
    const request = registerRequest();
    try {
      // Persist display order without changing component identities during edits.
      const persistedRules = submission.rules.map((rule, index) => ({
        ...rule,
        ordinal: (index + 1) * 10,
      }));
      const saved = await controlPlane.replaceAssetPolicy(
        assetId,
        submission.revision,
        persistedRules,
        revokeExistingTokens,
        request.signal,
      );
      if (!isCurrent(sessionGeneration) || !draft.saved(submission, saved.policy_revision)) return;
      markInventoryPolicySaved(
        queryClient,
        sessionCacheKey,
        assetId,
        saved.policy_revision,
        Boolean(submission.rules.length),
      );
      setNotice(
        revokeExistingTokens
          ? `Live policy saved; revoked ${saved.revoked_token_count} active token(s).`
          : "Live policy saved. Existing tokens remain valid until expiry unless an owner revokes them.",
      );
      invalidateAssetQueries(assetId);
    } catch (error) {
      if (isAbortError(error) || !isCurrent(sessionGeneration)) return;
      if (draft.failed(submission, (error as ApiFailure).fieldErrors ?? [])) {
        setNotice(
          recoveryMessage(error, "Save failed. Your unsaved changes remain in this browser."),
        );
      }
    } finally {
      if (isCurrent(sessionGeneration)) draft.dispatch({ type: "cancelSave", submission });
      request.finish();
    }
  }
  function changeTestPersona(setter: (value: string) => void, value: string) {
    previewInputEpoch.current += 1;
    setter(value);
    setPreview(null);
    setPreviewError("");
  }

  async function runPreview() {
    if (saveState === "saving") {
      setNotice("Policy save is in progress. Try the saved-policy test again when it completes.");
      return;
    }
    if (previewPendingRef.current) return;
    const parsed = parseTestClaims(previewClaims);
    if (parsed.error || (!previewProvider && !previewPrincipal.trim())) {
      setPreviewError(parsed.error ?? "Enter a principal to test.");
      return;
    }
    const personaScope = previewInputEpoch.current;
    setPreviewBusy(true);
    setPreviewError("");
    setPreview(null);
    previewPendingRef.current = true;
    const editScope = draft.current().editVersion;
    const revision = policyRevision;
    const request = registerRequest();
    try {
      const claims = parsed.claims;
      const result: Preview = await controlPlane.evaluate(
        asset.id,
        {
          principal: previewPrincipal.trim() || "synthetic",
          ...(previewProvider ? { provider_ordinal: Number(previewProvider) } : {}),
          groups: previewGroups
            .split(",")
            .map((value) => value.trim())
            .filter(Boolean),
          claims,
        },
        request.signal,
      );
      if (
        !isCurrent(sessionGeneration) ||
        editScope !== draft.current().editVersion ||
        revision !== draft.current().revision ||
        personaScope !== previewInputEpoch.current
      )
        return;
      setPreview(result);
      setNotice(
        `Server-side evaluation completed: ${result.decision === "allow" ? "allowed" : "denied"}.`,
      );
    } catch (error) {
      if (isAbortError(error) || !isCurrent(sessionGeneration)) return;
      if (
        !isCurrent(sessionGeneration) ||
        editScope !== draft.current().editVersion ||
        revision !== draft.current().revision ||
        personaScope !== previewInputEpoch.current
      )
        return;
      setPreview(null);
      setPreviewError(recoveryMessage(error, "Policy test could not run. Try again."));
    } finally {
      request.finish();
      if (isCurrent(sessionGeneration)) {
        previewPendingRef.current = false;
        setPreviewBusy(false);
      }
    }
  }

  async function revokeAssetTokens() {
    if (revokePending.current) return;
    if (
      !(await confirm({
        title: "Revoke all active tokens?",
        message: `Revoke every active token for ${asset.name}? Existing readers will be denied on their next token exchange and must request new access.`,
        confirmLabel: "Revoke tokens",
        destructive: true,
      }))
    )
      return;
    if (!isCurrent(sessionGeneration)) return;
    revokePending.current = true;
    setRevokingTokens(true);
    const request = registerRequest();
    try {
      const result = await controlPlane.revokeAssetTokens(asset.id, request.signal);
      if (!isCurrent(sessionGeneration) || request.signal.aborted) return;
      setNotice(`Revoked ${result.revoked_token_count} active token(s).`);
      invalidateAssetQueries(asset.id);
    } catch (error) {
      if (isCurrent(sessionGeneration) && !isAbortError(error))
        setNotice(recoveryMessage(error, "Could not revoke this asset's tokens."));
    } finally {
      request.finish();
      if (isCurrent(sessionGeneration)) {
        revokePending.current = false;
        setRevokingTokens(false);
      }
    }
  }

  return (
    <AssetWorkspace
      initialTab={initialTab}
      onTabChange={onTabChange}
      asset={asset}
      access={access}
      grants={grants}
      onBack={onBack}
      rules={rules}
      activeRevision={policyRevision}
      saveState={saveState}
      notice={notice}
      fieldErrors={fieldErrors}
      onUpdateRule={updateRule}
      onAddRule={addRule}
      onRemoveRule={removeRule}
      onDuplicateRule={duplicateRule}
      onDiscard={discardPolicyChanges}
      onAllowAll={allowAllPolicy}
      onSave={(revokeExistingTokens) => void savePolicy(revokeExistingTokens)}
      onPreview={() => void runPreview()}
      previewProvider={previewProvider}
      onPreviewProvider={(value) => changeTestPersona(setPreviewProvider, value)}
      previewPrincipal={previewPrincipal}
      previewGroups={previewGroups}
      previewClaims={previewClaims}
      onPreviewPrincipal={(value) => changeTestPersona(setPreviewPrincipal, value)}
      onPreviewGroups={(value) => changeTestPersona(setPreviewGroups, value)}
      onPreviewClaims={(value) => changeTestPersona(setPreviewClaims, value)}
      preview={preview}
      previewBusy={previewBusy}
      previewError={previewError}
      session={session}
      onReloadAccess={onReloadAccess}
      revokingTokens={revokingTokens}
      onRevokeTokens={() => void revokeAssetTokens()}
      onDirtyChange={reportFormDirty}
      queryClient={queryClient}
      sessionScope={sessionCacheKey}
    />
  );
}

function markInventoryPolicySaved(
  client: QueryClient,
  scope: string,
  assetId: string,
  revision: number,
  configured: boolean,
) {
  client.setQueriesData<InfiniteData<AssetPage>>(
    { queryKey: ["asset-inventory", scope] },
    (current) =>
      current && {
        ...current,
        pages: current.pages.map((page) => ({
          ...page,
          items: page.items.map((asset) =>
            asset.id === assetId
              ? {
                  ...asset,
                  policy_revision: revision,
                  policy_status: configured ? "configured" : "missing",
                }
              : asset,
          ),
        })),
      },
  );
}
