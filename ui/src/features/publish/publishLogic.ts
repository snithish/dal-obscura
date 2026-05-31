export type DraftReadiness = {
  assetCount: number;
  authProviderCount: number;
  catalogCount: number;
  missingPolicyCount: number;
  runtimeConfigured: boolean;
  unownedAssetCount: number;
};

export function canPublishDraft(readiness: DraftReadiness): boolean {
  return publishBlockers(readiness).length === 0;
}

export function publishBlockers(readiness: DraftReadiness): string[] {
  const blockers: string[] = [];
  if (readiness.catalogCount === 0) {
    blockers.push("Configure at least one catalog.");
  }
  if (!readiness.runtimeConfigured) {
    blockers.push("Configure runtime settings.");
  }
  if (readiness.authProviderCount === 0) {
    blockers.push("Configure at least one enabled auth provider.");
  }
  if (readiness.assetCount === 0) {
    blockers.push("Promote at least one governed asset.");
  }
  if (readiness.unownedAssetCount > 0) {
    blockers.push(
      `Resolve ${readiness.unownedAssetCount} unowned ${plural(
        readiness.unownedAssetCount,
        "asset",
      )}.`,
    );
  }
  if (readiness.missingPolicyCount > 0) {
    blockers.push(
      `Resolve ${readiness.missingPolicyCount} missing ${plural(
        readiness.missingPolicyCount,
        "policy",
        "policies",
      )}.`,
    );
  }
  return blockers;
}

export function activationConfirmationLabel(publicationId: string): string {
  return `Activate ${publicationId.slice(0, 8)}`;
}

function plural(count: number, singular: string, pluralForm = `${singular}s`): string {
  return count === 1 ? singular : pluralForm;
}
