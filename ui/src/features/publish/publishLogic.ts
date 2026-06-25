export type DraftReadiness = {
  assetCount: number;
  authProviderCount: number;
  catalogCount: number;
  missingPolicyCount: number;
  runtimeConfigured: boolean;
  unownedAssetCount: number;
};

export type PolicyVersionHistoryItem = {
  active: boolean;
  asset_id: string;
  asset_name: string;
  catalog: string;
  created_at: string;
  policy_version: number;
  target: string;
};

export type DraftReviewAsset = {
  catalog: string;
  id: string;
  name: string;
  policy_status: string;
};

export type DraftReviewCatalog = {
  name: string;
  status: string;
};

export type DraftReview = {
  asset_count: number;
  assets: DraftReviewAsset[];
  catalog_count: number;
  catalogs: DraftReviewCatalog[];
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

export function policyVersionLabel(item: PolicyVersionHistoryItem): string {
  return `${item.asset_name} v${item.policy_version}`;
}

export function buildDraftReview({
  assets,
  catalogs,
}: {
  assets: DraftReviewAsset[];
  catalogs: DraftReviewCatalog[];
}): DraftReview {
  return {
    asset_count: assets.length,
    assets,
    catalog_count: catalogs.length,
    catalogs,
  };
}

function plural(count: number, singular: string, pluralForm = `${singular}s`): string {
  return count === 1 ? singular : pluralForm;
}
