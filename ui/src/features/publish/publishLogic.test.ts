import { describe, expect, test } from "vitest";

import {
  buildDraftReview,
  canPublishDraft,
  policyVersionLabel,
  publishBlockers,
  type DraftReadiness,
} from "./publishLogic";

describe("publish readiness", () => {
  test("blocks publishing when the draft has no assets or unresolved ownership and policy gaps", () => {
    const readiness: DraftReadiness = {
      assetCount: 0,
      authProviderCount: 0,
      catalogCount: 1,
      missingPolicyCount: 2,
      runtimeConfigured: false,
      unownedAssetCount: 1,
    };

    expect(canPublishDraft(readiness)).toBe(false);
    expect(publishBlockers(readiness)).toEqual([
      "Configure runtime settings.",
      "Configure at least one enabled auth provider.",
      "Promote at least one governed asset.",
      "Resolve 1 unowned asset.",
      "Resolve 2 missing policies.",
    ]);
  });

  test("allows publishing only after catalogs, assets, owners, and policies are ready", () => {
    expect(
      canPublishDraft({
        assetCount: 3,
        authProviderCount: 1,
        catalogCount: 1,
        missingPolicyCount: 0,
        runtimeConfigured: true,
        unownedAssetCount: 0,
      }),
    ).toBe(true);
  });
});

describe("policy version labels", () => {
  test("uses asset name and policy version", () => {
    expect(
      policyVersionLabel({
        active: true,
        asset_id: "asset-1",
        asset_name: "default.users",
        catalog: "analytics",
        created_at: "2026-06-25T00:00:00Z",
        policy_version: 123,
        target: "default.users",
      }),
    ).toBe("default.users v123");
  });
});

describe("draft review", () => {
  test("builds review state from workspace catalogs and assets", () => {
    expect(
      buildDraftReview({
        assets: [
          {
            catalog: "analytics",
            id: "asset-1",
            name: "default.users",
            policy_status: "ready",
          },
        ],
        catalogs: [{ name: "analytics", status: "configured" }],
      }),
    ).toEqual({
      asset_count: 1,
      assets: [
        {
          catalog: "analytics",
          id: "asset-1",
          name: "default.users",
          policy_status: "ready",
        },
      ],
      catalog_count: 1,
      catalogs: [{ name: "analytics", status: "configured" }],
    });
  });
});
