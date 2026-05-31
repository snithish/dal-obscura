import { describe, expect, test } from "vitest";

import {
  activationConfirmationLabel,
  canPublishDraft,
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

describe("activation confirmation", () => {
  test("uses the publication prefix in confirmation labels", () => {
    expect(activationConfirmationLabel("12345678-90ab-cdef")).toBe(
      "Activate 12345678",
    );
  });
});
