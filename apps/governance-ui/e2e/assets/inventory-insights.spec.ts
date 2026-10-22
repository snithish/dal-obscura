import type { ApiSchemas } from "../support/api-types";
import { expect, test } from "@playwright/test";
import { uxApi } from "../support/ux-fixture";

test("inventory uses server totals and revisions without invented token counts", async ({
  page,
}) => {
  await uxApi(page);
  await page.route("**/v1/workspace/summary", (route) =>
    route.fulfill({
      json: {
        asset_count: 42,
        catalog_count: 6,
        missing_policy_count: 7,
        enabled_auth_provider_count: 1,
        runtime_configured: true,
        unowned_asset_count: 0,
      } satisfies ApiSchemas["WorkspaceSummaryResponse"],
    }),
  );
  await page.goto("/#assets");
  const totals = page.getByRole("region", { name: "Inventory summary" });
  await expect(totals.getByText("42", { exact: true })).toBeVisible();
  await expect(totals.getByText("7", { exact: true })).toBeVisible();
  await expect(totals.getByText("Live tokens", { exact: true })).toHaveCount(0);
  await expect(page.getByText("rev 1", { exact: true })).toBeVisible();
});

test("policy change history shows recorded actor, time and revocations on demand", async ({
  page,
}) => {
  await uxApi(page);
  await page.route("**/v1/audit/events/page?*", (route) =>
    new URL(route.request().url()).searchParams.has("asset_id")
      ? route.fulfill({
          json: {
            items: [
              {
                id: "change",
                action: "asset.policy.replace",
                actor: "issuer|steward",
                resource_type: "asset",
                resource_id: "00000000-0000-4000-8000-000000000001",
                created_at: "2026-10-15T09:00:00Z",
                outcome: "success",
                details: { revoked_token_count: 3 },
              },
            ],
            next_cursor: null,
          } satisfies ApiSchemas["AuditEventPageResponse"],
        })
      : route.fulfill({
          json: { items: [], next_cursor: null } satisfies ApiSchemas["AuditEventPageResponse"],
        }),
  );
  await page.goto("/#assets");
  await page.getByText("View last policy change", { exact: true }).click();
  await expect(page.getByText("issuer|steward", { exact: true })).toBeVisible();
  await expect(page.getByText("3 tokens revoked", { exact: true })).toBeVisible();
  await expect(page.locator("time")).toHaveAttribute("datetime", "2026-10-15T09:00:00Z");
});

test("recent policy changes render inline from one shared bounded audit request", async ({
  page,
}) => {
  await uxApi(page);
  let requests = 0;
  await page.route("**/v1/audit/events/page?*", (route) => {
    requests += 1;
    return route.fulfill({
      json: {
        items: [
          {
            id: "recent",
            action: "asset.policy.replace",
            actor: "issuer|recent-steward",
            resource_type: "asset",
            resource_id: "00000000-0000-4000-8000-000000000001",
            created_at: new Date().toISOString(),
            outcome: "success",
            details: { revoked_token_count: 2 },
          },
        ],
        next_cursor: null,
      } satisfies ApiSchemas["AuditEventPageResponse"],
    });
  });
  await page.goto("/#assets");
  await expect(page.getByText("issuer|recent-steward", { exact: true })).toBeVisible();
  await expect(page.getByText("2 tokens revoked", { exact: true })).toBeVisible();
  expect(requests).toBe(1);
});

test("paginated activity reports a distinct-asset lower bound", async ({ page }) => {
  await uxApi(page);
  const event = {
    id: "change",
    action: "asset.owners.replace",
    actor: "steward",
    resource_type: "asset",
    resource_id: "same-asset",
    created_at: new Date().toISOString(),
    outcome: "success",
    details: {},
  };
  await page.route("**/v1/audit/events/page?*", (route) =>
    route.fulfill({
      json: {
        items: [event, { ...event, id: "second-change" }],
        next_cursor: "next-page",
      } satisfies ApiSchemas["AuditEventPageResponse"],
    }),
  );
  await page.goto("/#assets");
  const summary = page.getByRole("region", { name: "Inventory summary" });
  await expect(summary.getByText("1+", { exact: true })).toBeVisible();
  await expect(
    summary.getByText("At least this many; recent activity only", { exact: true }),
  ).toBeVisible();
});
