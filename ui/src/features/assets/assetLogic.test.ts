import { describe, expect, test } from "vitest";

import {
  assetOptionsFromForm,
  filterAssets,
  ownersFromRows,
  type Asset,
} from "./assetLogic";

const assets: Asset[] = [
  {
    backend: "iceberg",
    catalog: "analytics",
    draft_status: "changed",
    id: "asset-1",
    name: "default.users",
    owner_count: 0,
    owners: [],
    policy_status: "missing",
    table_identifier: "prod.users",
  },
  {
    backend: "iceberg",
    catalog: "finance",
    draft_status: "clean",
    id: "asset-2",
    name: "billing.invoices",
    owner_count: 2,
    owners: ["user:alice@example.com", "group:finance"],
    policy_status: "configured",
    table_identifier: "prod.invoices",
  },
];

describe("assetOptionsFromForm", () => {
  test("rejects non-numeric snapshot ids before saving", () => {
    const result = assetOptionsFromForm("abc", []);

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toBe("Snapshot ID must be a number.");
    }
  });

  test("keeps numeric snapshots and simple option values", () => {
    const result = assetOptionsFromForm("42", [
      { key: "include_deleted", value: "false" },
      { key: "limit", value: "100" },
    ]);

    expect(result).toEqual({
      ok: true,
      options: { include_deleted: false, limit: 100, snapshot: 42 },
    });
  });
});

describe("ownersFromRows", () => {
  test("normalizes owner principal rows without duplicates", () => {
    expect(
      ownersFromRows([
        { principal: " user:alice@example.com " },
        { principal: "group:data-owners" },
        { principal: "user:alice@example.com" },
      ]),
    ).toEqual(["user:alice@example.com", "group:data-owners"]);
  });
});

describe("filterAssets", () => {
  test("filters assets by search text, catalog, owner status, and policy status", () => {
    expect(
      filterAssets(assets, {
        catalog: "analytics",
        owner: "unowned",
        search: "users",
        status: "missing",
      }),
    ).toEqual([assets[0]]);

    expect(
      filterAssets(assets, {
        catalog: "analytics",
        owner: "owned",
        search: "",
        status: "",
      }),
    ).toEqual([]);
  });
});
