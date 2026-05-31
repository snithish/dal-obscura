import { describe, expect, test } from "vitest";

import {
  CATALOG_ADAPTERS,
  assetPayloadFromDiscoveredTable,
  catalogAdapterLabel,
  catalogPayloadFromForm,
  formFromCatalogOptions,
  type CatalogForm,
} from "./catalogLogic";

describe("catalog adapter form mapping", () => {
  test("uses a product-level adapter choice to build the backend module payload", () => {
    const form: CatalogForm = {
      adapter: "iceberg",
      type: "rest",
      uri: "http://localhost:8181",
      warehouse: "warehouse",
    };

    expect(catalogPayloadFromForm(form)).toEqual({
      module: CATALOG_ADAPTERS.iceberg.module,
      options: {
        type: "rest",
        uri: "http://localhost:8181",
        warehouse: "warehouse",
      },
    });
  });

  test("labels known backend module paths without exposing them in the catalog list", () => {
    expect(catalogAdapterLabel(CATALOG_ADAPTERS.iceberg.module)).toBe("Iceberg catalog");
    expect(catalogAdapterLabel("custom.module.Path")).toBe("Custom adapter");
  });

  test("hydrates a friendly form from saved catalog options", () => {
    expect(
      formFromCatalogOptions({
        type: "sql",
        uri: "sqlite:///catalog.db",
      }),
    ).toEqual({
      adapter: "iceberg",
      type: "sql",
      uri: "sqlite:///catalog.db",
      warehouse: "",
    });
  });
});

describe("discovered table promotion", () => {
  test("builds the governed asset payload from a discovered Iceberg table", () => {
    expect(
      assetPayloadFromDiscoveredTable({
        backend: "iceberg",
        governed: false,
        name: "prod.orders",
        table_identifier: "warehouse.prod.orders",
        target: "prod.orders",
      }),
    ).toEqual({
      backend: "iceberg",
      options: {},
      table_identifier: "warehouse.prod.orders",
    });
  });
});
