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
      modulePath: "",
      type: "rest",
      uri: "http://localhost:8181",
      warehouse: "warehouse",
      extraOptionsJson: "",
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

  test("maps Unity Catalog as a REST-compatible catalog with extra options", () => {
    const form: CatalogForm = {
      adapter: "unity",
      modulePath: "",
      type: "rest",
      uri: "https://workspace.example/api/2.1/unity-catalog/iceberg-rest",
      warehouse: "main",
      extraOptionsJson: '{"token":"${UNITY_TOKEN}"}',
    };

    expect(catalogPayloadFromForm(form)).toEqual({
      module: CATALOG_ADAPTERS.unity.module,
      options: {
        type: "rest",
        uri: "https://workspace.example/api/2.1/unity-catalog/iceberg-rest",
        warehouse: "main",
        token: "${UNITY_TOKEN}",
      },
    });
  });

  test("allows a custom adapter module with JSON options", () => {
    const form: CatalogForm = {
      adapter: "custom",
      modulePath: "company.catalogs.UnityCatalog",
      type: "rest",
      uri: "",
      warehouse: "",
      extraOptionsJson: '{"endpoint":"https://catalog.example","profile":"prod"}',
    };

    expect(catalogPayloadFromForm(form)).toEqual({
      module: "company.catalogs.UnityCatalog",
      options: {
        endpoint: "https://catalog.example",
        profile: "prod",
      },
    });
  });

  test("labels known backend module paths without exposing them in the catalog list", () => {
    expect(catalogAdapterLabel(CATALOG_ADAPTERS.iceberg.module, { provider: "unity" })).toBe(
      "Unity Catalog",
    );
    expect(catalogAdapterLabel(CATALOG_ADAPTERS.iceberg.module, { type: "sql" })).toBe(
      "Iceberg catalog",
    );
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
      modulePath: "",
      type: "sql",
      uri: "sqlite:///catalog.db",
      warehouse: "",
      extraOptionsJson: "",
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
