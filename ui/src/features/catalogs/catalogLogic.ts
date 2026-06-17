export const CATALOG_ADAPTERS = {
  iceberg: {
    label: "Iceberg catalog",
    module: "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog",
  },
  unity: {
    label: "Unity Catalog",
    module: "dal_obscura.data_plane.infrastructure.adapters.unity_catalog.UnityCatalog",
  },
  custom: {
    label: "Custom adapter",
    module: "",
  },
} as const;

export type CatalogAdapter = keyof typeof CATALOG_ADAPTERS;
export type CatalogType = "rest" | "sql";

export type CatalogForm = {
  adapter: CatalogAdapter;
  extraOptionsJson: string;
  modulePath: string;
  type: CatalogType;
  uri: string;
  warehouse: string;
};

export type CatalogPayload = {
  module: string;
  options: Record<string, unknown>;
};

export type DiscoveredCatalogTable = {
  backend: string;
  governed: boolean;
  name: string;
  table_identifier: string;
  target: string;
};

export type AssetPromotionPayload = {
  backend: string;
  options: Record<string, unknown>;
  table_identifier: string;
};

export function catalogPayloadFromForm(form: CatalogForm): CatalogPayload {
  return {
    module: catalogModuleFromForm(form),
    options: catalogOptionsFromForm(form),
  };
}

export function assetPayloadFromDiscoveredTable(
  table: DiscoveredCatalogTable,
): AssetPromotionPayload {
  return {
    backend: table.backend,
    options: {},
    table_identifier: table.table_identifier,
  };
}

export function catalogAdapterLabel(module: string, options: Record<string, unknown> = {}): string {
  if (options.provider === "unity" || isUnityUri(options.uri)) {
    return "Unity Catalog";
  }
  if (module === CATALOG_ADAPTERS.unity.module) {
    return "Unity Catalog";
  }
  if (module === CATALOG_ADAPTERS.iceberg.module) {
    return "Iceberg catalog";
  }
  return "Custom adapter";
}

export function formFromCatalogOptions(options: Record<string, unknown>): CatalogForm {
  if (options.provider === "unity" || isUnityUri(options.uri) || options.base_url) {
    return {
      adapter: "unity",
      extraOptionsJson: "",
      modulePath: "",
      type: "rest",
      uri:
        typeof options.base_url === "string"
          ? options.base_url
          : typeof options.uri === "string"
            ? options.uri
            : "",
      warehouse: typeof options.uc_catalog === "string" ? options.uc_catalog : "",
    };
  }
  return {
    adapter: "iceberg",
    extraOptionsJson: "",
    modulePath: "",
    type: options.type === "rest" ? "rest" : "sql",
    uri: typeof options.uri === "string" ? options.uri : "",
    warehouse: typeof options.warehouse === "string" ? options.warehouse : "",
  };
}

function catalogModuleFromForm(form: CatalogForm): string {
  if (form.adapter === "custom") {
    return form.modulePath.trim();
  }
  return CATALOG_ADAPTERS[form.adapter].module;
}

function catalogOptionsFromForm(form: CatalogForm): Record<string, unknown> {
  if (form.adapter === "custom") {
    return parseExtraOptions(form.extraOptionsJson);
  }
  if (form.adapter === "unity") {
    return {
      ...parseExtraOptions(form.extraOptionsJson),
      base_url: form.uri,
      ...(form.warehouse ? { uc_catalog: form.warehouse } : {}),
    };
  }
  return {
    ...parseExtraOptions(form.extraOptionsJson),
    type: form.type,
    uri: form.uri,
    ...(form.warehouse ? { warehouse: form.warehouse } : {}),
  };
}

function isUnityUri(value: unknown): boolean {
  return typeof value === "string" && value.toLowerCase().includes("unity-catalog");
}

function parseExtraOptions(raw: string): Record<string, unknown> {
  if (!raw.trim()) {
    return {};
  }
  const parsed = JSON.parse(raw) as unknown;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Catalog options JSON must be an object.");
  }
  return parsed as Record<string, unknown>;
}
