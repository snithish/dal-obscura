export const CATALOG_ADAPTERS = {
  iceberg: {
    label: "Iceberg catalog",
    module: "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog",
  },
} as const;

export type CatalogAdapter = keyof typeof CATALOG_ADAPTERS;
export type CatalogType = "rest" | "sql";

export type CatalogForm = {
  adapter: CatalogAdapter;
  type: CatalogType;
  uri: string;
  warehouse: string;
};

export type CatalogPayload = {
  module: string;
  options: Record<string, unknown>;
};

export function catalogPayloadFromForm(form: CatalogForm): CatalogPayload {
  return {
    module: CATALOG_ADAPTERS[form.adapter].module,
    options: catalogOptionsFromForm(form),
  };
}

export function catalogAdapterLabel(module: string): string {
  return (
    Object.values(CATALOG_ADAPTERS).find((adapter) => adapter.module === module)?.label ??
    "Custom adapter"
  );
}

export function formFromCatalogOptions(options: Record<string, unknown>): CatalogForm {
  return {
    adapter: "iceberg",
    type: options.type === "rest" ? "rest" : "sql",
    uri: typeof options.uri === "string" ? options.uri : "",
    warehouse: typeof options.warehouse === "string" ? options.warehouse : "",
  };
}

function catalogOptionsFromForm(form: CatalogForm): Record<string, unknown> {
  return {
    type: form.type,
    uri: form.uri,
    ...(form.type === "rest" && form.warehouse ? { warehouse: form.warehouse } : {}),
  };
}
