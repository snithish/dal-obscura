"""Official catalog selection produces portable, credential-free format handles."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext, TableIdentifier


@pytest.mark.parametrize("backend", ["sql", "in-memory"])
def test_official_catalog_backend_resolves_without_a_sql_only_contract(tmp_path, backend):
    from dal_obscura_iceberg.catalog import IcebergCatalog

    options = {"type": backend, "warehouse": str(tmp_path / "warehouse")}
    if backend == "sql":
        options["uri"] = f"sqlite:///{tmp_path}/catalog.db"
    ctx = ExecutionContext(datetime.now(UTC) + timedelta(minutes=1), "catalog")
    catalog = IcebergCatalog(CatalogConfig("iceberg.catalog", "fixture", 1, options), ctx)
    try:
        provider = catalog._provider(ctx)
        provider.create_namespace("retail")
        table = provider.create_table("retail.sales", pa.schema([("id", pa.int64())]))
        table.append(pa.table({"id": [1]}, schema=table.schema().as_arrow()))
        identifier = TableIdentifier(("retail",), "sales")
        assert catalog.list_tables(ctx).entries == (identifier,)
        captured = catalog.resolve_table(identifier, ctx)
        assert captured.metadata == {"metadata_location": table.metadata_location}
        snapshot = table.current_snapshot()
        assert snapshot is not None
        assert captured.snapshot_id == str(snapshot.snapshot_id)
    finally:
        catalog.close()


@pytest.mark.parametrize(
    "warehouse", [None, 7, {"path": "s3://bucket"}], ids=["null", "number", "object"]
)
def test_catalog_rejects_non_string_provider_properties(monkeypatch, warehouse):
    import pyiceberg.catalog
    from dal_obscura_iceberg.catalog import IcebergCatalog

    monkeypatch.setattr(pyiceberg.catalog, "load_catalog", lambda name, **properties: object())
    ctx = ExecutionContext(datetime.now(UTC) + timedelta(minutes=1), "invalid-properties")
    with pytest.raises(ValueError, match="properties must be strings"):
        IcebergCatalog(
            CatalogConfig(
                "iceberg.catalog", "fixture", 1, {"type": "in-memory", "warehouse": warehouse}
            ),
            ctx,
        )
