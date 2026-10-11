"""Official catalog selection produces portable, credential-free format handles."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import CatalogConfig, ExecutionContext, TableIdentifier


@pytest.mark.parametrize("backend", ["sql", "in-memory"])
def test_official_catalog_backend_resolves_without_a_sql_only_contract(tmp_path, backend):
    from dal_obscura.sources.iceberg_catalog import IcebergCatalog

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
        assert captured.snapshot_id == str(table.current_snapshot().snapshot_id)
    finally:
        catalog.close()
