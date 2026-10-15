"""Execution requires the registry captured by startup composition."""

import pytest

from dal_obscura.adapters.providers.discovery import discover_public_catalog_tables
from dal_obscura.adapters.providers.schema import load_source_schema


@pytest.mark.parametrize("operation", ["discovery", "schema"])
def test_provider_io_requires_a_startup_registry(operation):
    with pytest.raises(ValueError, match="startup plugin registry"):
        if operation == "discovery":
            discover_public_catalog_tables("analytics", "iceberg.catalog", {}, plugin_registry=None)
        else:
            load_source_schema(
                asset={"name": "default.users", "backend": "iceberg"},
                catalog={"name": "analytics", "plugin_id": "iceberg.catalog"},
                options={},
                plugin_registry=None,
                validate_metadata=lambda metadata: None,
            )
