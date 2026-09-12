from __future__ import annotations

import pyarrow as pa
import pytest

from dal_obscura.common.plugin_api.contracts import (
    CatalogConfig,
    PluginDescriptor,
    SchemaDescriptor,
)


def test_plugin_descriptor_rejects_unbounded_or_invalid_metadata() -> None:
    with pytest.raises(ValueError, match="Invalid plugin ID"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="../loader",
            api_version="1",
            config_version=1,
            distribution="example",
            version="1.0.0",
        )
    with pytest.raises(ValueError, match="capabilities"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="example",
            api_version="1",
            config_version=1,
            distribution="example",
            version="1.0.0",
            capabilities=frozenset({""}),
        )


def test_plugin_contract_value_objects_validate_generation_and_schema_identity() -> None:
    config = CatalogConfig(plugin_id="iceberg.sql", instance_id="analytics", revision=0)
    assert config.revision == 0
    with pytest.raises(ValueError, match="cannot be negative"):
        CatalogConfig(plugin_id="iceberg.sql", instance_id="analytics", revision=-1)

    with pytest.raises(ValueError, match="SHA-256"):
        SchemaDescriptor(schema_version=1, fingerprint="bad", arrow_schema=pa.schema([]))
