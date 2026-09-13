from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import ExecutionContext as PublicExecutionContext

from dal_obscura.common.plugin_api.contracts import (
    CatalogConfig,
    ExecutionContext,
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

    with pytest.raises(ValueError, match="remote or executable"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="example",
            api_version="1",
            config_version=1,
            distribution="example",
            version="1.0.0",
            config_schema={"widget": {"$ref": "https://example.invalid/form.json"}},
        )
    with pytest.raises(ValueError, match="deeply nested"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="example",
            api_version="1",
            config_version=1,
            distribution="example",
            version="1.0.0",
            config_schema={"a": {"b": {"c": {"d": {"e": {"f": {"g": {"h": {"i": 1}}}}}}}}},
        )
    with pytest.raises(ValueError, match="JSON-like"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="example",
            api_version="1",
            config_version=1,
            distribution="example",
            version="1.0.0",
            config_schema={"factory": object()},
        )
    with pytest.raises(ValueError, match="too many nodes"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="example",
            api_version="1",
            config_version=1,
            distribution="example",
            version="1.0.0",
            config_schema={"items": [list(range(64)) for _ in range(4)]},
        )


def test_plugin_contract_value_objects_validate_generation_and_schema_identity() -> None:
    config = CatalogConfig(plugin_id="iceberg.sql", instance_id="analytics", revision=0)
    assert config.revision == 0
    with pytest.raises(ValueError, match="cannot be negative"):
        CatalogConfig(plugin_id="iceberg.sql", instance_id="analytics", revision=-1)

    with pytest.raises(ValueError, match="SHA-256"):
        SchemaDescriptor(schema_version=1, fingerprint="bad", arrow_schema=pa.schema([]))


@pytest.mark.parametrize("context_type", [ExecutionContext, PublicExecutionContext])
def test_execution_context_rejects_ambiguous_or_unbounded_values(context_type) -> None:
    valid = {
        "deadline": datetime.now(timezone.utc),
        "correlation_id": "request-1",
        "capabilities": frozenset({"nested_schema"}),
    }
    context = context_type(**valid)
    assert context.correlation_id == "request-1"

    with pytest.raises(ValueError, match="timezone-aware"):
        context_type(**{**valid, "deadline": datetime.now()})
    with pytest.raises(ValueError, match="correlation ID"):
        context_type(**{**valid, "correlation_id": "\n"})
    with pytest.raises(ValueError, match="capabilities"):
        context_type(**{**valid, "capabilities": frozenset({""})})
    with pytest.raises(ValueError, match="cancellation"):
        context_type(**{**valid, "cancel_check": "later"})
