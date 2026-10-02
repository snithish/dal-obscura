from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any, cast

import pyarrow as pa
import pytest
from dal_obscura_plugin_api import (
    CatalogConfig,
    DiscoveryPage,
    ExecutionContext,
    PluginDescriptor,
    SchemaDescriptor,
    TableHandle,
    TableIdentifier,
)


def test_plugin_descriptor_rejects_unbounded_or_invalid_metadata() -> None:
    with pytest.raises(ValueError, match="Invalid plugin ID"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="../loader",
            api_version="2",
            config_version=1,
            distribution="example",
            version="1.0.0",
        )
    with pytest.raises(ValueError, match="unsupported capability"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="example",
            api_version="2",
            config_version=1,
            distribution="example",
            version="1.0.0",
            capabilities=frozenset({"executes_arbitrary_sql"}),
        )

    with pytest.raises(ValueError, match="remote or executable"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="example",
            api_version="2",
            config_version=1,
            distribution="example",
            version="1.0.0",
            config_schema={"widget": {"$ref": "https://example.invalid/form.json"}},
        )
    with pytest.raises(ValueError, match="deeply nested"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="example",
            api_version="2",
            config_version=1,
            distribution="example",
            version="1.0.0",
            config_schema={"a": {"b": {"c": {"d": {"e": {"f": {"g": {"h": {"i": 1}}}}}}}}},
        )
    with pytest.raises(ValueError, match="JSON-like"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="example",
            api_version="2",
            config_version=1,
            distribution="example",
            version="1.0.0",
            config_schema={"factory": object()},
        )
    with pytest.raises(ValueError, match="too many nodes"):
        PluginDescriptor(
            kind="catalog",
            plugin_id="example",
            api_version="2",
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
    with pytest.raises(ValueError, match="JSON-like"):
        CatalogConfig(
            plugin_id="iceberg.sql",
            instance_id="analytics",
            revision=0,
            options={"provider": object()},
        )
    with pytest.raises(ValueError, match="too many keys"):
        CatalogConfig(
            plugin_id="iceberg.sql",
            instance_id="analytics",
            revision=0,
            options={f"key-{index}": index for index in range(65)},
        )
    with pytest.raises(ValueError, match="Invalid catalog plugin ID"):
        TableHandle(
            catalog_plugin_id="../loader",
            catalog_instance_id="analytics",
            catalog_revision=0,
            identifier=TableIdentifier(namespace=("default",), name="users"),
            format_plugin_id="iceberg",
            handle_version=1,
        )
    with pytest.raises(ValueError, match="JSON-like"):
        TableHandle(
            catalog_plugin_id="iceberg.sql",
            catalog_instance_id="analytics",
            catalog_revision=0,
            identifier=TableIdentifier(namespace=("default",), name="users"),
            format_plugin_id="iceberg",
            handle_version=1,
            metadata={"provider": object()},
        )
    with pytest.raises(ValueError, match="too many entries"):
        DiscoveryPage(
            tuple(
                TableIdentifier(namespace=("default",), name=f"table-{index}")
                for index in range(501)
            )
        )
    with pytest.raises(ValueError, match="printable"):
        DiscoveryPage((), continuation="bad\ncontinuation")
    with pytest.raises(ValueError, match="bounded printable"):
        DiscoveryPage((), continuation=cast(str, []))
    with pytest.raises(ValueError, match="entries must be a tuple"):
        DiscoveryPage(cast(tuple[TableIdentifier, ...], []), continuation=None)

    with pytest.raises(ValueError, match="Arrow schema"):
        SchemaDescriptor(arrow_schema=cast(pa.Schema, "opaque"))

    identifier = TableIdentifier(namespace=("default",), name="users")
    with pytest.raises(ValueError, match="Invalid table-format plugin ID"):
        TableHandle(
            catalog_plugin_id="manifest",
            catalog_instance_id="fixture",
            catalog_revision=1,
            identifier=identifier,
            format_plugin_id="../pickle",
            handle_version=1,
        )


def test_execution_context_rejects_ambiguous_or_unbounded_values() -> None:
    deadline = datetime.now(timezone.utc)
    context = ExecutionContext(deadline=deadline, correlation_id="request-1")
    assert context.correlation_id == "request-1"

    with pytest.raises(ValueError, match="timezone-aware"):
        ExecutionContext(deadline=datetime.now(), correlation_id="request-1")
    with pytest.raises(ValueError, match="correlation ID"):
        ExecutionContext(deadline=deadline, correlation_id="\n")
    with pytest.raises(ValueError, match="cancellation"):
        ExecutionContext(
            deadline=deadline,
            correlation_id="request-1",
            cancel_check=cast(Callable[[], bool], "later"),
        )


def test_table_identifier_rejects_unbounded_or_non_printable_segments() -> None:
    with pytest.raises(ValueError, match="too many segments"):
        TableIdentifier(namespace=tuple("ns" for _ in range(32)), name="users")
    with pytest.raises(ValueError, match="bounded printable"):
        TableIdentifier(namespace=("default",), name="orders\narchive")
    with pytest.raises(ValueError, match="bounded printable"):
        TableIdentifier(namespace=("default",), name=cast(str, 42))


def test_table_handle_captures_nested_metadata_and_rejects_boolean_versions() -> None:
    metadata = {"options": {"roots": ["warehouse"]}}
    arguments: dict[str, Any] = {
        "catalog_plugin_id": "manifest",
        "catalog_instance_id": "fixture",
        "catalog_revision": 1,
        "identifier": TableIdentifier(namespace=("default",), name="users"),
        "format_plugin_id": "parquet.dataset",
        "handle_version": 1,
    }
    handle = TableHandle(**arguments, metadata=metadata)
    metadata["options"]["roots"].append("outside")
    assert handle.to_json()["metadata"] == {"options": {"roots": ["warehouse"]}}
    assert TableHandle.from_json(handle.to_json()) == handle
    with pytest.raises(TypeError):
        cast(dict, handle.metadata["options"])["roots"] = []
    for field in ("catalog_revision", "handle_version"):
        with pytest.raises(ValueError, match="integer"):
            invalid: dict[str, Any] = {**arguments, field: True}
            TableHandle(**invalid)
