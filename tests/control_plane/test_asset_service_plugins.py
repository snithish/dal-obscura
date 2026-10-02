from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import Mock
from uuid import uuid4

import pytest
from dal_obscura_plugin_api import PluginDescriptor

from dal_obscura.control.asset_service import upsert_workspace_asset
from dal_obscura.control.errors import ValidationFailure


def _registry(*, overlap: bool = True) -> Any:
    capabilities = frozenset({"nested_schema"}) if overlap else frozenset({"snapshot_reads"})
    catalog = PluginDescriptor(
        kind="catalog",
        plugin_id="fixture.catalog",
        api_version="2",
        config_version=1,
        distribution="fixture",
        version="1.0.0",
        capabilities=capabilities,
        output_formats=frozenset({"fixture.format"}),
        handle_versions=frozenset({1}),
    )
    format_descriptor = PluginDescriptor(
        kind="table_format",
        plugin_id="fixture.format",
        api_version="2",
        config_version=1,
        distribution="fixture",
        version="1.0.0",
        capabilities=frozenset({"nested_schema"}),
        handle_versions=frozenset({1}),
        config_schema={"fields": [{"name": "format_option", "type": "string", "required": True}]},
    )
    registry = cast(Any, Mock())
    registry.admitted.return_value = {
        ("catalog", "fixture.catalog"): catalog,
        ("table_format", "fixture.format"): format_descriptor,
    }
    return registry


def _store() -> Mock:
    store = Mock()
    store.get_workspace.return_value = SimpleNamespace()
    store.get_workspace_catalog.return_value = {"plugin_id": "fixture.catalog"}
    store.upsert_asset.return_value = uuid4()
    return store


def test_asset_binding_validates_pair_capabilities_and_format_options() -> None:
    store = _store()

    with pytest.raises(ValidationFailure, match="missing required"):
        upsert_workspace_asset(
            store,
            "analytics",
            "events",
            "fixture.format",
            "default.events",
            {},
            plugin_registry=_registry(),
        )

    with pytest.raises(ValidationFailure, match="must be a string"):
        upsert_workspace_asset(
            store,
            "analytics",
            "events",
            "fixture.format",
            "default.events",
            {"format_option": 42},
            plugin_registry=_registry(),
        )

    result = upsert_workspace_asset(
        store,
        "analytics",
        "events",
        "fixture.format",
        "default.events",
        {"format_option": "safe"},
        plugin_registry=_registry(),
    )
    assert result["catalog"] == "analytics"
    store.upsert_asset.assert_called_once()


def test_asset_binding_rejects_non_overlapping_plugin_capabilities() -> None:
    with pytest.raises(ValidationFailure, match="capabilities do not overlap"):
        upsert_workspace_asset(
            _store(),
            "analytics",
            "events",
            "fixture.format",
            "default.events",
            {"format_option": "safe"},
            plugin_registry=_registry(overlap=False),
        )


def test_asset_binding_rejects_shared_capabilities_without_declared_output_format() -> None:
    registry = _registry()
    catalog = registry.admitted.return_value[("catalog", "fixture.catalog")]
    catalog = PluginDescriptor(
        kind=catalog.kind,
        plugin_id=catalog.plugin_id,
        api_version=catalog.api_version,
        config_version=catalog.config_version,
        distribution=catalog.distribution,
        version=catalog.version,
        capabilities=catalog.capabilities,
        output_formats=frozenset(),
        handle_versions=frozenset({1}),
    )
    registry.admitted.return_value[("catalog", "fixture.catalog")] = catalog
    with pytest.raises(ValidationFailure, match="does not declare"):
        upsert_workspace_asset(
            _store(),
            "analytics",
            "events",
            "fixture.format",
            "default.events",
            {"format_option": "safe"},
            plugin_registry=registry,
        )


@pytest.fixture(autouse=True)
def query_boundary(monkeypatch):
    from tests.support.storage_queries import install_query_doubles

    install_query_doubles(
        monkeypatch,
        [
            "get_workspace",
            "get_workspace_catalog",
            "upsert_asset",
            "get_workspace_asset",
            "record_asset_audit_event",
        ],
    )
