"""Plugin binding validates admission before publishing a persisted asset."""

from dataclasses import replace
from uuid import UUID

import pytest
from dal_obscura_plugin_api import PluginDescriptor

from dal_obscura.adapters.providers.governance import ProviderAssetSchemaLoader
from dal_obscura.application.governance.assets.service import AssetsCommands
from dal_obscura.application.governance.errors import ValidationFailure


@pytest.mark.parametrize(
    "catalog_id,format_id",
    [("fixture.catalog", "fixture.format"), ("iceberg.catalog", "iceberg")],
)
def test_asset_binding_requires_admission_for_every_pair(asset_commands, catalog_id, format_id):
    from dal_obscura.plugins import PluginRegistry

    _, factory, actor = asset_commands
    with factory() as work:
        work.catalogs.upsert_catalog(name="plugins", plugin_id=catalog_id, options={})
        work.commit()
    commands = AssetsCommands(
        factory,
        schema_loader=ProviderAssetSchemaLoader(),
        plugin_registry=PluginRegistry(allowlist={}),
    )
    with pytest.raises(ValidationFailure, match="Plugin pair is not admitted"):
        commands.upsert_workspace_asset(
            "plugins",
            "events",
            format_id,
            "default.events",
            {},
            actor=actor,
            correlation_id=None,
        )
    assert commands.list_workspace_assets(actor=actor, correlation_id=None) == []


def _commands(asset_commands, *, overlap=True, declared_format=True, handle_overlap=True):
    _, factory, actor = asset_commands
    catalog = PluginDescriptor(
        kind="catalog",
        plugin_id="fixture.catalog",
        api_version="3",
        config_version=1,
        distribution="fixture",
        version="1.0.0",
        capabilities=frozenset({"nested_schema"} if overlap else {"snapshot_reads"}),
        output_formats=frozenset({"fixture.format"}),
        handle_versions=frozenset({1}),
    )
    if not declared_format:
        catalog = replace(catalog, output_formats=frozenset())
    format_descriptor = PluginDescriptor(
        kind="table_format",
        plugin_id="fixture.format",
        api_version="3",
        config_version=1,
        distribution="fixture",
        version="1.0.0",
        capabilities=frozenset({"nested_schema"}),
        handle_versions=frozenset({1} if handle_overlap else {2}),
        config_schema={"fields": [{"name": "format_option", "type": "string", "required": True}]},
    )

    class Registry:
        def admitted(self):
            return {
                ("catalog", "fixture.catalog"): catalog,
                ("table_format", "fixture.format"): format_descriptor,
            }

    with factory() as work:
        work.catalogs.upsert_catalog(name="plugins", plugin_id="fixture.catalog", options={})
        work.commit()
    return AssetsCommands(
        factory, schema_loader=ProviderAssetSchemaLoader(), plugin_registry=Registry()
    ), actor


def test_asset_binding_validates_pair_capabilities_and_format_options(asset_commands):
    commands, actor = _commands(asset_commands)
    for options, message in [
        ({}, "missing required"),
        ({"format_option": 42}, "must be a string"),
        (
            {"format_option": {"secret": "FORMAT_OPTION", "scope": "catalog:plugins"}},
            "cannot contain secrets",
        ),
    ]:
        with pytest.raises(ValidationFailure, match=message):
            commands.upsert_workspace_asset(
                "plugins",
                "events",
                "fixture.format",
                "default.events",
                options,
                actor=actor,
                correlation_id=None,
            )
        assert commands.list_workspace_assets(actor=actor, correlation_id=None) == []
    result = commands.upsert_workspace_asset(
        "plugins",
        "events",
        "fixture.format",
        "default.events",
        {"format_option": "safe"},
        actor=actor,
        correlation_id=None,
    )
    persisted = commands.get_workspace_asset(UUID(result["id"]), actor=actor, correlation_id=None)
    assert persisted["catalog"] == "plugins"
    assert persisted["backend"] == "fixture.format"
    assert persisted["table_identifier"] == "default.events"
    assert persisted["options"] == {"format_option": "safe"}


@pytest.mark.parametrize(
    "overlap,declared_format,handle_overlap,message",
    [
        (False, True, True, "capabilities do not overlap"),
        (True, False, True, "does not declare"),
        (True, True, False, "handle versions do not overlap"),
    ],
)
def test_asset_binding_rejects_incompatible_plugin_pair_without_publishing(
    asset_commands, overlap, declared_format, handle_overlap, message
):
    commands, actor = _commands(
        asset_commands,
        overlap=overlap,
        declared_format=declared_format,
        handle_overlap=handle_overlap,
    )
    with pytest.raises(ValidationFailure, match=message):
        commands.upsert_workspace_asset(
            "plugins",
            "events",
            "fixture.format",
            "default.events",
            {"format_option": "safe"},
            actor=actor,
            correlation_id=None,
        )
    assert commands.list_workspace_assets(actor=actor, correlation_id=None) == []
