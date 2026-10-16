"""Plugin binding validates admission before publishing a persisted asset."""

from dataclasses import replace
from uuid import UUID

import pytest
from dal_obscura_plugin_api import PluginDescriptor

from dal_obscura.adapters.providers.governance import ProviderAssetSchemaLoader
from dal_obscura.application.governance.assets.service import AssetsCommands
from dal_obscura.application.governance.errors import ValidationFailure


def _commands(asset_commands, *, overlap=True, declared_format=True):
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
        handle_versions=frozenset({1}),
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
    for options, message in [({}, "missing required"), ({"format_option": 42}, "must be a string")]:
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
    "overlap,declared_format,message",
    [(False, True, "capabilities do not overlap"), (True, False, "does not declare")],
)
def test_asset_binding_rejects_incompatible_plugin_pair_without_publishing(
    asset_commands, overlap, declared_format, message
):
    commands, actor = _commands(asset_commands, overlap=overlap, declared_format=declared_format)
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
