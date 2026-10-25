"""Plugin authors load runtime metadata from their packaged descriptor."""

import json
import sys

import pytest
from dal_obscura_plugin_api import PluginDescriptor
from dal_obscura_plugin_api.descriptors import load_packaged_descriptor


@pytest.fixture
def descriptor_package(tmp_path, monkeypatch):
    package = tmp_path / "descriptor_fixture"
    package.mkdir()
    (package / "__init__.py").write_text("")
    info = tmp_path / "descriptor_fixture-2.1.dist-info"
    info.mkdir()
    (info / "METADATA").write_text(
        "Metadata-Version: 2.1\nName: descriptor-fixture\nVersion: 2.1\n"
    )
    monkeypatch.syspath_prepend(tmp_path)
    try:
        yield package / "dal_obscura-plugin.json"
    finally:
        sys.modules.pop("descriptor_fixture", None)


def test_packaged_descriptor_uses_installed_provenance_and_static_configuration(descriptor_package):
    descriptor_package.write_text(
        json.dumps(
            {
                "descriptors": [
                    {
                        "kind": "table_format",
                        "plugin_id": "fixture",
                        "api_version": "3",
                        "config_version": 1,
                        "display_name": "Fixture reader",
                        "config_schema": {"fields": [{"name": "batch_rows", "type": "integer"}]},
                    }
                ],
            }
        )
    )
    descriptor = load_packaged_descriptor(
        "descriptor-fixture", "descriptor_fixture", "table_format", "fixture"
    )
    assert (descriptor.distribution, descriptor.version, descriptor.display_name) == (
        "descriptor-fixture",
        "2.1",
        "Fixture reader",
    )
    assert descriptor.to_json()["config_schema"] == {
        "fields": [{"name": "batch_rows", "type": "integer"}]
    }


def test_packaged_descriptor_rejects_oversized_metadata(descriptor_package):
    descriptor_package.write_bytes(b" " * 65_537)
    with pytest.raises(ValueError, match="too large"):
        load_packaged_descriptor(
            "descriptor-fixture", "descriptor_fixture", "table_format", "fixture"
        )


@pytest.mark.parametrize(
    ("schema", "message"),
    [
        ({"fields": [{"name": "rows", "type": "intrger"}]}, "field type"),
        ({"fields": [{"name": "rows"}, {"name": "rows"}]}, "duplicate"),
        ({"fields": [{"name": "rows", "required": "yes"}]}, "boolean"),
        ({"fields": [{"name": "password", "type": "string", "secret": True}]}, "secret_reference"),
        ({"fields": [{"name": "backend", "type": "enum", "options": []}]}, "choices"),
        ({"fields": ["rows"]}, "field"),
        ({"fields": [{"name": "bad field"}]}, "name"),
        (
            {"fields": [{"name": "rows", "type": "integer"}], "defaults": {"rows": "1000"}},
            "integer",
        ),
        ({"fields": [], "defaults": {"unknown": "value"}}, "default"),
    ],
)
def test_descriptor_rejects_invalid_configuration_language_before_admission(schema, message):
    with pytest.raises(ValueError, match=message):
        PluginDescriptor("catalog", "fixture", "3", 1, "fixture", "1", config_schema=schema)
