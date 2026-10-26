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
        ({"fields": [], "additionalProperties": "true"}, "additionalProperties"),
    ],
)
def test_descriptor_rejects_invalid_configuration_language_before_admission(schema, message):
    with pytest.raises(ValueError, match=message):
        PluginDescriptor("catalog", "fixture", "3", 1, "fixture", "1", config_schema=schema)


@pytest.mark.parametrize("field_type", ["string", "uri"])
def test_text_fields_accept_strings_or_scoped_references(field_type):
    descriptor = PluginDescriptor(
        "catalog",
        "fixture",
        "3",
        1,
        "fixture",
        "1",
        config_schema={"fields": [{"name": "uri", "type": field_type}]},
    )
    assert descriptor.config_fields is not None
    field = descriptor.config_fields[0]
    field.validate("https://catalog.example")
    field.validate({"secret": "CATALOG_URI", "scope": "catalog:analytics"})
    for malformed in [
        {"secret": "CATALOG_URI"},
        {"secret": "CATALOG_URI", "scope": ""},
        {"secret": "CATALOG_URI", "scope": "catalog:analytics", "value": "inline"},
        {"secret": "", "scope": "catalog:analytics"},
    ]:
        with pytest.raises(ValueError, match="secret"):
            field.validate(malformed)
    with pytest.raises(ValueError, match="string"):
        field.validate(42)


@pytest.mark.parametrize(
    "field", [{"type": "integer"}, {"type": "enum", "options": ["safe", "fast"]}]
)
def test_non_text_fields_do_not_accept_secret_references(field):
    descriptor = PluginDescriptor(
        "catalog",
        "fixture",
        "3",
        1,
        "fixture",
        "1",
        config_schema={"fields": [{"name": "mode", **field}]},
    )
    assert descriptor.config_fields is not None
    with pytest.raises(ValueError):
        descriptor.config_fields[0].validate({"secret": "MODE", "scope": "catalog:analytics"})


@pytest.mark.parametrize(
    "value",
    [
        "https://catalog.example/data?revision=7",
        "abfs://warehouse@account.dfs.core.windows.net/data",
        "abfss://warehouse@account.dfs.core.windows.net/data",
    ],
)
def test_uri_preserves_non_secret_locations(value):
    from dal_obscura_plugin_api.configuration import parse_uri

    assert parse_uri(value).geturl() == value


@pytest.mark.parametrize(
    "value,reason",
    [
        ("https://[2001:db8::1/sensitive", "contains an invalid URI"),
        ("https://catalog.example:sensitive/data", "contains an invalid URI"),
        ("https://sensitive@catalog.example/data", "secret reference"),
        ("https://sensitive:@catalog.example/data", "secret reference"),
        ("abfss://warehouse:sensitive@account.dfs.core.windows.net/data", "secret reference"),
        ("https://catalog.example/data?ToKeN=sensitive", "URI query"),
        ("https://catalog.example/data?%61pi_key=sensitive", "URI query"),
    ],
)
def test_uri_errors_hide_credentials_and_parser_details(value, reason):
    from dal_obscura_plugin_api.configuration import parse_uri

    with pytest.raises(ValueError, match=reason) as caught:
        parse_uri(value)
    assert "sensitive" not in str(caught.value)
    assert caught.value.__suppress_context__ or caught.value.__context__ is None


def test_uri_allows_explicit_credentials_without_relaxing_structure():
    from dal_obscura_plugin_api.configuration import parse_uri

    value = "https://reader:sensitive@catalog.example/data?token=sensitive"
    assert parse_uri(value, allow_credentials=True).geturl() == value
    with pytest.raises(ValueError, match="contains an invalid URI") as caught:
        parse_uri("https://catalog.example:sensitive/data", allow_credentials=True)
    assert "sensitive" not in str(caught.value)
