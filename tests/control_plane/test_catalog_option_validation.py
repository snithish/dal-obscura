from __future__ import annotations

import pytest
from dal_obscura_plugin_api import PluginDescriptor

from dal_obscura.control.catalog_service import (
    validate_admitted_catalog_options,
    validate_catalog_options,
    validate_descriptor_options,
)
from dal_obscura.control.errors import ValidationFailure


def test_catalog_options_reject_non_finite_numbers() -> None:
    with pytest.raises(ValidationFailure, match="numbers must be finite"):
        validate_catalog_options({"properties": {"timeout": float("nan")}})


def test_catalog_options_reject_retired_catalog_name_alias() -> None:
    with pytest.raises(ValidationFailure, match=r"catalog_name.*provider_catalog_name"):
        validate_catalog_options({"catalog_name": "provider-name"})


@pytest.mark.parametrize("host", ["127.0.0.1", "10.0.0.8", "::1"])
def test_catalog_options_reject_unallowlisted_private_literal(host: str) -> None:
    with pytest.raises(ValidationFailure, match="private address"):
        validate_catalog_options(
            {"uri": f"https://[{host}]/v1" if ":" in host else f"https://{host}/v1"}
        )


def test_catalog_options_allow_explicit_private_literal() -> None:
    validate_catalog_options(
        {"uri": "https://127.0.0.1/v1"},
        egress_allowlist=("127.0.0.1",),
    )


def test_catalog_options_reject_remote_file_authority() -> None:
    with pytest.raises(ValidationFailure, match="local file URI"):
        validate_catalog_options({"warehouse": "file://remote-host/warehouse"})


def test_catalog_options_reject_malformed_uri_as_validation_failure() -> None:
    with pytest.raises(ValidationFailure, match="invalid URI"):
        validate_catalog_options({"uri": "https://[2001:db8::1/v1"})


@pytest.mark.parametrize("host", ["169.254.169.254", "0.0.0.0", "224.0.0.1"])
def test_catalog_options_reject_special_literal_even_when_allowlisted(host: str) -> None:
    with pytest.raises(ValidationFailure, match="special address"):
        validate_catalog_options(
            {"uri": f"https://{host}/v1"},
            egress_allowlist=(host,),
        )


def test_admitted_descriptor_rejects_unknown_and_missing_form_fields() -> None:
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="example.catalog",
        api_version="2",
        config_version=1,
        distribution="example",
        version="1.0.0",
        config_schema={
            "fields": [
                {"name": "uri", "required": True},
                {"name": "token", "required": False},
            ]
        },
    )
    with pytest.raises(ValidationFailure, match="unsupported fields"):
        validate_descriptor_options(descriptor, {"uri": "https://example", "debug": True})
    with pytest.raises(ValidationFailure, match="missing required fields"):
        validate_descriptor_options(descriptor, {})


def test_admitted_secret_reference_accepts_bounded_scope() -> None:
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="example.catalog",
        api_version="2",
        config_version=1,
        distribution="example",
        version="1.0.0",
        config_schema={"fields": [{"name": "token", "type": "secret_reference"}]},
    )
    validate_descriptor_options(
        descriptor,
        {"token": {"secret": "catalog-token", "scope": "catalog:analytics"}},
    )
    with pytest.raises(ValidationFailure, match="invalid secret scope"):
        validate_descriptor_options(
            descriptor,
            {"token": {"secret": "catalog-token", "scope": ""}},
        )


def test_admitted_descriptor_preserves_typed_boolean_integer_and_enum_values() -> None:
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="typed.catalog",
        api_version="2",
        config_version=1,
        distribution="example",
        version="1.0.0",
        config_schema={
            "fields": [
                {"name": "enabled", "type": "boolean"},
                {"name": "timeout", "type": "integer"},
                {"name": "ratio", "type": "number"},
                {"name": "mode", "type": "enum", "options": ["safe", "fast"]},
            ]
        },
    )
    validate_descriptor_options(
        descriptor, {"enabled": True, "timeout": 30, "ratio": 0.5, "mode": "safe"}
    )
    with pytest.raises(ValidationFailure, match="must be a boolean"):
        validate_descriptor_options(
            descriptor, {"enabled": "true", "timeout": 30, "ratio": 0.5, "mode": "safe"}
        )
    with pytest.raises(ValidationFailure, match="must be an integer"):
        validate_descriptor_options(
            descriptor, {"enabled": True, "timeout": True, "ratio": 0.5, "mode": "safe"}
        )
    with pytest.raises(ValidationFailure, match="declared choices"):
        validate_descriptor_options(
            descriptor, {"enabled": True, "timeout": 30, "ratio": 0.5, "mode": "unsafe"}
        )
    with pytest.raises(ValidationFailure, match="finite number"):
        validate_descriptor_options(
            descriptor, {"enabled": True, "timeout": 30, "ratio": float("inf"), "mode": "safe"}
        )


def test_admitted_catalog_options_are_checked_before_factory_use() -> None:
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="example.catalog",
        api_version="2",
        config_version=1,
        distribution="example",
        version="1.0.0",
        config_schema={"fields": [{"name": "uri", "required": True}]},
    )
    calls: list[tuple[str, str]] = []

    class Registry:
        def admitted(self):
            return {("catalog", "example.catalog"): descriptor}

        def load(self, kind, plugin_id):
            calls.append((kind, plugin_id))
            raise AssertionError("factory loading must happen after option validation")

    with pytest.raises(ValidationFailure, match="unsupported fields"):
        validate_admitted_catalog_options(
            "example.catalog",
            {"uri": "https://catalog.example", "debug": True},
            Registry(),
        )
    assert calls == []
