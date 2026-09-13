from __future__ import annotations

import pytest

from dal_obscura.common.plugin_api.contracts import PluginDescriptor
from dal_obscura.control_plane.application.catalog_service import (
    _validate_descriptor_options,
    validate_admitted_catalog_options,
    validate_catalog_options,
)
from dal_obscura.control_plane.application.errors import ValidationFailure


def test_catalog_options_reject_non_finite_numbers() -> None:
    with pytest.raises(ValidationFailure, match="numbers must be finite"):
        validate_catalog_options({"properties": {"timeout": float("nan")}})


def test_admitted_descriptor_rejects_unknown_and_missing_form_fields() -> None:
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="example.catalog",
        api_version="1",
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
        _validate_descriptor_options(descriptor, {"uri": "https://example", "debug": True})
    with pytest.raises(ValidationFailure, match="missing required fields"):
        _validate_descriptor_options(descriptor, {})


def test_admitted_catalog_options_are_checked_before_factory_use() -> None:
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="example.catalog",
        api_version="1",
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
