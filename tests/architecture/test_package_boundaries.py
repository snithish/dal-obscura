from __future__ import annotations

import importlib
from pathlib import Path

import pytest


def test_common_contract_imports_are_public() -> None:
    modules = [
        "dal_obscura.common.access_control.models",
        "dal_obscura.common.access_control.filters",
        "dal_obscura.common.catalog.ports",
        "dal_obscura.common.query_planning.models",
        "dal_obscura.common.table_format.ports",
        "dal_obscura.common.ticket_delivery.models",
        "dal_obscura.common.config_store.db",
        "dal_obscura.common.config_store.orm",
    ]

    for module in modules:
        importlib.import_module(module)


def test_data_plane_imports_are_public() -> None:
    modules = [
        "dal_obscura.data_plane.application.ports.identity",
        "dal_obscura.data_plane.application.use_cases.plan_access",
        "dal_obscura.data_plane.infrastructure.adapters.published_config",
        "dal_obscura.data_plane.infrastructure.table_formats.iceberg",
        "dal_obscura.data_plane.interfaces.flight.server",
        "dal_obscura.data_plane.interfaces.cli",
    ]

    for module in modules:
        importlib.import_module(module)


def test_legacy_root_packages_are_removed() -> None:
    modules = [
        "dal_obscura.application",
        "dal_obscura.domain",
        "dal_obscura.infrastructure",
        "dal_obscura.interfaces",
    ]

    for module in modules:
        with pytest.raises(ModuleNotFoundError):
            importlib.import_module(module)


def test_console_scripts_target_plane_specific_entry_points() -> None:
    pyproject = Path("pyproject.toml").read_text()

    assert 'dal-obscura = "dal_obscura.data_plane.interfaces.cli:main"' in pyproject
    assert (
        'dal-obscura-control-plane = "dal_obscura.control_plane.interfaces.cli:main"' in pyproject
    )
