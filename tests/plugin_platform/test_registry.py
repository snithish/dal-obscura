from __future__ import annotations

from types import SimpleNamespace

import pytest

from dal_obscura.common.plugin_api import PluginAdmissionError, PluginRegistry


class _EntryPoints:
    def __init__(self, entries: list[object]) -> None:
        self.entries = entries

    def select(self, *, group: str) -> list[object]:
        return [entry for entry in self.entries if entry.group == group]


def _entry(name: str, group: str, distribution: str = "plugin-wheel", version: str = "1.2.3"):
    loaded = {"name": name}
    return SimpleNamespace(
        name=name,
        group=group,
        dist=SimpleNamespace(name=distribution, version=version),
        load=lambda: loaded,
    )


def test_discovery_does_not_import_unapproved_entry_points() -> None:
    registry = PluginRegistry(
        allowlist={},
        entry_points_fn=lambda: _EntryPoints(
            [_entry("unapproved", "dal_obscura.catalogs.v1")]
        ),
        factory_loader=lambda _: pytest.fail("unapproved factory must not load"),
    )

    assert registry.discover() == {}


def test_admitted_entry_point_loads_only_after_lock_match() -> None:
    entry = _entry("iceberg.sql", "dal_obscura.catalogs.v1")
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): ("plugin-wheel", "1.2.3", "1")},
        entry_points_fn=lambda: _EntryPoints([entry]),
    )

    discovered = registry.discover()

    assert discovered[("catalog", "iceberg.sql")].api_version == "1"
    assert registry.load("catalog", "iceberg.sql") == {"name": "iceberg.sql"}


def test_unapproved_entry_point_cannot_be_loaded_on_request() -> None:
    registry = PluginRegistry(
        allowlist={},
        entry_points_fn=lambda: _EntryPoints(
            [_entry("unapproved", "dal_obscura.catalogs.v1")]
        ),
        factory_loader=lambda _: pytest.fail("unapproved factory must not load"),
    )

    with pytest.raises(PluginAdmissionError, match="not admitted"):
        registry.load("catalog", "unapproved")


def test_lock_mismatch_and_duplicate_ids_fail_closed() -> None:
    mismatched = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): ("plugin-wheel", "9.9.9", "1")},
        entry_points_fn=lambda: _EntryPoints(
            [_entry("iceberg.sql", "dal_obscura.catalogs.v1")]
        ),
    )
    with pytest.raises(PluginAdmissionError, match="lock mismatch"):
        mismatched.discover()

    duplicate = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): ("plugin-wheel", "1.2.3", "1")},
        entry_points_fn=lambda: _EntryPoints(
            [
                _entry("iceberg.sql", "dal_obscura.catalogs.v1"),
                _entry("iceberg.sql", "dal_obscura.catalogs.v1"),
            ]
        ),
    )
    with pytest.raises(PluginAdmissionError, match="Duplicate plugin ID"):
        duplicate.discover()


def test_invalid_plugin_id_cannot_be_loaded() -> None:
    registry = PluginRegistry(entry_points_fn=lambda: _EntryPoints([]))

    with pytest.raises(PluginAdmissionError, match="Invalid plugin ID"):
        registry.load("catalog", "../../import-anything")
