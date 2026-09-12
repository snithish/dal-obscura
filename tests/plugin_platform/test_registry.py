from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Protocol, cast

import pytest

from dal_obscura.common.plugin_api import PluginAdmissionError, PluginRegistry


class _Entry(Protocol):
    group: str


class _EntryPoints:
    def __init__(self, entries: list[_Entry]) -> None:
        self.entries = entries

    def select(self, *, group: str) -> list[object]:
        return [entry for entry in self.entries if entry.group == group]


def _entry(name: str, group: str, distribution: str = "plugin-wheel", version: str = "1.2.3"):
    loaded = {"name": name}
    return cast(_Entry, SimpleNamespace(
        name=name,
        group=group,
        dist=SimpleNamespace(name=distribution, version=version),
        load=lambda: loaded,
    ))


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


def test_allowlisted_entry_without_distribution_provenance_fails_closed() -> None:
    entry = cast(
        _Entry,
        SimpleNamespace(
            name="iceberg.sql",
            group="dal_obscura.catalogs.v1",
            dist=None,
            load=lambda: {"name": "iceberg.sql"},
        ),
    )
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): ("plugin-wheel", "1.2.3", "1")},
        entry_points_fn=lambda: _EntryPoints([entry]),
    )

    with pytest.raises(PluginAdmissionError, match="provenance is unavailable"):
        registry.discover()


def test_invalid_plugin_id_cannot_be_loaded() -> None:
    registry = PluginRegistry(entry_points_fn=lambda: _EntryPoints([]))

    with pytest.raises(PluginAdmissionError, match="Invalid plugin ID"):
        registry.load("catalog", "../../import-anything")


def test_failed_reload_keeps_last_valid_admission_snapshot() -> None:
    entries = [_entry("iceberg.sql", "dal_obscura.catalogs.v1")]
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): ("plugin-wheel", "1.2.3", "1")},
        entry_points_fn=lambda: _EntryPoints(entries),
    )

    initial = registry.reload()
    assert set(registry.admitted()) == {("catalog", "iceberg.sql")}

    entries[:] = [_entry("iceberg.sql", "dal_obscura.catalogs.v1", version="9.9.9")]
    with pytest.raises(PluginAdmissionError, match="lock mismatch"):
        registry.reload()

    assert registry.admitted() == initial


def test_load_uses_entry_point_captured_by_admitted_generation() -> None:
    first = _entry("iceberg.sql", "dal_obscura.catalogs.v1")
    entries = [first]
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): ("plugin-wheel", "1.2.3", "1")},
        entry_points_fn=lambda: _EntryPoints(entries),
        factory_loader=lambda entry: entry.load(),
    )

    registry.reload()
    replacement = _entry("iceberg.sql", "dal_obscura.catalogs.v1")
    cast(Any, replacement).load = lambda: {"name": "replacement"}
    entries[:] = [replacement]

    assert registry.load("catalog", "iceberg.sql") == {"name": "iceberg.sql"}
