from __future__ import annotations

from importlib import metadata
from types import SimpleNamespace
from typing import Any, Protocol, cast

import pytest

from dal_obscura.common.plugin_api import (
    PluginAdmissionError,
    PluginDescriptor,
    PluginRegistry,
    build_plugin_lock,
    load_static_plugin_descriptor,
)
from dal_obscura.common.plugin_api.registry import _artifact_digest, _descriptor_digest


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


def test_builtin_registration_is_admitted_without_entry_point_import() -> None:
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="iceberg.sql",
        api_version="1",
        config_version=1,
        distribution="dal-obscura",
        version="0.1.0",
    )
    registry = PluginRegistry(
        entry_points_fn=lambda: _EntryPoints([]),
        builtins={("catalog", "iceberg.sql"): (descriptor, {"builtin": True})},
    )

    admitted = registry.reload()

    assert admitted[("catalog", "iceberg.sql")] == descriptor
    assert registry.load("catalog", "iceberg.sql") == {"builtin": True}


def test_descriptor_loader_mismatch_fails_before_factory_import() -> None:
    entry = _entry("iceberg.sql", "dal_obscura.catalogs.v1")
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): ("plugin-wheel", "1.2.3", "1")},
        entry_points_fn=lambda: _EntryPoints([entry]),
        descriptor_loader=lambda _: PluginDescriptor(
            kind="catalog",
            plugin_id="iceberg.sql",
            api_version="2",
            config_version=1,
            distribution="plugin-wheel",
            version="1.2.3",
        ),
        factory_loader=lambda _: pytest.fail("mismatched descriptor must not import"),
    )

    with pytest.raises(PluginAdmissionError, match="descriptor mismatch"):
        registry.reload()


def test_malformed_plugin_lock_is_rejected() -> None:
    entry = _entry("iceberg.sql", "dal_obscura.catalogs.v1")
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): ("plugin-wheel", "1.2.3")},  # type: ignore[dict-item]
        entry_points_fn=lambda: _EntryPoints([entry]),
    )

    with pytest.raises(PluginAdmissionError, match="Invalid plugin lock"):
        registry.reload()


def test_extended_lock_accepts_matching_descriptor_and_distribution_digest(tmp_path) -> None:
    artifact = tmp_path / "plugin.py"
    artifact.write_text("trusted = True\n")
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="iceberg.sql",
        api_version="1",
        config_version=1,
        distribution="plugin-wheel",
        version="1.2.3",
    )
    entry = cast(
        _Entry,
        SimpleNamespace(
            name="iceberg.sql",
            group="dal_obscura.catalogs.v1",
            dist=SimpleNamespace(
                name="plugin-wheel",
                version="1.2.3",
                files=["plugin.py"],
                locate_file=lambda _: artifact,
                read_text=lambda _: (
                    '{"kind":"catalog","plugin_id":"iceberg.sql",'
                    '"api_version":"1","config_version":1}'
                ),
            ),
            load=lambda: {"name": "iceberg.sql"},
        ),
    )
    registry = PluginRegistry(
        allowlist={
            ("catalog", "iceberg.sql"): (
                "plugin-wheel",
                "1.2.3",
                "1",
                _descriptor_digest(descriptor),
                _artifact_digest(cast(metadata.EntryPoint, entry)),
            )
        },
        entry_points_fn=lambda: _EntryPoints([entry]),
    )

    assert registry.reload()[("catalog", "iceberg.sql")] == descriptor


def test_build_plugin_lock_derives_the_exact_verified_five_part_lock(tmp_path) -> None:
    artifact = tmp_path / "plugin.py"
    artifact.write_text("trusted = True\n")
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="iceberg.sql",
        api_version="1",
        config_version=1,
        distribution="plugin-wheel",
        version="1.2.3",
    )
    entry = cast(
        metadata.EntryPoint,
        SimpleNamespace(
            name="iceberg.sql",
            group="dal_obscura.catalogs.v1",
            dist=SimpleNamespace(
                name="plugin-wheel",
                version="1.2.3",
                files=["plugin.py"],
                locate_file=lambda _: artifact,
            ),
        ),
    )

    lock = build_plugin_lock("catalog", entry, descriptor)

    assert lock == (
        "plugin-wheel",
        "1.2.3",
        "1",
        _descriptor_digest(descriptor),
        _artifact_digest(entry),
    )


def test_static_descriptor_loader_reads_metadata_without_factory_import() -> None:
    descriptor_json = (
        '{"kind":"catalog","plugin_id":"rest.catalog","api_version":"1",'
        '"config_version":1,"capabilities":["nested"],'
        '"config_schema":{"fields":[]},"display_name":"REST Catalog"}'
    )
    distribution = SimpleNamespace(
        name="rest-wheel",
        version="2.0.0",
        read_text=lambda filename: descriptor_json
        if filename == "dal_obscura-plugin.json"
        else None,
    )
    entry = cast(
        _Entry,
        SimpleNamespace(
            name="rest.catalog",
            group="dal_obscura.catalogs.v1",
            dist=distribution,
        ),
    )

    descriptor = load_static_plugin_descriptor(cast(metadata.EntryPoint, entry))

    assert descriptor.kind == "catalog"
    assert descriptor.plugin_id == "rest.catalog"
    assert descriptor.distribution == "rest-wheel"
    assert descriptor.version == "2.0.0"
    assert descriptor.capabilities == frozenset({"nested"})


def test_static_descriptor_loader_rejects_identity_mismatch() -> None:
    distribution = SimpleNamespace(
        name="rest-wheel",
        version="2.0.0",
        read_text=lambda _: (
            '{"kind":"catalog","plugin_id":"other","api_version":"1",'
            '"config_version":1}'
        ),
    )
    entry = cast(
        _Entry,
        SimpleNamespace(
            name="rest.catalog",
            group="dal_obscura.catalogs.v1",
            dist=distribution,
        ),
    )

    with pytest.raises(PluginAdmissionError, match="identity mismatch"):
        load_static_plugin_descriptor(cast(metadata.EntryPoint, entry))


def test_static_descriptor_loader_rejects_unreadable_metadata() -> None:
    distribution = SimpleNamespace(
        name="rest-wheel",
        version="2.0.0",
        read_text=lambda _: (_ for _ in ()).throw(OSError("missing")),
    )
    entry = cast(
        _Entry,
        SimpleNamespace(
            name="rest.catalog",
            group="dal_obscura.catalogs.v1",
            dist=distribution,
        ),
    )

    with pytest.raises(PluginAdmissionError, match="unreadable"):
        load_static_plugin_descriptor(cast(metadata.EntryPoint, entry))
