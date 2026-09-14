from __future__ import annotations

import json
from importlib import metadata
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Protocol, cast

import pytest
from dal_obscura_plugin_api import PluginDescriptor

from dal_obscura.common.plugin_api import (
    PluginAdmissionError,
    PluginRegistry,
    build_plugin_lock,
    load_static_plugin_descriptor,
)
from dal_obscura.common.plugin_api.lifecycle import PluginLifecycleError, PluginLifecycleState
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
    kind = "catalog" if group.endswith("catalogs.v1") else "table_format"
    descriptor_json = json.dumps(
        {
            "kind": kind,
            "plugin_id": name,
            "api_version": "1",
            "config_version": 1,
        }
    )
    return cast(
        _Entry,
        SimpleNamespace(
            name=name,
            group=group,
            dist=SimpleNamespace(
                name=distribution,
                version=version,
                files=[],
                read_text=lambda filename: (
                    descriptor_json if filename == "dal_obscura-plugin.json" else None
                ),
            ),
            value="fixture.module:factory",
            load=lambda: loaded,
        ),
    )


def _lock(
    name: str,
    *,
    group: str = "dal_obscura.catalogs.v1",
    distribution: str = "plugin-wheel",
    version: str = "1.2.3",
    api_version: str = "1",
) -> tuple[str, str, str, str, str]:
    entry = _entry(name, group, distribution, version)
    descriptor = PluginDescriptor(
        kind="catalog" if group.endswith("catalogs.v1") else "table_format",
        plugin_id=name,
        api_version=api_version,
        config_version=1,
        distribution=distribution,
        version=version,
    )
    return (
        distribution,
        version,
        api_version,
        _descriptor_digest(descriptor),
        _artifact_digest(cast(metadata.EntryPoint, entry)),
    )


def test_discovery_does_not_import_unapproved_entry_points() -> None:
    registry = PluginRegistry(
        allowlist={},
        entry_points_fn=lambda: _EntryPoints([_entry("unapproved", "dal_obscura.catalogs.v1")]),
        factory_loader=lambda _: pytest.fail("unapproved factory must not load"),
    )

    assert registry.discover() == {}


def test_discovery_ignores_malformed_unallowlisted_entry_point() -> None:
    registry = PluginRegistry(
        allowlist={},
        entry_points_fn=lambda: _EntryPoints(
            [cast(_Entry, SimpleNamespace(name="../../escape", group="dal_obscura.catalogs.v1"))]
        ),
    )

    assert registry.discover() == {}


def test_admitted_entry_point_loads_only_after_lock_match() -> None:
    entry = _entry("iceberg.sql", "dal_obscura.catalogs.v1")
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): _lock("iceberg.sql")},
        entry_points_fn=lambda: _EntryPoints([entry]),
    )

    discovered = registry.discover()

    assert discovered[("catalog", "iceberg.sql")].api_version == "1"
    assert registry.load("catalog", "iceberg.sql") == {"name": "iceberg.sql"}


def test_unapproved_entry_point_cannot_be_loaded_on_request() -> None:
    registry = PluginRegistry(
        allowlist={},
        entry_points_fn=lambda: _EntryPoints([_entry("unapproved", "dal_obscura.catalogs.v1")]),
        factory_loader=lambda _: pytest.fail("unapproved factory must not load"),
    )

    with pytest.raises(PluginAdmissionError, match="not admitted"):
        registry.load("catalog", "unapproved")


def test_lock_mismatch_and_duplicate_ids_fail_closed() -> None:
    mismatched = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): _lock("iceberg.sql", version="9.9.9")},
        entry_points_fn=lambda: _EntryPoints([_entry("iceberg.sql", "dal_obscura.catalogs.v1")]),
    )
    with pytest.raises(PluginAdmissionError, match="lock mismatch"):
        mismatched.discover()

    duplicate = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): _lock("iceberg.sql")},
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
        allowlist={("catalog", "iceberg.sql"): _lock("iceberg.sql")},
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
        allowlist={("catalog", "iceberg.sql"): _lock("iceberg.sql")},
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
        allowlist={("catalog", "iceberg.sql"): _lock("iceberg.sql")},
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


def test_status_report_distinguishes_enabled_missing_and_incompatible_without_import() -> None:
    enabled = _entry("enabled", "dal_obscura.catalogs.v1")
    entries = [enabled]
    registry = PluginRegistry(
        allowlist={
            ("catalog", "enabled"): _lock("enabled"),
            ("catalog", "missing"): _lock("missing", distribution="missing-wheel", version="1.0.0"),
            ("catalog", "wrong"): _lock("wrong", version="9.9.9"),
        },
        entry_points_fn=lambda: _EntryPoints(entries),
        factory_loader=lambda _: pytest.fail("status reporting must not import factories"),
    )

    registry.reload()
    entries.append(_entry("wrong", "dal_obscura.catalogs.v1"))
    statuses = {row["plugin_id"]: row["status"] for row in registry.status_report()}

    assert statuses == {
        "enabled": "enabled",
        "missing": "not_installed",
        "wrong": "incompatible",
    }


def test_plugin_lifecycle_draining_blocks_new_admissions_and_is_reported() -> None:
    entry = _entry("drainable", "dal_obscura.catalogs.v1")
    registry = PluginRegistry(
        allowlist={("catalog", "drainable"): _lock("drainable")},
        entry_points_fn=lambda: _EntryPoints([entry]),
        factory_loader=lambda _: {"loaded": True},
    )
    registry.reload()

    assert (
        registry.set_lifecycle("catalog", "drainable", PluginLifecycleState.DRAINING)
        is PluginLifecycleState.DRAINING
    )
    with pytest.raises(PluginAdmissionError, match="draining"):
        registry.load("catalog", "drainable")
    assert registry.status_report() == (
        {
            "kind": "catalog",
            "plugin_id": "drainable",
            "status": "enabled",
            "lifecycle": "draining",
        },
    )
    assert (
        registry.set_lifecycle("catalog", "drainable", PluginLifecycleState.REVOKED)
        is PluginLifecycleState.REVOKED
    )
    with pytest.raises(PluginLifecycleError):
        registry.set_lifecycle("catalog", "drainable", PluginLifecycleState.ENABLED)


def test_revoked_plugin_can_only_be_removed() -> None:
    registry = PluginRegistry(
        allowlist={("catalog", "revoked"): _lock("revoked")},
        entry_points_fn=lambda: _EntryPoints([]),
    )
    registry.set_lifecycle("catalog", "revoked", PluginLifecycleState.REVOKED)
    assert (
        registry.set_lifecycle("catalog", "revoked", PluginLifecycleState.REMOVED)
        is PluginLifecycleState.REMOVED
    )
    with pytest.raises(PluginAdmissionError, match="removed"):
        registry.load("catalog", "revoked")


def test_descriptor_loader_mismatch_fails_before_factory_import() -> None:
    entry = _entry("iceberg.sql", "dal_obscura.catalogs.v1")
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): _lock("iceberg.sql")},
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


def test_registry_rejects_self_consistent_unsupported_api_version() -> None:
    entry = _entry("iceberg.sql", "dal_obscura.catalogs.v1")
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): _lock("iceberg.sql", api_version="2")},
        entry_points_fn=lambda: _EntryPoints([entry]),
        descriptor_loader=lambda _: PluginDescriptor(
            kind="catalog",
            plugin_id="iceberg.sql",
            api_version="2",
            config_version=1,
            distribution="plugin-wheel",
            version="1.2.3",
        ),
        factory_loader=lambda _: pytest.fail("unsupported API must not import"),
    )

    with pytest.raises(PluginAdmissionError, match="unsupported API version"):
        registry.reload()


def test_registry_rejects_self_consistent_unsupported_config_version() -> None:
    entry = _entry("iceberg.sql", "dal_obscura.catalogs.v1")
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.sql"): _lock("iceberg.sql")},
        entry_points_fn=lambda: _EntryPoints([entry]),
        descriptor_loader=lambda _: PluginDescriptor(
            kind="catalog",
            plugin_id="iceberg.sql",
            api_version="1",
            config_version=2,
            distribution="plugin-wheel",
            version="1.2.3",
        ),
        factory_loader=lambda _: pytest.fail("unsupported config must not import"),
    )

    with pytest.raises(PluginAdmissionError, match="unsupported config version"):
        registry.reload()


def test_malformed_plugin_lock_is_rejected() -> None:
    entry = _entry("iceberg.sql", "dal_obscura.catalogs.v1")
    registry = PluginRegistry(
        allowlist=cast(Any, {("catalog", "iceberg.sql"): ("plugin-wheel", "1.2.3", "1")}),
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
        '"config_version":1,"capabilities":["nested_schema"],'
        '"config_schema":{"fields":[]},"display_name":"REST Catalog"}'
    )
    distribution = SimpleNamespace(
        name="rest-wheel",
        version="2.0.0",
        read_text=lambda filename: (
            descriptor_json if filename == "dal_obscura-plugin.json" else None
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

    descriptor = load_static_plugin_descriptor(cast(metadata.EntryPoint, entry))

    assert descriptor.kind == "catalog"
    assert descriptor.plugin_id == "rest.catalog"
    assert descriptor.distribution == "rest-wheel"
    assert descriptor.version == "2.0.0"
    assert descriptor.capabilities == frozenset({"nested_schema"})


def test_static_descriptor_loader_reads_setuptools_package_data_without_importing_factory() -> None:
    descriptor_json = (
        '{"kind":"catalog","plugin_id":"rest.catalog","api_version":"1",'
        '"config_version":1,"capabilities":[],"config_schema":{"fields":[]},'
        '"display_name":"REST Catalog"}'
    )
    requested: list[str] = []

    def read_text(filename: str) -> str | None:
        requested.append(filename)
        return descriptor_json if filename == "rest_pkg/dal_obscura-plugin.json" else None

    distribution = SimpleNamespace(name="rest-wheel", version="2.0.0", read_text=read_text)
    entry = cast(
        _Entry,
        SimpleNamespace(
            name="rest.catalog",
            group="dal_obscura.catalogs.v1",
            value="rest_pkg.catalog:factory",
            dist=distribution,
        ),
    )

    descriptor = load_static_plugin_descriptor(cast(metadata.EntryPoint, entry))

    assert descriptor.plugin_id == "rest.catalog"
    assert requested == ["dal_obscura-plugin.json", "rest_pkg/dal_obscura-plugin.json"]


def test_static_descriptor_loader_reads_nested_path_distribution_wheels(tmp_path: Path) -> None:
    descriptor_json = (
        '{"kind":"catalog","plugin_id":"rest.catalog","api_version":"1",'
        '"config_version":1,"capabilities":[],"config_schema":{"fields":[]},'
        '"display_name":"REST Catalog"}'
    )
    descriptor_path = tmp_path / "rest_pkg" / "dal_obscura-plugin.json"
    descriptor_path.parent.mkdir()
    descriptor_path.write_text(descriptor_json, encoding="utf-8")

    distribution = SimpleNamespace(
        name="rest-wheel",
        version="2.0.0",
        files=["rest_pkg/dal_obscura-plugin.json"],
        read_text=lambda _: None,
        locate_file=lambda path: tmp_path / path,
    )
    entry = cast(
        _Entry,
        SimpleNamespace(
            name="rest.catalog",
            group="dal_obscura.catalogs.v1",
            value="rest_pkg.catalog:factory",
            dist=distribution,
        ),
    )

    descriptor = load_static_plugin_descriptor(cast(metadata.EntryPoint, entry))

    assert descriptor.plugin_id == "rest.catalog"


def test_static_descriptor_loader_selects_matching_descriptor_from_multi_kind_wheel() -> None:
    descriptor_json = (
        '{"descriptors":['
        '{"kind":"catalog","plugin_id":"manifest","api_version":"1",'
        '"config_version":1,"display_name":"Manifest"},'
        '{"kind":"table_format","plugin_id":"parquet.dataset",'
        '"api_version":"1","config_version":1,"display_name":"Parquet"}'
        "]}"
    )
    distribution = SimpleNamespace(
        name="manifest-wheel",
        version="1.0.0",
        read_text=lambda _: descriptor_json,
    )
    entry = cast(
        _Entry,
        SimpleNamespace(
            name="parquet.dataset",
            group="dal_obscura.table_formats.v1",
            dist=distribution,
        ),
    )

    descriptor = load_static_plugin_descriptor(cast(metadata.EntryPoint, entry))

    assert descriptor.kind == "table_format"
    assert descriptor.plugin_id == "parquet.dataset"
    assert descriptor.display_name == "Parquet"


def test_static_descriptor_loader_rejects_identity_mismatch() -> None:
    distribution = SimpleNamespace(
        name="rest-wheel",
        version="2.0.0",
        read_text=lambda _: (
            '{"kind":"catalog","plugin_id":"other","api_version":"1","config_version":1}'
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
