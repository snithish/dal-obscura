from __future__ import annotations

import json
from dataclasses import replace
from importlib import metadata
from types import SimpleNamespace
from typing import Any, Protocol, cast

import pytest
from dal_obscura_plugin_api import PluginDescriptor, PluginKind

from dal_obscura.plugins import (
    PluginAdmissionError,
    PluginRegistry,
    build_plugin_lock,
    load_static_plugin_descriptor,
)


class _Entry(Protocol):
    group: str


class _EntryPoints:
    def __init__(self, entries: list[_Entry]) -> None:
        self.entries = entries

    def select(self, *, group: str) -> list[object]:
        return [entry for entry in self.entries if entry.group == group]


def test_plugin_discovery_requires_selectable_entry_points() -> None:
    registry = PluginRegistry(entry_points_fn=lambda: cast(Any, {"catalog": []}))

    with pytest.raises(AttributeError, match="select"):
        registry._select("dal_obscura.catalogs.v3")


@pytest.fixture
def entry_factory(tmp_path):
    sequence = 0

    def create(name, group, distribution="plugin-wheel", version="1.2.3"):
        nonlocal sequence
        sequence += 1
        root = tmp_path / str(sequence)
        info = root / "plugin.dist-info"
        info.mkdir(parents=True)
        (info / "METADATA").write_text(
            f"Metadata-Version: 2.1\nName: {distribution}\nVersion: {version}\n"
        )
        kind = "catalog" if group.endswith("catalogs.v3") else "table_format"
        (info / "dal_obscura-plugin.json").write_text(
            json.dumps(
                {
                    "kind": kind,
                    "plugin_id": name,
                    "api_version": "3",
                    "config_version": 1,
                }
            )
        )
        (info / "RECORD").write_text(
            "plugin.dist-info/METADATA,,\nplugin.dist-info/dal_obscura-plugin.json,,\n"
        )
        return SimpleNamespace(
            name=name,
            group=group,
            dist=metadata.Distribution.at(info),
            value="fixture.module:factory",
            load=lambda: create,
        )

    return create


@pytest.fixture
def lock_for(entry_factory):
    def create(
        name,
        *,
        group="dal_obscura.catalogs.v3",
        distribution="plugin-wheel",
        version="1.2.3",
        api_version="3",
    ):
        entry = entry_factory(name, group, distribution, version)
        descriptor = PluginDescriptor(
            kind="catalog" if group.endswith("catalogs.v3") else "table_format",
            plugin_id=name,
            api_version=api_version,
            config_version=1,
            distribution=distribution,
            version=version,
        )
        return build_plugin_lock(kind=descriptor.kind, entry=entry, descriptor=descriptor)

    return create


def test_discovery_does_not_import_unapproved_entry_points(entry_factory) -> None:
    registry = PluginRegistry(
        allowlist={},
        entry_points_fn=lambda: _EntryPoints(
            [entry_factory("unapproved", "dal_obscura.catalogs.v3")]
        ),
        factory_loader=lambda _: pytest.fail("unapproved factory must not load"),
    )

    assert registry.discover() == {}


def test_discovery_ignores_malformed_unallowlisted_entry_point() -> None:
    registry = PluginRegistry(
        allowlist={},
        entry_points_fn=lambda: _EntryPoints(
            [cast(_Entry, SimpleNamespace(name="../../escape", group="dal_obscura.catalogs.v3"))]
        ),
    )

    assert registry.discover() == {}


def test_admitted_entry_point_loads_only_after_lock_match(entry_factory, lock_for) -> None:
    entry = entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3")
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.catalog"): lock_for("iceberg.catalog")},
        entry_points_fn=lambda: _EntryPoints([entry]),
    )

    discovered = registry.discover()

    assert discovered[("catalog", "iceberg.catalog")].api_version == "3"
    with pytest.raises(PluginAdmissionError, match="not admitted"):
        registry.load("catalog", "iceberg.catalog")
    registry.reload()
    assert registry.load("catalog", "iceberg.catalog") is entry.load()


def test_unapproved_entry_point_cannot_be_loaded_on_request(entry_factory) -> None:
    registry = PluginRegistry(
        allowlist={},
        entry_points_fn=lambda: _EntryPoints(
            [entry_factory("unapproved", "dal_obscura.catalogs.v3")]
        ),
        factory_loader=lambda _: pytest.fail("unapproved factory must not load"),
    )

    with pytest.raises(PluginAdmissionError, match="not admitted"):
        registry.load("catalog", "unapproved")


def test_lock_mismatch_and_duplicate_ids_fail_closed(entry_factory, lock_for) -> None:
    mismatched = PluginRegistry(
        allowlist={("catalog", "iceberg.catalog"): lock_for("iceberg.catalog", version="9.9.9")},
        entry_points_fn=lambda: _EntryPoints(
            [entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3")]
        ),
    )
    with pytest.raises(PluginAdmissionError, match="lock mismatch"):
        mismatched.discover()

    duplicate = PluginRegistry(
        allowlist={("catalog", "iceberg.catalog"): lock_for("iceberg.catalog")},
        entry_points_fn=lambda: _EntryPoints(
            [
                entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3"),
                entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3"),
            ]
        ),
    )
    with pytest.raises(PluginAdmissionError, match="Duplicate plugin ID"):
        duplicate.discover()


def test_allowlisted_entry_without_distribution_provenance_fails_closed(lock_for) -> None:
    entry = cast(
        _Entry,
        SimpleNamespace(
            name="iceberg.catalog",
            group="dal_obscura.catalogs.v3",
            dist=None,
            load=lambda: {"name": "iceberg.catalog"},
        ),
    )
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.catalog"): lock_for("iceberg.catalog")},
        entry_points_fn=lambda: _EntryPoints([entry]),
    )

    with pytest.raises(PluginAdmissionError, match="provenance is unavailable"):
        registry.discover()


def test_invalid_plugin_id_cannot_be_loaded() -> None:
    registry = PluginRegistry(entry_points_fn=lambda: _EntryPoints([]))

    with pytest.raises(PluginAdmissionError, match="Invalid plugin ID"):
        registry.load("catalog", "../../import-anything")


def test_failed_reload_keeps_last_valid_admission_snapshot(entry_factory, lock_for) -> None:
    entries = [entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3")]
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.catalog"): lock_for("iceberg.catalog")},
        entry_points_fn=lambda: _EntryPoints(entries),
    )

    initial = registry.reload()
    assert set(registry.admitted()) == {("catalog", "iceberg.catalog")}

    entries[:] = [entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3", version="9.9.9")]
    with pytest.raises(PluginAdmissionError, match="lock mismatch"):
        registry.reload()

    assert registry.admitted() == initial


def test_load_uses_entry_point_captured_by_admitted_generation(entry_factory, lock_for) -> None:
    first = entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3")
    entries = [first]
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.catalog"): lock_for("iceberg.catalog")},
        entry_points_fn=lambda: _EntryPoints(entries),
        factory_loader=lambda entry: entry.load(),
    )

    registry.reload()
    replacement = entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3")
    cast(Any, replacement).load = lambda: {"name": "replacement"}
    entries[:] = [replacement]

    assert registry.load("catalog", "iceberg.catalog") is first.load()


def test_status_report_distinguishes_enabled_missing_and_incompatible_without_import(
    entry_factory, lock_for
) -> None:
    enabled = entry_factory("enabled", "dal_obscura.catalogs.v3")
    entries = [enabled]
    registry = PluginRegistry(
        allowlist={
            ("catalog", "enabled"): lock_for("enabled"),
            ("catalog", "missing"): lock_for(
                "missing", distribution="missing-wheel", version="1.0.0"
            ),
            ("catalog", "wrong"): lock_for("wrong", version="9.9.9"),
        },
        entry_points_fn=lambda: _EntryPoints(entries),
        factory_loader=lambda _: pytest.fail("status reporting must not import factories"),
    )

    registry.reload()
    entries.append(entry_factory("wrong", "dal_obscura.catalogs.v3"))
    statuses = {row["plugin_id"]: row["status"] for row in registry.status_report()}

    assert statuses == {
        "enabled": "enabled",
        "missing": "not_installed",
        "wrong": "incompatible",
    }


def test_descriptor_loader_mismatch_fails_before_factory_import(entry_factory, lock_for) -> None:
    entry = entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3")
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.catalog"): lock_for("iceberg.catalog")},
        entry_points_fn=lambda: _EntryPoints([entry]),
        descriptor_loader=lambda _: PluginDescriptor(
            kind="catalog",
            plugin_id="iceberg.catalog",
            api_version="99",
            config_version=1,
            distribution="plugin-wheel",
            version="1.2.3",
        ),
        factory_loader=lambda _: pytest.fail("mismatched descriptor must not import"),
    )

    with pytest.raises(PluginAdmissionError, match="descriptor mismatch"):
        registry.reload()


def test_registry_rejects_self_consistent_unsupported_api_version(entry_factory, lock_for) -> None:
    entry = entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3")
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.catalog"): lock_for("iceberg.catalog", api_version="99")},
        entry_points_fn=lambda: _EntryPoints([entry]),
        descriptor_loader=lambda _: PluginDescriptor(
            kind="catalog",
            plugin_id="iceberg.catalog",
            api_version="99",
            config_version=1,
            distribution="plugin-wheel",
            version="1.2.3",
        ),
        factory_loader=lambda _: pytest.fail("unsupported API must not import"),
    )

    with pytest.raises(PluginAdmissionError, match="unsupported API version"):
        registry.reload()


def test_registry_rejects_self_consistent_unsupported_config_version(
    entry_factory, lock_for
) -> None:
    entry = entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3")
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.catalog"): lock_for("iceberg.catalog")},
        entry_points_fn=lambda: _EntryPoints([entry]),
        descriptor_loader=lambda _: PluginDescriptor(
            kind="catalog",
            plugin_id="iceberg.catalog",
            api_version="3",
            config_version=2,
            distribution="plugin-wheel",
            version="1.2.3",
        ),
        factory_loader=lambda _: pytest.fail("unsupported config must not import"),
    )

    with pytest.raises(PluginAdmissionError, match="unsupported config version"):
        registry.reload()


def test_malformed_plugin_lock_is_rejected(entry_factory) -> None:
    entry = entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3")
    registry = PluginRegistry(
        allowlist=cast(Any, {("catalog", "iceberg.catalog"): ("plugin-wheel", "1.2.3", "1")}),
        entry_points_fn=lambda: _EntryPoints([entry]),
    )

    with pytest.raises(PluginAdmissionError, match="Invalid plugin lock"):
        registry.reload()


def test_extended_lock_accepts_matching_descriptor_and_distribution_digest(entry_factory) -> None:
    entry = entry_factory("iceberg.catalog", "dal_obscura.catalogs.v3")
    descriptor = load_static_plugin_descriptor(entry)
    registry = PluginRegistry(
        allowlist={("catalog", "iceberg.catalog"): build_plugin_lock("catalog", entry, descriptor)},
        entry_points_fn=lambda: _EntryPoints([entry]),
    )
    assert registry.reload()[("catalog", "iceberg.catalog")] == descriptor


@pytest.mark.parametrize("source", ["global", "captured"])
@pytest.mark.parametrize(
    "credential,message",
    [
        ({"secret": "ENDPOINT", "scope": "catalog:analytics"}, "cannot contain secrets"),
        ("https://user:sentinel-credential@private.example/v1", "no credentials"),
        ("https://private.example/v1?ToKeN=sentinel-credential", "no credentials"),
    ],
)
def test_format_options_keep_credentials_out_of_signed_work(
    entry_factory, source, credential, message
):
    entry = entry_factory("fixture.format", "dal_obscura.table_formats.v3")
    descriptor = replace(
        load_static_plugin_descriptor(entry),
        config_schema={"fields": [{"name": "endpoint", "type": "uri"}]},
    )
    registry = PluginRegistry(
        allowlist={
            ("table_format", "fixture.format"): build_plugin_lock("table_format", entry, descriptor)
        },
        entry_points_fn=lambda: _EntryPoints([entry]),
        descriptor_loader=lambda _: descriptor,
        options={"dal.plugins.fixture.format.endpoint": credential} if source == "global" else {},
    )
    registry.reload()
    assert dict(
        registry.format_options("fixture.format", captured={"endpoint": "https://public.example"})
    ) == {"endpoint": "https://public.example"}
    with pytest.raises(ValueError, match=message) as error:
        if source == "global":
            registry.format_options("fixture.format")
        else:
            registry.format_options("fixture.format", captured={"endpoint": credential})
    assert "sentinel-credential" not in str(error.value)


def test_build_plugin_lock_derives_the_exact_verified_five_part_lock(tmp_path) -> None:
    artifact = tmp_path / "plugin.py"
    artifact.write_text("trusted = True\n")
    descriptor = PluginDescriptor(
        kind="catalog",
        plugin_id="iceberg.catalog",
        api_version="3",
        config_version=1,
        distribution="plugin-wheel",
        version="1.2.3",
    )
    entry = cast(
        metadata.EntryPoint,
        SimpleNamespace(
            name="iceberg.catalog",
            group="dal_obscura.catalogs.v3",
            dist=SimpleNamespace(
                name="plugin-wheel",
                version="1.2.3",
                files=["plugin.py"],
                locate_file=lambda _: artifact,
            ),
        ),
    )

    import hashlib

    descriptor_digest = hashlib.sha256(
        json.dumps(
            {
                "kind": "catalog",
                "plugin_id": "iceberg.catalog",
                "api_version": "3",
                "config_version": 1,
                "distribution": "plugin-wheel",
                "version": "1.2.3",
                "capabilities": [],
                "config_schema": {},
                "display_name": "",
                "output_formats": [],
                "handle_versions": [1],
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    artifact_digest = hashlib.sha256(b"plugin.py\0trusted = True\n\0").hexdigest()
    lock = build_plugin_lock("catalog", entry, descriptor)

    assert lock == (
        "plugin-wheel",
        "1.2.3",
        "3",
        descriptor_digest,
        artifact_digest,
    )


@pytest.mark.parametrize("kind", ["catalog", "table_format"])
def test_registry_rejects_non_callable_admitted_factory(
    kind: PluginKind, entry_factory, lock_for
) -> None:
    group = "dal_obscura.catalogs.v3" if kind == "catalog" else "dal_obscura.table_formats.v3"
    entry = entry_factory("invalid", group)
    registry = PluginRegistry(
        allowlist={(kind, "invalid"): lock_for("invalid", group=group)},
        entry_points_fn=lambda: _EntryPoints([entry]),
        factory_loader=lambda entry: object(),
    )
    registry.reload()

    with pytest.raises(PluginAdmissionError, match="not callable"):
        registry.load(kind, "invalid")
