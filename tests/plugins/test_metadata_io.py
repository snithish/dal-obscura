"""Installed admission metadata must fit its budget before allocation."""

import builtins
import io
import json
import os
from contextlib import contextmanager
from functools import partial
from importlib import metadata
from pathlib import Path
from types import SimpleNamespace

import pytest

from dal_obscura.plugins import (
    PluginAdmissionError,
    PluginRegistry,
    build_plugin_lock,
    load_plugin_lock_file,
    load_static_plugin_descriptor,
)
from tests.support.plugins.installed_plugin import installed_plugin


@pytest.mark.parametrize("kind", ["lock", "descriptor", "artifact"])
def test_metadata_reads_are_bounded_at_the_file_boundary(tmp_path, monkeypatch, kind):
    entry = next(iter(installed_plugin(tmp_path)))
    if kind in {"descriptor", "artifact"}:
        target = tmp_path / "acme_delta_plugin-1.0.dist-info/dal_obscura-plugin.json"
        descriptor = load_static_plugin_descriptor(entry)
        if kind == "descriptor":
            limit = 65_536
            load = partial(load_static_plugin_descriptor, entry)
            expected = descriptor
        else:
            limit = 1_048_576
            load = partial(build_plugin_lock, "catalog", entry, descriptor)
            expected = load()
    else:
        target = tmp_path / "plugins.json"
        target.write_text('{"version":1,"plugins":[]}')
        limit = 1_048_576
        load = partial(load_plugin_lock_file, target)
    target_identity = (target.stat().st_dev, target.stat().st_ino)
    original_open = io.open
    reads = []

    @contextmanager
    def bounded_open(*args, **kwargs):
        with original_open(*args, **kwargs) as stream:
            info = os.fstat(stream.fileno())
            if (info.st_dev, info.st_ino) != target_identity:
                yield stream
                return

            def read(size=-1):
                assert 0 <= size <= limit + 1, "metadata read exceeded its allocation budget"
                reads.append(size)
                return stream.read(size)

            yield SimpleNamespace(read=read, fileno=stream.fileno)

    monkeypatch.setattr(io, "open", bounded_open)
    monkeypatch.setattr(builtins, "open", bounded_open)
    if kind == "lock":
        # An empty plugin list is invalid, after the bounded read succeeds.
        with pytest.raises(PluginAdmissionError, match="bounded plugin list"):
            load()
    else:
        assert load() == expected
    assert reads


@pytest.mark.parametrize("location", ["metadata", "root", "package"])
def test_descriptor_is_read_from_advertised_wheel_files_without_imports(tmp_path, location):
    entry = next(iter(installed_plugin(tmp_path)))
    info = tmp_path / "acme_delta_plugin-1.0.dist-info"
    source = info / "dal_obscura-plugin.json"
    if location != "metadata":
        relative = (
            "dal_obscura-plugin.json"
            if location == "root"
            else "never_import_this_factory/dal_obscura-plugin.json"
        )
        target = tmp_path / relative
        target.parent.mkdir(exist_ok=True)
        source.rename(target)
        record = info / "RECORD"
        record.write_text(
            record.read_text().replace(
                "acme_delta_plugin-1.0.dist-info/dal_obscura-plugin.json", relative
            )
        )
    descriptor = load_static_plugin_descriptor(entry)
    assert (descriptor.kind, descriptor.plugin_id, descriptor.distribution, descriptor.version) == (
        "catalog",
        "acme.delta",
        "acme-delta-plugin",
        "1.0",
    )


@pytest.mark.parametrize(
    ("raw", "message"),
    [
        (
            '{"kind":"catalog","plugin_id":"other","api_version":"3","config_version":1}',
            "identity mismatch",
        ),
        ('{"kind":"catalog","plugin_id":"acme.delta","plugin_id":"other"}', "invalid JSON"),
        (
            '{"kind":"catalog","plugin_id":"acme.delta","api_version":"3","config_version":true}',
            "version fields",
        ),
        (" " * 65_537, "too large"),
    ],
)
def test_descriptor_rejects_invalid_or_oversize_metadata(tmp_path, raw, message):
    entry = next(iter(installed_plugin(tmp_path)))
    (tmp_path / "acme_delta_plugin-1.0.dist-info/dal_obscura-plugin.json").write_text(raw)
    with pytest.raises(PluginAdmissionError, match=message):
        load_static_plugin_descriptor(entry)


def test_descriptor_selects_one_identity_from_a_multi_kind_wheel(tmp_path):
    installed_plugin(tmp_path)
    path = tmp_path / "acme_delta_plugin-1.0.dist-info/dal_obscura-plugin.json"
    payload = json.loads(path.read_text())
    payload["descriptors"].append(
        {
            "kind": "table_format",
            "plugin_id": "delta",
            "api_version": "3",
            "config_version": 1,
        }
    )
    path.write_text(json.dumps(payload))
    info = tmp_path / "acme_delta_plugin-1.0.dist-info"
    (info / "entry_points.txt").write_text(
        "[dal_obscura.table_formats.v3]\ndelta = never_import_this_factory:format\n"
    )
    format_entry = next(iter(metadata.Distribution.at(info).entry_points))
    descriptor = load_static_plugin_descriptor(format_entry)
    assert (descriptor.kind, descriptor.plugin_id) == ("table_format", "delta")


def test_admission_hashes_a_shared_distribution_once_and_rechecks_on_reload(tmp_path, monkeypatch):
    installed_plugin(tmp_path)
    info = tmp_path / "acme_delta_plugin-1.0.dist-info"
    manifest = info / "dal_obscura-plugin.json"
    payload = json.loads(manifest.read_text())
    payload["descriptors"].append(
        {
            "kind": "table_format",
            "plugin_id": "delta",
            "api_version": "3",
            "config_version": 1,
        }
    )
    manifest.write_text(json.dumps(payload))
    (info / "entry_points.txt").write_text(
        "[dal_obscura.catalogs.v3]\nacme.delta = never_import_this_factory:catalog\n"
        "[dal_obscura.table_formats.v3]\ndelta = never_import_this_factory:format\n"
    )
    entries = metadata.Distribution.at(info).entry_points
    locks = {}
    for entry in entries:
        descriptor = load_static_plugin_descriptor(entry)
        locks[(descriptor.kind, descriptor.plugin_id)] = build_plugin_lock(
            descriptor.kind, entry, descriptor
        )
    registry = PluginRegistry(allowlist=locks, entry_points_fn=lambda: entries)
    artifact_opens = []
    original_open = builtins.open

    def counted_open(file, *args, **kwargs):
        if Path(file) == manifest:
            artifact_opens.append(file)
        return original_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", counted_open)
    assert set(registry.reload()) == set(locks)
    # Two bounded descriptor reads and one artifact read for the shared wheel.
    assert len(artifact_opens) == 3
    manifest.write_text(manifest.read_text() + " ")
    with pytest.raises(PluginAdmissionError, match="artifact digest mismatch"):
        registry.reload()


@pytest.mark.parametrize("replacement", ["missing", "directory", "symlink", "fifo"])
def test_descriptor_rejects_unreadable_or_special_files(tmp_path, replacement):
    entry = next(iter(installed_plugin(tmp_path)))
    path = tmp_path / "acme_delta_plugin-1.0.dist-info/dal_obscura-plugin.json"
    path.unlink()
    if replacement == "directory":
        path.mkdir()
    elif replacement == "symlink":
        (tmp_path / "other.json").write_text("{}")
        path.symlink_to(tmp_path / "other.json")
    elif replacement == "fifo":
        os.mkfifo(path)
    with pytest.raises(
        PluginAdmissionError, match="missing" if replacement == "missing" else "unreadable"
    ):
        load_static_plugin_descriptor(entry)
