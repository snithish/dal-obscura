from __future__ import annotations

import hashlib
import json
from importlib import metadata
from typing import cast

import pytest

from dal_obscura.plugins import PluginAdmissionError, load_plugin_lock_file
from dal_obscura.plugins.inspection import inspect_distributions
from tests.support.plugins.installed_plugin import installed_plugin


def test_inspection_reads_installed_artifacts_without_importing_factory(tmp_path, monkeypatch):
    entries = installed_plugin(tmp_path)
    monkeypatch.setattr(metadata, "entry_points", lambda: entries)
    inspected = inspect_distributions(["acme_delta_plugin"])
    descriptor = cast(list[dict[str, object]], inspected["descriptors"])[0]
    digest = hashlib.sha256()
    for path in sorted((tmp_path / "acme_delta_plugin-1.0.dist-info").iterdir()):
        if path.name == "RECORD":
            continue
        digest.update(str(path.relative_to(tmp_path)).encode() + b"\0")
        digest.update(path.read_bytes() + b"\0")
    descriptor_digest = hashlib.sha256(
        json.dumps(
            descriptor,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    path = tmp_path / "plugin-lock.json"
    path.write_text(json.dumps(inspected["lock"]))
    assert load_plugin_lock_file(path) == {
        ("catalog", "acme.delta"): (
            "acme-delta-plugin",
            "1.0",
            "3",
            descriptor_digest,
            digest.hexdigest(),
        )
    }
    assert descriptor["plugin_id"] == "acme.delta"


def test_inspection_refuses_unsupported_api_even_with_consistent_artifact(tmp_path, monkeypatch):
    entries = installed_plugin(tmp_path, api_version="1")
    monkeypatch.setattr(metadata, "entry_points", lambda: entries)
    with pytest.raises(PluginAdmissionError, match="unsupported API"):
        inspect_distributions(["acme-delta-plugin"])


def test_inspection_does_not_admit_plugins_from_unselected_dependencies(tmp_path, monkeypatch):
    entries = installed_plugin(tmp_path)
    monkeypatch.setattr(metadata, "entry_points", lambda: entries)
    with pytest.raises(PluginAdmissionError, match="No static plugins found for: other-package"):
        inspect_distributions(["other-package"])


def test_artifact_lock_is_portable_between_installations(tmp_path, monkeypatch):
    locks = []
    for name in ("planner", "worker"):
        directory = tmp_path / name
        directory.mkdir()
        entries = installed_plugin(directory)
        info = directory / "acme_delta_plugin-1.0.dist-info"
        (info / "direct_url.json").write_text(json.dumps({"url": str(directory)}))
        with (info / "RECORD").open("a") as record:
            record.write(f"\n{info.name}/direct_url.json,,\n")
        monkeypatch.setattr(metadata, "entry_points", lambda entries=entries: entries)
        locks.append(inspect_distributions(["acme-delta-plugin"])["lock"])
    assert locks[0] == locks[1]
