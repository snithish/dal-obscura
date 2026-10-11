from __future__ import annotations

import hashlib
import json
from importlib import metadata
from typing import cast

import pytest

from dal_obscura.sources.plugins import PluginAdmissionError, load_plugin_lock_file
from dal_obscura.sources.plugins.inspection import inspect_distributions
from tests.support.installed_plugin import installed_plugin


def test_inspection_reads_installed_artifacts_without_importing_factory(tmp_path, monkeypatch):
    entries = installed_plugin(tmp_path)
    monkeypatch.setattr(metadata, "entry_points", lambda: entries)
    inspected = inspect_distributions(["acme_delta_plugin"])
    descriptor = cast(list[dict[str, object]], inspected["descriptors"])[0]
    digest = hashlib.sha256()
    for path in sorted((tmp_path / "acme_delta_plugin-1.0.dist-info").iterdir()):
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
            "2",
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
