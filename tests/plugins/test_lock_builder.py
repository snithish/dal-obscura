from __future__ import annotations

from importlib import metadata

import pytest

from dal_obscura.plugins.registry import PluginAdmissionError
from scripts.build_plugin_lock import build_document, parse_selection, write_lock


def test_parse_selection_requires_an_admitted_plugin_kind() -> None:
    assert parse_selection("catalog:iceberg.catalog") == ("catalog", "iceberg.catalog")
    assert parse_selection("table_format:parquet.dataset") == (
        "table_format",
        "parquet.dataset",
    )
    with pytest.raises(PluginAdmissionError):
        parse_selection("format:parquet.dataset")


def test_build_document_reads_installed_descriptors_and_sorts_rows(tmp_path):
    import hashlib
    import json

    from tests.support.plugins.installed_plugin import installed_plugin

    entries = []
    expected = []
    for plugin_id in ("zeta.delta", "alpha.delta"):
        root = tmp_path / plugin_id
        root.mkdir()
        installed_plugin(root)
        info = root / "acme_delta_plugin-1.0.dist-info"
        entry_file = info / "entry_points.txt"
        entry_file.write_text(entry_file.read_text().replace("acme.delta", plugin_id))
        descriptor_file = info / "dal_obscura-plugin.json"
        descriptor_file.write_text(descriptor_file.read_text().replace("acme.delta", plugin_id))
        distribution = metadata.Distribution.at(info)
        entries.append(("catalog", next(iter(distribution.entry_points))))
        descriptor = {
            "kind": "catalog",
            "plugin_id": plugin_id,
            "api_version": "3",
            "config_version": 1,
            "distribution": "acme-delta-plugin",
            "version": "1.0",
            "capabilities": [],
            "config_schema": {},
            "display_name": "",
            "output_formats": [],
            "handle_versions": [1],
        }
        descriptor_digest = hashlib.sha256(
            json.dumps(descriptor, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        artifact_digest = hashlib.sha256()
        for path in sorted(info.iterdir()):
            if path.name == "RECORD":
                continue
            artifact_digest.update(str(path.relative_to(root)).encode() + b"\0")
            artifact_digest.update(path.read_bytes() + b"\0")
        expected.append(
            {
                "kind": "catalog",
                "plugin_id": plugin_id,
                "lock": [
                    "acme-delta-plugin",
                    "1.0",
                    "3",
                    descriptor_digest,
                    artifact_digest.hexdigest(),
                ],
            }
        )
    document = build_document(
        [("catalog", "zeta.delta"), ("catalog", "alpha.delta")], entry_points=entries
    )
    assert document == {"version": 1, "plugins": sorted(expected, key=lambda row: row["plugin_id"])}


def test_write_lock_preserves_exact_document_and_refuses_overwrite(tmp_path):
    import json

    output = tmp_path / "plugin-lock.json"
    document: dict[str, object] = {"version": 1, "plugins": [{"plugin_id": "fixture"}]}
    write_lock(output, document)
    expected = (json.dumps(document, indent=2, sort_keys=True) + "\n").encode()
    assert output.read_bytes() == expected
    with pytest.raises(PluginAdmissionError, match="refusing to overwrite"):
        write_lock(output, {"version": 1, "plugins": []})
    assert output.read_bytes() == expected
