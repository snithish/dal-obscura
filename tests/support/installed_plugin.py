"""Small on-disk distribution for static installed-artifact admission tests."""

from __future__ import annotations

import json
from importlib import metadata
from pathlib import Path


def installed_plugin(tmp_path: Path, *, api_version: str = "2") -> metadata.EntryPoints:
    info = tmp_path / "acme_delta_plugin-1.0.dist-info"
    info.mkdir()
    (info / "METADATA").write_text("Metadata-Version: 2.1\nName: acme-delta-plugin\nVersion: 1.0\n")
    (info / "entry_points.txt").write_text(
        "[dal_obscura.catalogs.v2]\nacme.delta = never_import_this_factory:catalog\n"
    )
    (info / "dal_obscura-plugin.json").write_text(
        json.dumps(
            {
                "descriptors": [
                    {
                        "kind": "catalog",
                        "plugin_id": "acme.delta",
                        "api_version": api_version,
                        "config_version": 1,
                    }
                ],
            }
        )
    )
    (info / "RECORD").write_text(
        "\n".join(
            f"{info.name}/{name},,"
            for name in (
                "METADATA",
                "entry_points.txt",
                "dal_obscura-plugin.json",
                "RECORD",
            )
        )
    )
    return metadata.EntryPoints(metadata.Distribution.at(info).entry_points)
