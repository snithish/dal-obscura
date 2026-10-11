"""Deployment draft data shared by operator boundary tests."""

from __future__ import annotations

import json
from pathlib import Path


def prepared_operator_workspace(workspace: Path) -> dict:
    image = "sha256:" + "c" * 64
    descriptor = {
        "kind": "catalog",
        "plugin_id": "acme.delta",
        "api_version": "2",
        "config_version": 1,
        "distribution": "acme-delta-plugin",
        "version": "1.0",
        "config_schema": {},
    }
    candidate = {
        "image": image,
        "base_image": "server@sha256:" + "a" * 64,
        "requirements": {"acme-delta-plugin": "acme-delta-plugin==1.0"},
        "descriptors": [descriptor],
        "lock": {
            "version": 1,
            "plugins": [
                {
                    "kind": "catalog",
                    "plugin_id": "acme.delta",
                    "lock": ["acme-delta-plugin", "1.0", "2", "d" * 64, "e" * 64],
                }
            ],
        },
    }
    target = {
        "base_image": candidate["base_image"],
        "uv_image": "uv@sha256:" + "b" * 64,
        "compose_file": "compose.yaml",
        "control_url": "https://control.example",
        "image_repository": "registry.example/dal-obscura",
        "project_name": "operator-test",
        "smoke": {
            "uri": "grpc+tls://flight.example:443",
            "catalog": "synthetic",
            "target": "default.smoke",
            "columns": ["id"],
            "expected_rows": [{"id": 1}],
        },
    }
    (workspace / "candidate.json").write_text(json.dumps(candidate))
    (workspace / "target.json").write_text(json.dumps(target))
    return candidate
