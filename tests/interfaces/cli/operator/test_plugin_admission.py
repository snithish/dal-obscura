from __future__ import annotations

import json
import os

import pytest

from dal_obscura.interfaces.cli import operator


@pytest.mark.parametrize("malformed", ["previous", "inspected"])
def test_malformed_admission_preserves_prepared_candidate(tmp_path, monkeypatch, capsys, malformed):
    (tmp_path / "target.json").write_text(
        json.dumps(
            {
                "base_image": "server@sha256:" + "a" * 64,
                "uv_image": "uv@sha256:" + "b" * 64,
            }
        )
    )
    previous_lock = ["acme-plugin", "1.0", "3", "a" * 64, "b" * 64]
    inspected_lock = ["acme-plugin", "2.0", "3", "c" * 64, "d" * 64]
    invalid = previous_lock if malformed == "previous" else inspected_lock
    invalid[4] = "invalid-artifact-digest"
    previous = {
        "requirements": {"acme-plugin": "acme-plugin==1.0"},
        "lock": {
            "version": 1,
            "plugins": [
                {
                    "kind": "catalog",
                    "plugin_id": "acme.catalog",
                    "lock": previous_lock,
                }
            ],
        },
    }
    inspected = {
        "lock": {
            "version": 1,
            "plugins": [
                {
                    "kind": "catalog",
                    "plugin_id": "acme.catalog",
                    "lock": inspected_lock,
                }
            ],
        },
        "descriptors": [
            {"distribution": "acme-plugin", "version": "2.0", "plugin_id": "acme.catalog"}
        ],
    }
    prepared = json.dumps(previous)
    (tmp_path / "candidate.json").write_text(prepared)
    inspection_path = tmp_path / "inspection.json"
    inspection_path.write_text(json.dumps(inspected))
    docker = tmp_path / "docker"
    docker.write_text(
        '#!/bin/sh\ncase "$1" in\n'
        "build) exit 0 ;;\n"
        f"image) printf '%s\\n' 'sha256:{'e' * 64}' ;;\n"
        'run) cat "$DAL_TEST_IMAGE_INSPECTION" ;;\n'
        "*) exit 2 ;;\nesac\n"
    )
    docker.chmod(0o700)
    monkeypatch.setenv("PATH", str(tmp_path) + os.pathsep + os.environ["PATH"])
    monkeypatch.setenv("DAL_TEST_IMAGE_INSPECTION", str(inspection_path))

    assert operator.main(["--workspace", str(tmp_path), "plugin", "add", "acme-plugin"]) == 1
    assert (tmp_path / "candidate.json").read_text() == prepared
    assert "digests" in capsys.readouterr().err
