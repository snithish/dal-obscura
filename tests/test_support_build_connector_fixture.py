from __future__ import annotations

import json
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]


def _reserve_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run_fixture_builder(tmp_path: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            "tests/support/build_connector_fixture.py",
            "--output-dir",
            str(tmp_path),
            "--port",
            str(_reserve_port()),
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return cast(dict[str, Any], json.loads(completed.stdout))


def test_build_connector_fixture_emits_heavyweight_metadata(tmp_path: Path):
    metadata = _run_fixture_builder(tmp_path)

    assert metadata["catalog"] == "spark_catalog"
    assert metadata["target"] == "default.complex_users"
    assert Path(metadata["app_path"]).is_file()
    assert metadata["user_token"]

    expected = metadata["expected"]
    assert expected["row_count"] >= 100_000
    assert expected["supports_multiple_tickets"] is True
    assert expected["append_batch_count"] >= 2
    assert expected["partitioned"] is True
    assert expected["mask_rule_types"] == [
        "default",
        "email",
        "hash",
        "keep_last",
        "null",
        "redact",
    ]


def test_build_connector_fixture_writes_expected_policy_and_paths(tmp_path: Path):
    metadata = _run_fixture_builder(tmp_path)

    app_path = Path(metadata["app_path"])
    app_config = yaml.safe_load(app_path.read_text())
    policy_path = Path(app_config["policy_file"])
    policy = yaml.safe_load(policy_path.read_text())
    target_policy = policy["catalogs"][metadata["catalog"]]["targets"][metadata["target"]]
    rules = target_policy["rules"]

    assert app_config["catalog_file"] == str(tmp_path / "catalogs.yaml")
    assert policy_path == tmp_path / "policies.yaml"
    assert len(rules) >= 2

    mask_types = {mask["type"] for rule in rules for mask in rule.get("masks", {}).values()}
    assert mask_types == {"null", "redact", "hash", "default", "email", "keep_last"}

    expected = metadata["expected"]
    assert expected["catalog"] == metadata["catalog"]
    assert expected["target"] == metadata["target"]
    assert expected["sample_values"]["region_even"] == "us"
    assert expected["sample_values"]["region_odd"] == "eu"
    assert expected["sample_values"]["masked_email"] == "u***@example.com"
