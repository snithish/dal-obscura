from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import yaml
from pyiceberg.catalog import load_catalog

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXED_DUMMY_PORT = 31337


def _run_fixture_builder(tmp_path: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [
            sys.executable,
            "tests/support/build_connector_fixture.py",
            "--output-dir",
            str(tmp_path),
            "--port",
            str(FIXED_DUMMY_PORT),
        ],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return cast(dict[str, Any], json.loads(completed.stdout))


def _load_fixture_table(metadata: dict[str, Any]):
    catalog_path = Path(cast(str, metadata["catalog_path"]))
    catalog_config = yaml.safe_load(catalog_path.read_text())
    catalog_entry = catalog_config["catalogs"][metadata["catalog"]]
    return load_catalog(
        cast(str, metadata["catalog"]),
        type=catalog_entry["options"]["type"],
        uri=catalog_entry["options"]["uri"],
        warehouse=catalog_entry["options"]["warehouse"],
    ).load_table(cast(str, metadata["target"]))


def _load_sorted_projection(metadata: dict[str, Any], columns: list[str]) -> pa.Table:
    return (
        _load_fixture_table(metadata)
        .scan()
        .to_arrow()
        .select(columns)
        .sort_by([("id", "ascending")])
    )


def test_build_connector_fixture_uses_iceberg_sql_catalog(tmp_path: Path):
    metadata = _run_fixture_builder(tmp_path)
    app_path = Path(cast(str, metadata["app_path"]))
    app_config = yaml.safe_load(app_path.read_text())
    catalog_path = Path(cast(str, metadata["catalog_path"]))
    catalog_config = yaml.safe_load(catalog_path.read_text())
    catalog_entry = catalog_config["catalogs"][metadata["catalog"]]
    table = _load_fixture_table(metadata)

    assert cast(str, metadata["uri"]) == f"grpc+tcp://localhost:{FIXED_DUMMY_PORT}"
    assert app_config["catalog_file"] == str(catalog_path)
    assert catalog_entry["module"] == (
        "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog"
    )
    assert catalog_entry["options"] == {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path / 'spark_catalog.db'}",
        "warehouse": str(tmp_path / "warehouse"),
    }
    assert Path(tmp_path / "spark_catalog.db").is_file()
    assert Path(tmp_path / "warehouse").is_dir()
    assert table.metadata.format_version == 2
    assert [field.name for field in table.spec().fields] == ["region", "market"]


def test_build_connector_fixture_emits_exact_heavyweight_metadata(tmp_path: Path):
    metadata = _run_fixture_builder(tmp_path)
    table = _load_sorted_projection(metadata, ["id", "region", "market", "active", "score"])

    assert metadata["catalog"] == "spark_catalog"
    assert metadata["target"] == "default.complex_users"
    assert Path(metadata["app_path"]).is_file()
    assert Path(cast(str, metadata["catalog_path"])).is_file()
    assert Path(cast(str, metadata["policy_path"])).is_file()
    assert metadata["user_token"]

    expected = metadata["expected"]
    assert expected["catalog"] == "spark_catalog"
    assert expected["target"] == "default.complex_users"
    assert expected["row_count"] == 125_000
    assert table.num_rows == 125_000
    assert expected["append_batch_count"] == 5
    assert expected["rows_per_batch"] == 25_000
    assert expected["supports_multiple_tickets"] is True
    assert expected["partitioned"] is True
    assert expected["partition_fields"] == ["region"]
    assert expected["mask_rule_types"] == [
        "default",
        "email",
        "hash",
        "keep_last",
        "null",
        "redact",
    ]
    assert expected["policy_row_filter"] == "region = 'us' AND active = true"
    assert expected["counts"] == {
        "policy_row_count": 41_666,
        "enterprise_row_count": 50_000,
        "policy_and_enterprise_row_count": 16_666,
        "high_score_row_count": 12_500,
    }
    assert expected["projection_columns"] == {
        "masked": [
            "id",
            "region",
            "market",
            "active",
            "account_number",
            "email",
            "status",
            "notes",
            "nickname",
            "user",
            "account",
            "devices",
            "support_ticket",
        ],
        "nested": [
            "user.email",
            "user.address.zip",
            "user.preferences.theme",
            "account.manager.region",
            "support_ticket.ticket_id",
        ],
    }
    assert expected["sample_values"] == {
        "region_even": "us",
        "region_odd": "eu",
        "masked_email": "u***@example.com",
        "redacted_note": "[redacted-note]",
        "default_status": "partner-visible",
        "masked_preference_theme": "[hidden-theme]",
        "null_nickname": None,
        "hash_hex_length": 64,
    }
    assert expected["filter_examples"] == {
        "pushdown": "market = 'enterprise'",
        "residual": "score >= 900.0",
        "partition": "region = 'us'",
    }
    rows = cast(list[dict[str, Any]], table.to_pylist())
    assert expected["counts"] == {
        "policy_row_count": sum(
            1 for row in rows if row["region"] == "us" and row["active"] is True
        ),
        "enterprise_row_count": sum(1 for row in rows if row["market"] == "enterprise"),
        "policy_and_enterprise_row_count": sum(
            1
            for row in rows
            if row["region"] == "us" and row["active"] is True and row["market"] == "enterprise"
        ),
        "high_score_row_count": sum(1 for row in rows if row["score"] >= 900.0),
    }


def test_build_connector_fixture_writes_expected_policy_and_paths(tmp_path: Path):
    metadata = _run_fixture_builder(tmp_path)
    table = _load_sorted_projection(metadata, ["id", "region", "email"])

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
    spark_user_rule = rules[0]
    assert spark_user_rule["row_filter"] == expected["policy_row_filter"]
    assert spark_user_rule["masks"]["notes"]["value"] == expected["sample_values"]["redacted_note"]
    assert (
        spark_user_rule["masks"]["status"]["value"] == expected["sample_values"]["default_status"]
    )
    assert (
        spark_user_rule["masks"]["user.preferences.theme"]["value"]
        == expected["sample_values"]["masked_preference_theme"]
    )
    first_two_rows = cast(list[dict[str, Any]], table.slice(0, 2).to_pylist())
    assert expected["sample_values"]["region_even"] == first_two_rows[0]["region"]
    assert expected["sample_values"]["region_odd"] == first_two_rows[1]["region"]
    assert expected["sample_values"]["masked_email"] == "u***@example.com"
    assert expected["sample_values"]["null_nickname"] is None
