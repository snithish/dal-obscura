from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from sqlalchemy import select

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import (
    ActivePublicationRecord,
    PolicyRuleRecord,
    PublishedAssetRecord,
)

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


def _load_fixture_table(tmp_path: Path, metadata: dict[str, Any]):
    return load_catalog(
        cast(str, metadata["catalog"]),
        type="sql",
        uri=f"sqlite:///{tmp_path / 'spark_catalog.db'}",
        warehouse=str(tmp_path / "warehouse"),
    ).load_table(cast(str, metadata["target"]))


def _load_sorted_projection(
    tmp_path: Path,
    metadata: dict[str, Any],
    columns: list[str],
) -> pa.Table:
    return (
        _load_fixture_table(tmp_path, metadata)
        .scan()
        .to_arrow()
        .select(columns)
        .sort_by([("id", "ascending")])
    )


def test_build_connector_fixture_uses_iceberg_sql_catalog(tmp_path: Path):
    metadata = _run_fixture_builder(tmp_path)
    table = _load_fixture_table(tmp_path, metadata)

    assert cast(str, metadata["uri"]) == f"grpc+tcp://localhost:{FIXED_DUMMY_PORT}"
    assert (
        cast(str, metadata["database_url"]) == f"sqlite+pysqlite:///{tmp_path / 'control-plane.db'}"
    )
    assert UUID(cast(str, metadata["cell_id"]))
    assert Path(tmp_path / "spark_catalog.db").is_file()
    assert Path(tmp_path / "control-plane.db").is_file()
    assert Path(tmp_path / "warehouse").is_dir()
    assert not Path(tmp_path / "app.yaml").exists()
    assert not Path(tmp_path / "catalogs.yaml").exists()
    assert not Path(tmp_path / "policies.yaml").exists()
    assert table.metadata.format_version == 2
    assert [field.name for field in table.spec().fields] == ["region", "market"]


def test_build_connector_fixture_emits_exact_heavyweight_metadata(tmp_path: Path):
    metadata = _run_fixture_builder(tmp_path)
    table = _load_sorted_projection(
        tmp_path, metadata, ["id", "region", "market", "active", "score"]
    )

    assert metadata["catalog"] == "spark_catalog"
    assert metadata["target"] == "default.complex_users"
    assert "app_path" not in metadata
    assert "catalog_path" not in metadata
    assert "policy_path" not in metadata
    assert Path(cast(str, metadata["database_url"]).removeprefix("sqlite+pysqlite:///")).is_file()
    assert UUID(cast(str, metadata["cell_id"]))
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


def test_build_connector_fixture_publishes_expected_policy_and_runtime(tmp_path: Path):
    metadata = _run_fixture_builder(tmp_path)
    table = _load_sorted_projection(tmp_path, metadata, ["id", "region", "email"])

    engine = create_engine_from_url(cast(str, metadata["database_url"]))
    with session_factory(engine)() as session:
        active = session.get(ActivePublicationRecord, UUID(cast(str, metadata["cell_id"])))
        assert active is not None
        published = session.scalar(
            select(PublishedAssetRecord).where(
                PublishedAssetRecord.publication_id == active.publication_id,
                PublishedAssetRecord.catalog == metadata["catalog"],
                PublishedAssetRecord.target == metadata["target"],
            )
        )
        assert published is not None
        target_config = published.compiled_config_json["target"]
        rules = published.compiled_config_json["policy"]["rules"]
        authoring_rules = session.scalars(
            select(PolicyRuleRecord).order_by(PolicyRuleRecord.ordinal)
        ).all()

    assert target_config == {
        "backend": "iceberg",
        "table": "default.complex_users",
        "options": {},
    }
    assert len(rules) >= 2
    assert [rule.ordinal for rule in authoring_rules] == [10, 20]

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
