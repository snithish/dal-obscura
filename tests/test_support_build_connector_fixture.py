from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from sqlalchemy import select

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import AssetRecord, CatalogRecord, PolicyRuleRecord

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


def test_connector_fixture_matches_rows_and_live_policy(tmp_path: Path):
    metadata = _run_fixture_builder(tmp_path)
    iceberg_table = _load_fixture_table(tmp_path, metadata)
    table = _load_sorted_projection(
        tmp_path, metadata, ["id", "region", "market", "active", "score"]
    )
    expected = metadata["expected"]

    assert metadata["uri"] == f"grpc+tcp://localhost:{FIXED_DUMMY_PORT}"
    assert metadata["user_token"]
    assert iceberg_table.metadata.format_version == 2
    assert set(expected["partition_fields"]) <= {
        field.name for field in iceberg_table.spec().fields
    }
    assert table.num_rows == expected["row_count"] == 125_000
    rows = cast(list[dict[str, Any]], table.to_pylist())
    assert expected["counts"] == {
        "policy_row_count": sum(row["region"] == "us" and row["active"] is True for row in rows),
        "enterprise_row_count": sum(row["market"] == "enterprise" for row in rows),
        "policy_and_enterprise_row_count": sum(
            row["region"] == "us" and row["active"] is True and row["market"] == "enterprise"
            for row in rows
        ),
        "high_score_row_count": sum(row["score"] >= 900.0 for row in rows),
    }

    engine = create_engine_from_url(cast(str, metadata["database_url"]))
    try:
        with session_factory(engine)() as session:
            asset = session.scalar(
                select(AssetRecord)
                .join(CatalogRecord, CatalogRecord.id == AssetRecord.catalog_id)
                .where(
                    CatalogRecord.name == metadata["catalog"],
                    AssetRecord.target == metadata["target"],
                )
            )
            assert asset is not None
            assert asset.backend == "iceberg"
            assert asset.table_identifier == metadata["target"]
            rules = session.scalars(
                select(PolicyRuleRecord)
                .where(PolicyRuleRecord.asset_id == asset.id)
                .order_by(PolicyRuleRecord.ordinal)
            ).all()
            assert [rule.ordinal for rule in rules] == [10, 20]
            mask_types = {
                cast(dict[str, str], mask)["type"]
                for rule in rules
                for mask in rule.masks_json.values()
            }
            assert mask_types == set(expected["mask_rule_types"])
            assert rules[0].row_filter_sql == expected["policy_row_filter"]
            masks = cast(dict[str, dict[str, object]], rules[0].masks_json)
            samples = expected["sample_values"]
            assert masks["notes"]["value"] == samples["redacted_note"]
            assert masks["status"]["value"] == samples["default_status"]
            assert masks["user.preferences.theme"]["value"] == samples["masked_preference_theme"]
    finally:
        engine.dispose()
    assert samples["region_even"] == rows[0]["region"]
    assert samples["region_odd"] == rows[1]["region"]
