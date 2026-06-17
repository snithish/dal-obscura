from __future__ import annotations

import json
from pathlib import Path


def test_keycloak_demo_fixture_declares_catalog_backed_iceberg_tables():
    fixture = json.loads(
        Path("examples/demo/keycloak/fixtures/demo_fixture.json").read_text(encoding="utf-8")
    )

    assert {catalog["name"] for catalog in fixture["catalogs"]} == {"retail_demo"}
    assert {catalog["kind"] for catalog in fixture["catalogs"]} == {"iceberg_sql"}
    assert {table["backend"] for table in fixture["tables"]} == {"iceberg"}
    assert all("table_path" not in table for table in fixture["tables"])
    assert all(table["rows"] for table in fixture["tables"])
    assert fixture["owners"] == ["group:asset-owners"]
    assert len(fixture["policies"]) == 3
