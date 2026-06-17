from __future__ import annotations

import json
from pathlib import Path


def test_keycloak_demo_fixture_declares_iceberg_and_delta_tables():
    fixture = json.loads(
        Path("examples/demo/keycloak/fixtures/demo_fixture.json").read_text(encoding="utf-8")
    )

    assert {catalog["name"] for catalog in fixture["catalogs"]} == {
        "retail_demo",
        "retail_delta",
    }
    assert {table["backend"] for table in fixture["tables"]} == {"iceberg", "delta"}
    assert all(table["rows"] for table in fixture["tables"])
    assert fixture["owners"] == ["group:asset-owners"]
    assert len(fixture["policies"]) == 3
