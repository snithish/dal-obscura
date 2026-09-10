from __future__ import annotations

import pytest

from dal_obscura.control_plane.application.operator_manifest import (
    ManifestValidationError,
    compile_manifest,
    load_manifest,
)


def _manifest() -> dict[str, object]:
    return {
        "runtime": {
            "ticket_ttl_seconds": 300,
            "max_tickets": 16,
            "max_ticket_exchanges": 1,
        },
        "auth_providers": [{"args": {"issuer": "https://issuer.example"}}],
        "catalogs": [
            {"name": "analytics", "options": {"type": "sql", "uri": "sqlite:///warehouse.db"}}
        ],
        "assets": [
            {
                "catalog": "analytics",
                "target": "default.users",
                "table_identifier": "prod.users",
                "rules": [
                    {
                        "principals": ["group:analyst"],
                        "columns": ["id", "email"],
                        "masks": {"email": {"type": "email"}},
                    }
                ],
            }
        ],
    }


def test_compile_manifest_uses_supported_runtime_components() -> None:
    compiled = compile_manifest(_manifest())

    assert compiled.manifest_hash
    assert compiled.assets[0].compiled_config["target"]["table"] == "prod.users"


def test_load_manifest_rejects_duplicate_json_keys(tmp_path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text('{"runtime": {}, "runtime": {}}')

    with pytest.raises(ManifestValidationError, match="duplicate key"):
        load_manifest(path)


def test_compile_manifest_rejects_unknown_catalog_reference() -> None:
    manifest = _manifest()
    manifest["assets"] = [{"catalog": "missing", "target": "users", "table_identifier": "users"}]

    with pytest.raises(ManifestValidationError, match="unknown catalog"):
        compile_manifest(manifest)
