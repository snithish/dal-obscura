from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


def _load_script(name: str) -> ModuleType:
    path = Path(__file__).parents[2] / "examples/demo/keycloak/scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(f"demo_{name}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _fixture() -> dict[str, object]:
    return {
        "catalogs": [{"name": "retail_demo", "plugin_id": "iceberg.sql"}],
        "tables": [{"catalog": "retail_demo", "target": "retail.customer_revenue"}],
    }


def test_seed_table_keeps_existing_table(monkeypatch, tmp_path) -> None:
    module = _load_script("seed_table")
    monkeypatch.setattr(module, "RUNTIME_DIR", tmp_path / ".runtime")
    calls: list[str] = []

    class ExistingCatalog:
        def table_exists(self, target: str) -> bool:
            calls.append(f"exists:{target}")
            return True

    monkeypatch.setattr(module, "load_catalog", lambda *args, **kwargs: ExistingCatalog())
    monkeypatch.setattr(module, "_schemas", lambda fields: pytest.fail("schema should not rebuild"))

    module._create_iceberg_table(
        {"catalog": "retail_demo", "target": "retail.customer_revenue", "schema": []}
    )

    assert calls == ["exists:retail.customer_revenue"]


def test_seed_table_creates_missing_table(monkeypatch, tmp_path) -> None:
    module = _load_script("seed_table")
    monkeypatch.setattr(module, "RUNTIME_DIR", tmp_path / ".runtime")
    calls: list[str] = []

    class EmptyCatalog:
        def table_exists(self, target: str) -> bool:
            calls.append(f"exists:{target}")
            return False

        def create_table(self, target: str, *, schema, properties):
            calls.append(f"create:{target}")
            return type("Table", (), {"append": lambda self, table: calls.append("append")})()

    monkeypatch.setattr(module, "load_catalog", lambda *args, **kwargs: EmptyCatalog())
    monkeypatch.setattr(module, "_schemas", lambda fields: (object(), object()))

    class FakeArrowTable:
        @staticmethod
        def from_pylist(rows, schema):
            return object()

    monkeypatch.setattr(module, "pa", type("Arrow", (), {"Table": FakeArrowTable}))

    module._create_iceberg_table(
        {
            "catalog": "retail_demo",
            "target": "retail.customer_revenue",
            "schema": [],
            "rows": [],
        }
    )

    assert calls == ["exists:retail.customer_revenue", "create:retail.customer_revenue", "append"]


def test_provision_reuses_complete_workspace_without_writes(monkeypatch) -> None:
    monkeypatch.setenv("DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN", "test-admin")
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    module = _load_script("provision_demo")
    calls: list[tuple[str, str]] = []
    responses = {
        "/v1/workspace/summary": {
            "catalog_count": 1,
            "asset_count": 1,
            "unowned_asset_count": 0,
            "missing_policy_count": 0,
            "runtime_configured": True,
            "enabled_auth_provider_count": 1,
        },
        "/v1/assets": [
            {
                "id": "asset-1",
                "catalog": "retail_demo",
                "name": "retail.customer_revenue",
                "policy_status": "configured",
            }
        ],
        "/v1/settings/auth-providers": [module._demo_auth_provider()],
    }

    def request(method: str, path: str, body=None):
        calls.append((method, path))
        return responses[path]

    monkeypatch.setattr(module, "_request", request)
    monkeypatch.setattr(module, "_workspace_cell_id", lambda: "cell-1")

    assert module._provision_workspace(_fixture()) == "cell-1"
    assert calls == [
        ("GET", "/v1/workspace/summary"),
        ("GET", "/v1/assets"),
        ("GET", "/v1/settings/auth-providers"),
    ]


def test_provision_enables_identity_provider_before_saving_live_policy(monkeypatch) -> None:
    monkeypatch.setenv("DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN", "test-admin")
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    module = _load_script("provision_demo")
    fixture = {
        "catalogs": [{"name": "retail_demo", "plugin_id": "iceberg.sql"}],
        "tables": [
            {
                "catalog": "retail_demo",
                "target": "retail.customer_revenue",
                "schema": [],
            }
        ],
        "owners": ["group:asset-owners"],
        "grants": [],
        "policies": [],
    }
    calls: list[tuple[str, str, object]] = []

    def request(method: str, path: str, body=None):
        calls.append((method, path, body))
        if path == "/v1/settings/runtime":
            return {"revision": 0}
        if path == "/v1/settings/auth-providers":
            return []
        if path == "/v1/settings/auth-providers/revision":
            return {"revision": 0}
        if path == "/v1/catalogs/retail_demo/tables":
            return {
                "tables": [
                    {
                        "target": "retail.customer_revenue",
                        "backend": "iceberg",
                        "table_identifier": "retail.customer_revenue",
                    }
                ]
            }
        if path == "/v1/assets/retail_demo/retail.customer_revenue":
            return {"id": "asset-1"}
        if path == "/v1/assets/asset-1":
            return {"revision": 1, "policy_revision": 0}
        if path == "/v1/assets/asset-1/schema":
            return {
                "stable_field_ids": True,
                "fields": [
                    {
                        "field_id": 1,
                        "name": "customer_id",
                        "path": {
                            "segments": [{"kind": "field", "name": "customer_id", "field_id": 1}]
                        },
                        "type": "long",
                        "nullable": False,
                    }
                ],
            }
        return {}

    monkeypatch.setattr(module, "_workspace_state", lambda value: "empty")
    monkeypatch.setattr(module, "_workspace_cell_id", lambda: "cell-1")
    monkeypatch.setattr(module, "_request", request)

    assert module._provision_workspace(fixture) == "cell-1"

    provider_call = next(
        i for i, call in enumerate(calls) if call[1] == "/v1/settings/auth-providers"
    )
    policy_calls = [
        (i, call) for i, call in enumerate(calls) if call[1] == "/v1/assets/asset-1/policy"
    ]
    schema_fields_call = next(
        call for call in calls if call[1] == "/v1/assets/asset-1/schema-fields"
    )
    assert len(policy_calls) == 1
    assert provider_call < policy_calls[0][0]
    runtime_call = next(
        call for call in calls if call[0] == "PUT" and call[1] == "/v1/settings/runtime"
    )
    assert runtime_call[2] == {
        "ticket_ttl_seconds": 600,
        "max_tickets": 16,
        "max_ticket_exchanges": 1,
        "expected_revision": 0,
    }
    assert schema_fields_call[2] == {
        "expected_revision": 1,
        "fields": [
            {
                "name": "customer_id",
                "field_id": "1",
                "path": ["customer_id"],
                "type": "long",
                "nullable": False,
            }
        ],
    }
    assert policy_calls[0][1][2] == {"expected_revision": 0, "rules": []}


def test_provision_rejects_partial_workspace_before_mutation(monkeypatch) -> None:
    monkeypatch.setenv("DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN", "test-admin")
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    module = _load_script("provision_demo")
    calls: list[tuple[str, str]] = []

    def request(method: str, path: str, body=None):
        calls.append((method, path))
        return {
            "catalog_count": 1,
            "asset_count": 1,
            "unowned_asset_count": 0,
            "missing_policy_count": 1,
            "runtime_configured": True,
            "enabled_auth_provider_count": 0,
        }

    monkeypatch.setattr(module, "_request", request)

    with pytest.raises(RuntimeError, match="partially configured"):
        module._provision_workspace(_fixture())

    assert calls == [("GET", "/v1/workspace/summary")]


def test_demo_owner_keys_are_scoped_to_oidc_issuer(monkeypatch) -> None:
    monkeypatch.setenv("DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN", "test-admin")
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    module = _load_script("provision_demo")

    assert module._scoped_demo_owners(["group:asset-owners", "asset-owner"]) == [
        "http://127.0.0.1:20080/realms/dal-obscura-demo|g|asset-owners",
        "http://127.0.0.1:20080/realms/dal-obscura-demo|u|asset-owner",
    ]
    assert module._scoped_demo_owners(["https://issuer.example/realm|group:asset-owners"]) == [
        "https://issuer.example/realm|group:asset-owners"
    ]


def test_demo_owner_keys_require_an_owner(monkeypatch) -> None:
    monkeypatch.setenv("DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN", "test-admin")
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", "sqlite+pysqlite:///:memory:")
    module = _load_script("provision_demo")

    with pytest.raises(ValueError, match="at least one owner"):
        module._scoped_demo_owners([])
