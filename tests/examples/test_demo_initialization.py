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
        "catalogs": [{"name": "retail_demo", "kind": "iceberg_sql"}],
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
            {"id": "asset-1", "catalog": "retail_demo", "target": "retail.customer_revenue"}
        ],
        "/v1/policy-versions": [{"asset_id": "asset-1"}],
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
        ("GET", "/v1/policy-versions"),
    ]


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
        "http://127.0.0.1:8080/realms/dal-obscura-demo|group:asset-owners",
        "http://127.0.0.1:8080/realms/dal-obscura-demo|asset-owner",
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
