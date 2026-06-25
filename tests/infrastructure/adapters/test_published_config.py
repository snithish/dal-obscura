from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from uuid import uuid4

import pyarrow as pa
import pytest
from sqlalchemy.orm import Session

from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.catalog.ports import (
    CatalogTableDescriptor,
    CatalogTableListing,
    TableFormat,
)
from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import (
    Base,
    PublishedCatalogRecord,
    PublishedCellRuntimeRecord,
)
from dal_obscura.common.query_planning.models import PlanRequest
from dal_obscura.common.table_format.ports import InputPartition, Plan, ScanTask
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore
from dal_obscura.data_plane.infrastructure.adapters.published_config import (
    PublishedConfigAuthorizer,
    PublishedConfigCatalogRegistry,
    PublishedConfigStore,
)
from dal_obscura.data_plane.infrastructure.table_providers.registry import TableProviderFactory

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
FAKE_CATALOG_MODULE = "tests.infrastructure.adapters.test_published_config.FakeCatalog"


@pytest.fixture
def db_session() -> Iterator[Session]:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_maker = session_factory(engine)
    with session_maker() as session:
        yield session


def test_published_authorizer_resolves_policy_from_active_asset(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    authorizer = PublishedConfigAuthorizer(PublishedConfigStore(db_session, cell_id=cell_id))

    decision = authorizer.authorize(
        principal=Principal(id="user1", groups=[], attributes={"tenant_id": str(tenant_id)}),
        target="default.users",
        catalog="analytics",
        requested_columns=["id", "email"],
    )

    assert decision.allowed_columns == ["id", "email"]
    assert decision.masks["email"].type == "email"
    assert decision.row_filter == "(region = 'us')"
    assert decision.policy_version == 123


def test_published_authorizer_accepts_tenant_slug_attribute(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    authorizer = PublishedConfigAuthorizer(PublishedConfigStore(db_session, cell_id=cell_id))

    decision = authorizer.authorize(
        principal=Principal(id="user1", groups=[], attributes={"tenant_id": f"tenant-{tenant_id}"}),
        target="default.users",
        catalog="analytics",
        requested_columns=["id"],
    )

    assert decision.allowed_columns == ["id"]
    assert decision.policy_version == 123


def test_published_store_uses_last_good_asset_after_transient_failure(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    config_store = PublishedConfigStore(db_session, cell_id=cell_id)

    first = config_store.get_asset(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )

    def fail_scalar(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(db_session, "scalar", fail_scalar)
    second = config_store.get_asset(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )

    assert second == first


def test_published_catalog_registry_reads_catalog_config_from_published_catalogs(
    db_session: Session,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(
        db_session,
        cell_id=cell_id,
        tenant_id=tenant_id,
        policy_version=123,
        catalog_module=FAKE_CATALOG_MODULE,
        catalog_options={
            "provider_id": "postgres",
            "provider_modules": [
                "tests.infrastructure.adapters.test_published_config.FakePostgresFactory"
            ],
            "dsn": "postgresql://example/db",
        },
        backend="postgres",
        table="legacy-asset-table-should-not-be-used",
    )
    registry = PublishedConfigCatalogRegistry(PublishedConfigStore(db_session, cell_id=cell_id))

    table = registry.describe("analytics", "default.users", tenant_id=str(tenant_id))

    assert table.format == "postgres"
    assert table.table_name == "default.users"


def test_published_config_catalog_registry_reuses_registry_for_active_publication(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(
        db_session,
        cell_id=cell_id,
        tenant_id=tenant_id,
        policy_version=123,
        catalog_module=FAKE_CATALOG_MODULE,
        catalog_options={
            "provider_id": "postgres",
            "provider_modules": [
                "tests.infrastructure.adapters.test_published_config.FakePostgresFactory"
            ],
            "dsn": "postgresql://example/db",
        },
        backend="postgres",
    )
    registry_constructions = 0
    from dal_obscura.data_plane.infrastructure.adapters import published_config

    real_registry = published_config.DynamicCatalogRegistry

    class CountingDynamicCatalogRegistry(real_registry):
        def __init__(self, *args, **kwargs):
            nonlocal registry_constructions
            registry_constructions += 1
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(
        published_config,
        "DynamicCatalogRegistry",
        CountingDynamicCatalogRegistry,
    )
    registry = PublishedConfigCatalogRegistry(PublishedConfigStore(db_session, cell_id=cell_id))

    first = registry.describe("analytics", "default.users", tenant_id=str(tenant_id))
    second = registry.describe("analytics", "default.users", tenant_id=str(tenant_id))

    assert first.format == "postgres"
    assert second.format == "postgres"
    assert registry_constructions == 1


def _publish_asset(
    session: Session,
    *,
    cell_id,
    tenant_id,
    policy_version: int,
    backend: str = "iceberg",
    table: str = "prod.users",
    catalog_module: str = ICEBERG_CATALOG_MODULE,
    catalog_options: dict[str, object] | None = None,
    target_options: dict[str, object] | None = None,
) -> None:
    publication_id = uuid4()
    store = PublicationStore(session)
    store.create_cell(cell_id=cell_id, name=f"cell-{cell_id}", region="local")
    store.create_tenant(
        tenant_id=tenant_id,
        slug=f"tenant-{tenant_id}",
        display_name="Default",
    )
    store.assign_tenant_to_cell(cell_id=cell_id, tenant_id=tenant_id, shard_key="default")
    store.insert_publication(
        cell_id=cell_id,
        publication_id=publication_id,
        manifest_hash="b" * 64,
    )
    session.add(
        PublishedCellRuntimeRecord(
            publication_id=publication_id,
            auth_chain_json={"providers": []},
            ticket_json={},
            path_rules_json=[],
        )
    )
    session.add(
        PublishedCatalogRecord(
            publication_id=publication_id,
            tenant_id=tenant_id,
            catalog="analytics",
            config_json={
                "module": catalog_module,
                "options": dict(catalog_options or {}),
            },
        )
    )
    store.insert_published_asset(
        publication_id=publication_id,
        tenant_id=tenant_id,
        catalog="analytics",
        target="default.users",
        backend=backend,
        compiled_config={
            "catalog": {
                "module": ICEBERG_CATALOG_MODULE,
                "options": {},
            },
            "target": {
                "backend": backend,
                "table": table,
                "options": dict(target_options or {}),
            },
            "policy": {
                "rules": [
                    {
                        "principals": ["user1"],
                        "columns": ["id", "email"],
                        "effect": "allow",
                        "when": {},
                        "masks": {"email": {"type": "email"}},
                        "row_filter": "region = 'us'",
                    }
                ]
            },
        },
        policy_version=policy_version,
    )
    store.activate_publication(cell_id=cell_id, publication_id=publication_id)
    session.commit()


class FakePostgresFactory(TableProviderFactory):
    provider_id = "postgres"

    def create(self, descriptor, *, path_enforcer=None):
        del path_enforcer
        assert descriptor.location is None
        assert descriptor.options["dsn"] == "postgresql://example/db"
        assert descriptor.table_identifier == "default.users"
        return FakePostgresTableFormat(
            catalog_name=descriptor.catalog_name,
            table_name=descriptor.requested_target,
            format="postgres",
        )


@dataclass(frozen=True, kw_only=True)
class FakePostgresTableFormat(TableFormat):
    def get_schema(self) -> pa.Schema:
        return pa.schema([pa.field("id", pa.int64())])

    def plan(self, request: PlanRequest, max_tickets: int) -> Plan:
        del request, max_tickets
        schema = self.get_schema()
        return Plan(
            schema=schema,
            tasks=[
                ScanTask(
                    table_format=self,
                    schema=schema,
                    partition=InputPartition(),
                )
            ],
        )

    def execute(self, partition: InputPartition):
        del partition
        return self.get_schema(), iter(())


class FakeCatalog:
    def __init__(self, name: str, options: dict[str, object], path_enforcer=None):
        del path_enforcer
        self._name = name
        self._options = dict(options)

    @property
    def name(self) -> str:
        return self._name

    def describe_table(self, target: str) -> CatalogTableDescriptor:
        return CatalogTableDescriptor(
            catalog_name=self.name,
            requested_target=target,
            provider_id=str(self._options["provider_id"]),
            table_identifier=target,
            options=dict(self._options),
        )

    def list_tables(self) -> list[CatalogTableListing]:
        return []
