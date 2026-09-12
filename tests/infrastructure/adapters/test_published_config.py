from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import pyarrow as pa
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.common.config_store.orm import (
    PublishedAssetRecord,
    PublishedCatalogRecord,
    PublishedCellRuntimeRecord,
)
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore
from dal_obscura.data_plane.infrastructure.adapters.published_config import (
    PublishedAsset,
    PublishedConfigAuthorizer,
    PublishedConfigStore,
    _validate_schema_admission,
)

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)


@pytest.fixture
def db_session() -> Iterator[Session]:
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
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
    assert decision.policy_version != 123


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
    assert decision.policy_version != 123


def test_published_store_loads_asset_and_catalog_from_one_generation(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    store = PublishedConfigStore(db_session, cell_id=cell_id)

    asset, catalog = store.get_asset_and_catalog(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )

    assert asset.publication_id == catalog.publication_id
    assert asset.catalog == catalog.catalog == "analytics"


def test_published_store_fails_closed_by_default_after_transient_failure(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    config_store = PublishedConfigStore(db_session, cell_id=cell_id)

    config_store.get_asset(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )

    def fail_get(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(db_session, "get", fail_get)

    with pytest.raises(RuntimeError, match="database unavailable"):
        config_store.get_asset(
            tenant_id=str(tenant_id),
            catalog="analytics",
            target="default.users",
        )


def test_published_store_rejects_assets_removed_by_new_generation(
    db_session: Session,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    config_store = PublishedConfigStore(db_session, cell_id=cell_id)

    config_store.get_asset(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )
    removed_asset_publication_id = uuid4()
    store = PublicationStore(db_session)
    store.insert_publication(
        cell_id=cell_id,
        publication_id=removed_asset_publication_id,
        manifest_hash="c" * 64,
    )
    db_session.add(
        PublishedCellRuntimeRecord(
            publication_id=removed_asset_publication_id,
            auth_chain_json={"providers": []},
            ticket_json={},
            path_rules_json=[],
        )
    )
    store.activate_publication(cell_id=cell_id, publication_id=removed_asset_publication_id)
    db_session.commit()

    with pytest.raises(LookupError, match="No published asset"):
        config_store.get_asset(
            tenant_id=str(tenant_id),
            catalog="analytics",
            target="default.users",
        )
    assert config_store._asset_cache == {}


def test_published_authorizer_rejects_corrupt_mask_instead_of_dropping_it(
    db_session: Session,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    record = db_session.scalar(select(PublishedAssetRecord))
    assert record is not None
    record.compiled_config_json["policy"]["rules"][0]["masks"] = {"email": {}}
    db_session.commit()
    authorizer = PublishedConfigAuthorizer(PublishedConfigStore(db_session, cell_id=cell_id))

    with pytest.raises(ValueError, match=r"mask\.type"):
        authorizer.authorize(
            principal=Principal(id="user1", groups=[], attributes={"tenant_id": str(tenant_id)}),
            target="default.users",
            catalog="analytics",
            requested_columns=["email"],
        )


def test_published_schema_admission_rejects_rebound_or_added_field():
    asset = PublishedAsset(
        publication_id=uuid4(),
        tenant_id=uuid4(),
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "schema": {
                "encoding": 1,
                "fields": [
                    {
                        "name": "profile.email",
                        "field_id": "iceberg:3",
                        "path": ["profile", "email"],
                        "type": "string",
                        "nullable": True,
                    }
                ],
            }
        },
        policy_version=1,
    )
    schema = pa.schema(
        [
            pa.field(
                "profile",
                pa.struct(
                    [
                        pa.field(
                            "email",
                            pa.string(),
                            metadata={b"PARQUET:field_id": b"iceberg:99"},
                        )
                    ]
                ),
                metadata={b"PARQUET:field_id": b"iceberg:2"},
            )
        ]
    )

    with pytest.raises(ValueError, match="no longer matches"):
        _validate_schema_admission(asset, schema)


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


def test_published_authorizer_changes_effective_version_for_new_generation(db_session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    authorizer = PublishedConfigAuthorizer(PublishedConfigStore(db_session, cell_id=cell_id))
    principal = Principal(id="user1", groups=[], attributes={"tenant_id": str(tenant_id)})
    initial_version = authorizer.authorize(
        principal=principal,
        target="default.users",
        catalog="analytics",
        requested_columns=["id"],
    ).policy_version
    asset = db_session.scalar(select(PublishedAssetRecord))
    catalog = db_session.scalar(select(PublishedCatalogRecord))
    runtime = db_session.scalar(select(PublishedCellRuntimeRecord))
    assert asset is not None and catalog is not None and runtime is not None

    new_publication_id = uuid4()
    store = PublicationStore(db_session)
    store.insert_publication(
        cell_id=cell_id,
        publication_id=new_publication_id,
        manifest_hash="d" * 64,
    )
    db_session.add(
        PublishedCellRuntimeRecord(
            publication_id=new_publication_id,
            auth_chain_json=dict(runtime.auth_chain_json),
            ticket_json=dict(runtime.ticket_json),
            path_rules_json=list(runtime.path_rules_json),
        )
    )
    db_session.add(
        PublishedCatalogRecord(
            publication_id=new_publication_id,
            tenant_id=tenant_id,
            catalog=catalog.catalog,
            config_json=dict(catalog.config_json),
        )
    )
    store.insert_published_asset(
        publication_id=new_publication_id,
        tenant_id=tenant_id,
        catalog=asset.catalog,
        target=asset.target,
        backend=asset.backend,
        compiled_config=dict(asset.compiled_config_json),
        policy_version=asset.policy_version,
    )
    store.activate_publication(cell_id=cell_id, publication_id=new_publication_id)
    db_session.commit()

    current_version = authorizer.current_policy_version(
        "default.users", "analytics", tenant_id=str(tenant_id)
    )
    assert current_version != initial_version
