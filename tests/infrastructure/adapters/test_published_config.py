from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import Base
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore
from dal_obscura.data_plane.infrastructure.adapters.published_config import (
    PublishedConfigAuthorizer,
    PublishedConfigStore,
)

ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)


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


def _publish_asset(
    session: Session,
    *,
    cell_id,
    tenant_id,
    policy_version: int,
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
    store.insert_published_asset(
        publication_id=publication_id,
        tenant_id=tenant_id,
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "catalog": {
                "module": ICEBERG_CATALOG_MODULE,
                "options": {},
            },
            "target": {"backend": "iceberg", "table": "prod.users"},
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
