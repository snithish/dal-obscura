from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.common.config_store.orm import (
    PublishedCatalogRecord,
    PublishedCellRuntimeRecord,
)
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


def test_published_store_uses_last_good_asset_when_stale_mode_is_enabled(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    now = 100.0
    config_store = PublishedConfigStore(
        db_session,
        cell_id=cell_id,
        allow_stale_seconds=60,
        clock=lambda: now,
    )

    first = config_store.get_asset(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )

    def fail_get(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(db_session, "get", fail_get)
    second = config_store.get_asset(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )

    assert second == first


def test_published_store_rejects_expired_last_good_asset(
    db_session: Session,
    monkeypatch: pytest.MonkeyPatch,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    now = 100.0
    config_store = PublishedConfigStore(
        db_session,
        cell_id=cell_id,
        allow_stale_seconds=60,
        clock=lambda: now,
    )

    config_store.get_asset(
        tenant_id=str(tenant_id),
        catalog="analytics",
        target="default.users",
    )

    def fail_get(*args, **kwargs):
        del args, kwargs
        raise RuntimeError("database unavailable")

    now = 161.0
    monkeypatch.setattr(db_session, "get", fail_get)

    with pytest.raises(RuntimeError, match="database unavailable"):
        config_store.get_asset(
            tenant_id=str(tenant_id),
            catalog="analytics",
            target="default.users",
        )


def test_published_store_does_not_use_last_good_when_asset_is_removed(
    db_session: Session,
):
    cell_id = uuid4()
    tenant_id = uuid4()
    _publish_asset(db_session, cell_id=cell_id, tenant_id=tenant_id, policy_version=123)
    config_store = PublishedConfigStore(
        db_session,
        cell_id=cell_id,
        allow_stale_seconds=60,
    )

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
