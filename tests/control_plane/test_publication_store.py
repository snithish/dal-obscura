from __future__ import annotations

from uuid import uuid4

from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def test_store_reads_active_publication_and_one_published_asset(db_session):
    store = PublicationStore(db_session)
    cell_id = uuid4()
    tenant_id = uuid4()
    publication_id = uuid4()

    store.create_cell(cell_id=cell_id, name="default", region="local")
    store.create_tenant(tenant_id=tenant_id, slug="default", display_name="Default")
    store.assign_tenant_to_cell(cell_id=cell_id, tenant_id=tenant_id, shard_key="default")
    store.insert_publication(cell_id=cell_id, publication_id=publication_id, manifest_hash="a" * 64)
    store.insert_published_asset(
        publication_id=publication_id,
        tenant_id=tenant_id,
        catalog="analytics",
        target="default.users",
        backend="iceberg",
        compiled_config={
            "catalog": {"module": "module.Catalog", "options": {"type": "sql"}},
            "target": {"backend": "iceberg", "table": "prod.users"},
            "policy": {"rules": []},
        },
        policy_version=123,
    )
    store.insert_published_asset(
        publication_id=publication_id,
        tenant_id=tenant_id,
        catalog="analytics",
        target="default.orders",
        backend="iceberg",
        compiled_config={
            "catalog": {"module": "module.Catalog", "options": {"type": "sql"}},
            "target": {"backend": "iceberg", "table": "prod.orders"},
            "policy": {"rules": []},
        },
        policy_version=456,
    )
    store.activate_publication(cell_id=cell_id, publication_id=publication_id)

    active = store.get_active_publication(cell_id)
    asset = store.get_published_asset(
        publication_id=active.publication_id,
        tenant_id=tenant_id,
        catalog="analytics",
        target="default.users",
    )

    assert active.publication_id == publication_id
    assert asset.target == "default.users"
    assert asset.policy_version == 123
