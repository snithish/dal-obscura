from __future__ import annotations

from uuid import uuid4

import pytest

from dal_obscura.control_plane.application.errors import PublicationConflictError
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


def test_store_persists_qualified_plugin_ids_with_published_asset(db_session):
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
            "catalog": {
                "module": (
                    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry."
                    "IcebergCatalog"
                ),
                "options": {},
            },
            "plugins": {"catalog": "iceberg.sql", "table_format": "iceberg"},
            "target": {"backend": "iceberg", "table": "default.users"},
            "policy": {"rules": []},
        },
        policy_version=1,
    )
    db_session.commit()

    asset = store.get_published_asset(
        publication_id=publication_id,
        tenant_id=tenant_id,
        catalog="analytics",
        target="default.users",
    )

    assert asset.compiled_config["plugins"] == {
        "catalog": "iceberg.sql",
        "table_format": "iceberg",
    }


def test_store_compare_and_swap_activation_rejects_stale_generation(db_session):
    store = PublicationStore(db_session)
    cell_id = uuid4()
    first_publication_id = uuid4()
    second_publication_id = uuid4()
    third_publication_id = uuid4()

    store.create_cell(cell_id=cell_id, name="default", region="local")
    for publication_id in (first_publication_id, second_publication_id, third_publication_id):
        store.insert_publication(
            cell_id=cell_id,
            publication_id=publication_id,
            manifest_hash="a" * 64,
        )
    store.activate_publication(cell_id=cell_id, publication_id=first_publication_id)

    store.activate_publication_if_current(
        cell_id=cell_id,
        publication_id=second_publication_id,
        expected_publication_id=first_publication_id,
    )

    with pytest.raises(PublicationConflictError, match="active generation changed"):
        store.activate_publication_if_current(
            cell_id=cell_id,
            publication_id=third_publication_id,
            expected_publication_id=first_publication_id,
        )

    assert store.get_active_publication(cell_id).publication_id == second_publication_id


def test_catalog_updates_advance_a_revision_only_when_configuration_changes(db_session):
    store = PublicationStore(db_session)
    cell_id = uuid4()
    tenant_id = uuid4()
    store.create_cell(cell_id=cell_id, name="default", region="local")
    store.create_tenant(tenant_id=tenant_id, slug="default", display_name="Default")
    store.assign_tenant_to_cell(cell_id=cell_id, tenant_id=tenant_id, shard_key="default")

    catalog_id = store.upsert_catalog(
        cell_id=cell_id,
        tenant_id=tenant_id,
        name="analytics",
        module="iceberg.sql",
        options={"uri": "sqlite:///catalog.db"},
    )
    assert store.list_catalogs(cell_id)[0]["revision"] == 0

    store.upsert_catalog(
        cell_id=cell_id,
        tenant_id=tenant_id,
        name="analytics",
        module="iceberg.sql",
        options={"uri": "sqlite:///catalog.db"},
    )
    assert store.list_catalogs(cell_id)[0]["revision"] == 0

    assert store.upsert_catalog(
        cell_id=cell_id,
        tenant_id=tenant_id,
        name="analytics",
        module="iceberg.sql",
        options={"uri": "sqlite:///catalog-revised.db"},
    ) == catalog_id
    assert store.list_catalogs(cell_id)[0]["revision"] == 1

    with pytest.raises(PublicationConflictError, match="Catalog revision changed"):
        store.upsert_catalog(
            cell_id=cell_id,
            tenant_id=tenant_id,
            name="analytics",
            module="iceberg.sql",
            options={"uri": "sqlite:///catalog-stale.db"},
            expected_revision=0,
        )


def test_schema_identity_rejects_non_text_and_control_values(db_session):
    store = PublicationStore(db_session)
    cell_id = uuid4()
    tenant_id = uuid4()
    store.create_cell(cell_id=cell_id, name="default", region="local")
    store.create_tenant(tenant_id=tenant_id, slug="default", display_name="Default")
    store.assign_tenant_to_cell(cell_id=cell_id, tenant_id=tenant_id, shard_key="default")
    store.upsert_catalog(
        cell_id=cell_id,
        tenant_id=tenant_id,
        name="analytics",
        module="iceberg.sql",
        options={"type": "sql", "uri": "sqlite:///catalog.db"},
    )
    asset_id = store.upsert_asset(
        cell_id=cell_id,
        tenant_id=tenant_id,
        catalog="analytics",
        target="default.events",
        backend="iceberg",
        table_identifier="default.events",
        options={},
    )

    with pytest.raises(ValueError, match="field id must be text"):
        store.replace_asset_schema_fields(
            asset_id=asset_id,
            fields=[{"name": "id", "field_id": 1}],
        )
    with pytest.raises(ValueError, match="printable"):
        store.replace_asset_schema_fields(
            asset_id=asset_id,
            fields=[{"name": "id", "field_id": "safe\nvalue"}],
        )
    with pytest.raises(ValueError, match="bounded printable"):
        store.replace_asset_schema_fields(
            asset_id=asset_id,
            fields=[{"name": "id", "path": ["id", "x" * 257]}],
        )
