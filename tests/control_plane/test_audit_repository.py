from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from dal_obscura.common.config_store.orm import (
    AssetOwnerRecord,
    AssetRecord,
    AuditEventRecord,
    CatalogRecord,
)
from dal_obscura.control_plane.infrastructure.repositories import ConfigStore


def test_scoped_audit_visibility_enforces_cell_and_tenant_context(db_session) -> None:
    store = ConfigStore(db_session)
    cell_id = uuid4()
    visible_tenant_id = uuid4()
    hidden_tenant_id = uuid4()
    visible_asset_id = uuid4()
    hidden_asset_id = uuid4()
    store.create_cell(cell_id=cell_id, name="default", region="local")
    store.create_tenant(tenant_id=visible_tenant_id, slug="a-visible", display_name="Visible")
    store.create_tenant(tenant_id=hidden_tenant_id, slug="b-hidden", display_name="Hidden")
    store.assign_tenant_to_cell(cell_id=cell_id, tenant_id=visible_tenant_id, shard_key="visible")
    store.assign_tenant_to_cell(cell_id=cell_id, tenant_id=hidden_tenant_id, shard_key="hidden")
    db_session.add_all(
        [
            CatalogRecord(
                id=uuid4(),
                cell_id=cell_id,
                tenant_id=visible_tenant_id,
                name="visible-catalog",
                plugin_id="plugin.catalog",
                options_json={},
            ),
            CatalogRecord(
                id=uuid4(),
                cell_id=cell_id,
                tenant_id=hidden_tenant_id,
                name="hidden-catalog",
                plugin_id="plugin.catalog",
                options_json={},
            ),
        ]
    )
    db_session.flush()
    catalogs = db_session.query(CatalogRecord).order_by(CatalogRecord.name).all()
    db_session.add_all(
        [
            AssetRecord(
                id=visible_asset_id,
                cell_id=cell_id,
                tenant_id=visible_tenant_id,
                catalog_id=catalogs[1].id,
                target="visible.events",
                backend="parquet",
                options_json={},
            ),
            AssetRecord(
                id=hidden_asset_id,
                cell_id=cell_id,
                tenant_id=hidden_tenant_id,
                catalog_id=catalogs[0].id,
                target="hidden.events",
                backend="parquet",
                options_json={},
            ),
            AssetOwnerRecord(
                id=uuid4(),
                asset_id=visible_asset_id,
                ordinal=0,
                principal="shared-owner",
            ),
            AssetOwnerRecord(
                id=uuid4(),
                asset_id=hidden_asset_id,
                ordinal=0,
                principal="shared-owner",
            ),
        ]
    )
    now = datetime.now(timezone.utc)
    db_session.add_all(
        [
            AuditEventRecord(
                id=uuid4(),
                cell_id=cell_id,
                tenant_id=visible_tenant_id,
                actor_principal="shared-owner",
                action="visible.action",
                resource_type="asset",
                resource_id=str(visible_asset_id),
                outcome="success",
                details_json={},
                created_at=now,
            ),
            AuditEventRecord(
                id=uuid4(),
                cell_id=cell_id,
                tenant_id=hidden_tenant_id,
                actor_principal="shared-owner",
                action="hidden.action",
                resource_type="asset",
                resource_id=str(hidden_asset_id),
                outcome="success",
                details_json={},
                created_at=now,
            ),
        ]
    )
    db_session.flush()

    context = store.get_default_workspace_context()
    assert context is not None
    page = store.list_audit_events_page(context, principals={"shared-owner"}, limit=50)

    assert [item["action"] for item in page.items] == ["visible.action"]
