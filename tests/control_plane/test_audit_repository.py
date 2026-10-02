from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from dal_obscura.storage import audit as _db_audit
from dal_obscura.storage import workspace as _db_workspace
from dal_obscura.storage.database.orm import (
    AssetOwnerRecord,
    AssetRecord,
    AuditEventRecord,
    CatalogRecord,
)


def test_audit_visibility_enforces_asset_ownership(db_session) -> None:
    store = db_session

    visible_asset_id = uuid4()
    hidden_asset_id = uuid4()
    _db_workspace.ensure_workspace(store)
    db_session.add_all(
        [
            CatalogRecord(
                id=uuid4(), name="visible-catalog", plugin_id="plugin.catalog", options_json={}
            ),
            CatalogRecord(
                id=uuid4(), name="hidden-catalog", plugin_id="plugin.catalog", options_json={}
            ),
        ]
    )
    db_session.flush()
    catalogs = db_session.query(CatalogRecord).order_by(CatalogRecord.name).all()
    db_session.add_all(
        [
            AssetRecord(
                id=visible_asset_id,
                catalog_id=catalogs[1].id,
                target="visible.events",
                backend="parquet",
                options_json={},
            ),
            AssetRecord(
                id=hidden_asset_id,
                catalog_id=catalogs[0].id,
                target="hidden.events",
                backend="parquet",
                options_json={},
            ),
            AssetOwnerRecord(
                id=uuid4(), asset_id=visible_asset_id, ordinal=0, principal="shared-owner"
            ),
            AssetOwnerRecord(
                id=uuid4(), asset_id=hidden_asset_id, ordinal=0, principal="other-owner"
            ),
        ]
    )
    now = datetime.now(timezone.utc)
    db_session.add_all(
        [
            AuditEventRecord(
                id=uuid4(),
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

    context = _db_workspace.get_workspace(store)
    assert context is not None
    page = _db_audit.list_audit_events_page(store, principals={"shared-owner"}, limit=50)

    assert [item["action"] for item in page.items] == ["visible.action"]
