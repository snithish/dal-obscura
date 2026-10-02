"""Canonical configuration records for adapter and integration scenarios."""

from uuid import uuid4

from sqlalchemy.orm import Session

from dal_obscura.common.config_store.orm import (
    AssetRecord,
    AuthProviderRecord,
    CatalogRecord,
    PolicyRuleRecord,
    RuntimeSettingsRecord,
    WorkspaceRecord,
)

ICEBERG_CATALOG_ID = "iceberg.sql"


def seed_live_config(session: Session):
    catalog_id = uuid4()
    asset_id = uuid4()
    session.add_all(
        [
            WorkspaceRecord(id=1),
            RuntimeSettingsRecord(
                ticket_ttl_seconds=600,
                max_tickets=12,
                max_ticket_exchanges=2,
                revision=1,
                path_rules_json=[],
                id=1,
            ),
            CatalogRecord(
                id=catalog_id,
                name="analytics",
                plugin_id=ICEBERG_CATALOG_ID,
                options_json={"type": "sql", "uri": "sqlite:///catalog.db"},
                revision=3,
            ),
            AssetRecord(
                id=asset_id,
                catalog_id=catalog_id,
                target="default.users",
                backend="iceberg",
                table_identifier="prod.users",
                options_json={},
                revision=2,
                policy_revision=4,
            ),
            PolicyRuleRecord(
                id=uuid4(),
                asset_id=asset_id,
                ordinal=10,
                effect="allow",
                principals_json=["user1"],
                when_json={},
                columns_json=["id"],
                masks_json={},
                row_filter_sql=None,
            ),
            AuthProviderRecord(
                id=uuid4(),
                ordinal=1,
                module="example.IdentityProvider",
                args_json={"issuer": "https://issuer.example"},
                enabled=True,
                revision=1,
            ),
        ]
    )
    session.flush([record for record in session.new if isinstance(record, CatalogRecord)])
    session.flush([record for record in session.new if isinstance(record, AssetRecord)])
    session.commit()
    return (asset_id,)
