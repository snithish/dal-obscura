from __future__ import annotations

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
    AssetRecord,
    AuthProviderRecord,
    CatalogRecord,
    CellRecord,
    CellRuntimeSettingsRecord,
    CellTenantRecord,
    PolicyRuleRecord,
    TenantRecord,
)
from dal_obscura.data_plane.infrastructure.adapters.live_config import (
    LiveConfigAuthorizer,
    LiveConfigStore,
)

ICEBERG_CATALOG_ID = "iceberg.sql"


@pytest.fixture
def db_session():
    engine = create_engine_from_url("sqlite+pysqlite:///:memory:")
    migrate_config_store(engine)
    with session_factory(engine)() as session:
        yield session


def test_live_store_reads_current_policy_from_canonical_records(db_session: Session):
    cell_id, tenant_id, asset_id = _seed_live_config(db_session)
    store = LiveConfigStore(db_session, cell_id=cell_id)
    authorizer = LiveConfigAuthorizer(store)
    principal = Principal(id="user1", groups=[], attributes={"tenant_id": str(tenant_id)})

    first = authorizer.authorize(principal, "default.users", "analytics", ["id"])
    assert first.allowed_columns == ["id"]
    assert first.asset_id == str(asset_id)

    rule = db_session.query(PolicyRuleRecord).filter_by(asset_id=asset_id).one()
    rule.columns_json = ["email"]
    asset = db_session.get(AssetRecord, asset_id)
    assert asset is not None
    asset.policy_revision += 1
    cell = db_session.get(CellRecord, cell_id)
    assert cell is not None
    cell.configuration_revision += 1
    db_session.commit()

    second = authorizer.authorize(principal, "default.users", "analytics", ["email"])
    assert second.allowed_columns == ["email"]
    assert second.policy_version != first.policy_version


def test_live_runtime_settings_and_catalogs_need_no_snapshot_table(db_session: Session):
    cell_id, tenant_id, _ = _seed_live_config(db_session)
    store = LiveConfigStore(db_session, cell_id=cell_id)

    runtime = store.get_runtime()
    asset, catalog = store.get_asset_and_catalog(
        tenant_id=str(tenant_id), catalog="analytics", target="default.users"
    )

    assert runtime.ticket == {"ttl_seconds": 600, "max_tickets": 12, "max_exchanges": 2}
    assert runtime.auth_chain["providers"] == [
        {
            "ordinal": 1,
            "module": "example.IdentityProvider",
            "args": {"issuer": "https://issuer.example"},
            "enabled": True,
        }
    ]
    assert asset.config_revision == catalog.config_revision
    assert catalog.config["plugin_id"] == "iceberg.sql"
    assert catalog.config["revision"] == 3


def test_live_store_invalidates_asset_cache_after_direct_catalog_edit(db_session: Session):
    cell_id, tenant_id, _ = _seed_live_config(db_session)
    store = LiveConfigStore(db_session, cell_id=cell_id)
    first = store.get_asset(tenant_id=str(tenant_id), catalog="analytics", target="default.users")

    catalog = db_session.query(CatalogRecord).filter_by(name="analytics").one()
    catalog.options_json = {"type": "sql", "uri": "sqlite:///changed.db"}
    catalog.revision += 1
    cell = db_session.get(CellRecord, cell_id)
    assert cell is not None
    cell.configuration_revision += 1
    db_session.commit()

    second = store.get_asset(tenant_id=str(tenant_id), catalog="analytics", target="default.users")
    assert second.config_revision != first.config_revision
    assert second.compiled_config["catalog"]["options"]["uri"] == "sqlite:///changed.db"


def _seed_live_config(session: Session):
    cell_id = uuid4()
    tenant_id = uuid4()
    catalog_id = uuid4()
    asset_id = uuid4()
    session.add_all(
        [
            CellRecord(id=cell_id, name=f"cell-{cell_id}", region="local"),
            TenantRecord(id=tenant_id, slug=f"tenant-{tenant_id}", display_name="Test"),
            CellTenantRecord(cell_id=cell_id, tenant_id=tenant_id, shard_key="default"),
            CellRuntimeSettingsRecord(
                cell_id=cell_id,
                ticket_ttl_seconds=600,
                max_tickets=12,
                max_ticket_exchanges=2,
                revision=1,
                path_rules_json=[],
            ),
            CatalogRecord(
                id=catalog_id,
                cell_id=cell_id,
                tenant_id=tenant_id,
                name="analytics",
                plugin_id=ICEBERG_CATALOG_ID,
                options_json={"type": "sql", "uri": "sqlite:///catalog.db"},
                revision=3,
            ),
            AssetRecord(
                id=asset_id,
                cell_id=cell_id,
                tenant_id=tenant_id,
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
                cell_id=cell_id,
                ordinal=1,
                module="example.IdentityProvider",
                args_json={"issuer": "https://issuer.example"},
                enabled=True,
                revision=1,
            ),
        ]
    )
    session.commit()
    return cell_id, tenant_id, asset_id
