"""Production database evidence for request-scoped configuration isolation."""

from uuid import uuid4

import pytest
from sqlalchemy import event, select

from dal_obscura.common.config_store.orm import (
    AssetRecord,
    AssetSchemaFieldRecord,
    CatalogRecord,
    PolicyRuleRecord,
)
from dal_obscura.data_plane.infrastructure.adapters.live_config import LiveConfigStore
from tests.infrastructure.adapters.test_live_config_store import _seed_live_config
from tests.support.postgres import isolated_postgres_sessions


@pytest.mark.integration
def test_postgres_snapshot_survives_writer_commit_between_configuration_reads():
    with isolated_postgres_sessions() as factory:
        with factory() as session:
            _seed_live_config(session)
        with factory() as session:
            engine = session.get_bind().engine
        store = LiveConfigStore(factory)
        edited = False
        isolation = []

        def concurrent_write(conn, cursor, statement, parameters, context, many):
            nonlocal edited
            if not edited and "FROM assets JOIN catalogs" in statement:
                edited = True
                isolation.append(conn.exec_driver_sql("SHOW transaction_isolation").scalar())
                assert conn.exec_driver_sql("SHOW transaction_read_only").scalar() == "on"
                with factory() as writer:
                    asset = writer.scalar(select(AssetRecord))
                    rule = writer.scalar(select(PolicyRuleRecord))
                    catalog = writer.scalar(select(CatalogRecord))
                    assert asset is not None and rule is not None and catalog is not None
                    writer.add(
                        AssetSchemaFieldRecord(
                            id=uuid4(),
                            asset_id=asset.id,
                            ordinal=1,
                            name="id",
                            field_id="synthetic:id",
                            path_json=["id"],
                            type="int64",
                            nullable=True,
                        )
                    )
                    asset.policy_revision += 1
                    rule.columns_json = ["email"]
                    catalog.revision += 1
                    catalog.options_json = {"uri": "sqlite:///new.db"}
                    writer.commit()

        event.listen(engine, "after_cursor_execute", concurrent_write)
        try:
            asset, catalog = store.get_asset_and_catalog(
                catalog="analytics", target="default.users"
            )
        finally:
            event.remove(engine, "after_cursor_execute", concurrent_write)
        assert isolation == ["repeatable read"]
        assert "schema" not in asset.compiled_config
        assert asset.policy_version == 4
        assert asset.compiled_config["policy"]["rules"][0]["columns"] == ["id"]
        assert catalog.config["options"]["uri"] == "sqlite:///catalog.db"
        next_asset, next_catalog = store.get_asset_and_catalog(
            catalog="analytics", target="default.users"
        )
        assert next_asset.compiled_config["schema"]["fields"][0]["name"] == "id"
        assert next_asset.policy_version == 5
        assert next_asset.compiled_config["policy"]["rules"][0]["columns"] == ["email"]
        assert next_catalog.config["options"]["uri"] == "sqlite:///new.db"


@pytest.mark.integration
@pytest.mark.parametrize("resource", ["policy", "asset", "catalog", "runtime", "auth"])
def test_resource_cas_rejects_stale_session_after_another_writer_commits(resource):
    from dal_obscura.common.config_store.orm import RuntimeSettingsRecord, WorkspaceRecord
    from dal_obscura.control_plane.application.errors import ConfigurationConflictError
    from dal_obscura.control_plane.infrastructure.repositories import ConfigStore

    with isolated_postgres_sessions() as factory:
        with factory() as session:
            (asset_id,) = _seed_live_config(session)

        def mutate(store):
            if resource == "policy":
                store.replace_policy_rules(asset_id=asset_id, rules=[], expected_revision=4)
            elif resource == "asset":
                store.upsert_asset(
                    catalog="analytics",
                    target="default.users",
                    backend="iceberg",
                    table_identifier="prod.changed",
                    options={},
                    expected_revision=2,
                )
            elif resource == "catalog":
                store.upsert_catalog(
                    name="analytics",
                    plugin_id="iceberg.sql",
                    options={"uri": "sqlite:///changed.db"},
                    expected_revision=3,
                )
            elif resource == "runtime":
                store.upsert_runtime_settings(
                    ticket_ttl_seconds=700,
                    max_tickets=12,
                    max_ticket_exchanges=2,
                    expected_revision=1,
                )
            else:
                store.replace_auth_providers(providers=[], expected_revision=0)

        with factory() as stale:
            # Keep ORM identities alive, reproducing reads made before the lock.
            cached = [
                stale.scalar(select(model))
                for model in (AssetRecord, CatalogRecord, RuntimeSettingsRecord, WorkspaceRecord)
            ]
            assert all(record is not None for record in cached)
            with factory() as writer:
                mutate(ConfigStore(writer))
                writer.commit()
            with pytest.raises(ConfigurationConflictError):
                mutate(ConfigStore(stale))
