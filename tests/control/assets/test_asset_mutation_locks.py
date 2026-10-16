"""Asset mutations reject stale revisions without changing published state."""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import UUID

import pytest

from dal_obscura.adapters.providers.governance import ProviderAssetSchemaLoader
from dal_obscura.adapters.sql.unit_of_work import SqlGovernanceUnitOfWorkFactory
from dal_obscura.application.governance.assets.service import AssetsCommands
from dal_obscura.application.governance.errors import ConfigurationConflictError, ValidationFailure
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from tests.support.storage.postgres import isolated_postgres_sessions


@pytest.mark.parametrize("mutation", ["owners", "grants", "schema"])
def test_stale_asset_mutation_preserves_current_owners_and_revision(asset_commands, mutation):
    commands, work_factory, actor = asset_commands
    asset = commands.upsert_workspace_asset(
        "analytics", "events", "iceberg", "default.events", {}, actor=actor, correlation_id=None
    )
    asset_id = UUID(asset["id"])
    commands.replace_asset_owners(asset_id, ["current-owner"], 0, actor=actor, correlation_id=None)
    with work_factory() as work:
        before = work.assets.get_workspace_asset(asset_id)
    with pytest.raises(ConfigurationConflictError, match="revision") as failure:
        if mutation == "owners":
            commands.replace_asset_owners(
                asset_id, ["stale-owner"], 0, actor=actor, correlation_id=None
            )
        elif mutation == "grants":
            commands.replace_asset_grants(
                asset_id,
                [{"principal": "stale-reader", "capability": "read"}],
                0,
                actor=actor,
                correlation_id=None,
            )
        else:
            commands.replace_asset_schema_fields(
                asset_id,
                [{"name": "stale-field", "path": ["stale-field"], "field_id": "1"}],
                0,
                actor=actor,
                correlation_id=None,
            )
    assert failure.value.current_revision == 1
    with work_factory() as work:
        assert work.assets.get_workspace_asset(asset_id) == before
        assert work.assets.list_asset_owners(asset_id) == ["current-owner"]
        assert work.assets.list_asset_grants(asset_id) == []
        assert work.assets.list_asset_schema_fields(asset_id) == []


def test_schema_identity_input_errors_are_safe_validation_failures(asset_commands):
    commands, work_factory, actor = asset_commands
    asset = commands.upsert_workspace_asset(
        "analytics", "events", "iceberg", "default.events", {}, actor=actor, correlation_id=None
    )
    asset_id = UUID(asset["id"])
    with pytest.raises(ValidationFailure, match="field id must be text"):
        commands.replace_asset_schema_fields(
            asset_id,
            [{"name": "id", "path": ["id"], "field_id": 42}],
            0,
            actor=actor,
            correlation_id=None,
        )
    with work_factory() as work:
        assert work.assets.list_asset_schema_fields(asset_id) == []
        assert work.assets.get_workspace_asset(asset_id)["revision"] == 0


@pytest.mark.integration
def test_concurrent_owner_replacements_commit_exactly_one_revision():
    actor = ControlPlaneActor.for_platform_admin("admin")
    with isolated_postgres_sessions() as sessions:
        factory = SqlGovernanceUnitOfWorkFactory(sessions)
        with factory() as work:
            work.workspace.ensure_workspace()
            work.catalogs.upsert_catalog(name="analytics", plugin_id="iceberg.catalog", options={})
            asset_id = work.assets.upsert_asset(
                catalog="analytics",
                target="events",
                backend="iceberg",
                table_identifier="default.events",
                options={},
            )
            work.commit()
        commands = AssetsCommands(
            factory, schema_loader=ProviderAssetSchemaLoader(), plugin_registry=None
        )
        ready = Barrier(2)

        def replace(owner):
            with factory() as work:
                revision = work.assets.get_workspace_asset(asset_id)["revision"]
            assert isinstance(revision, int)
            ready.wait(timeout=10)
            try:
                commands.replace_asset_owners(
                    asset_id, [owner], revision, actor=actor, correlation_id=owner
                )
                return owner
            except ConfigurationConflictError:
                return None

        with ThreadPoolExecutor(max_workers=2) as workers:
            outcomes = list(workers.map(replace, ["owner-a", "owner-b"]))
        winners = [owner for owner in outcomes if owner is not None]
        assert len(winners) == 1
        with factory() as work:
            assert work.assets.list_asset_owners(asset_id) == winners
            assert work.assets.get_workspace_asset(asset_id)["revision"] == 1
