"""Real migrated storage for administrative asset command contracts."""

import pytest

from dal_obscura.adapters.providers.governance import ProviderAssetSchemaLoader
from dal_obscura.adapters.sql.database.db import session_factory
from dal_obscura.adapters.sql.unit_of_work import SqlGovernanceUnitOfWorkFactory
from dal_obscura.application.governance.assets.service import AssetsCommands
from dal_obscura.kernel.identity.actor import ControlPlaneActor


@pytest.fixture
def asset_commands(db_engine):
    work_factory = SqlGovernanceUnitOfWorkFactory(session_factory(db_engine))
    with work_factory() as work:
        work.workspace.ensure_workspace()
        work.catalogs.upsert_catalog(name="analytics", plugin_id="iceberg.catalog", options={})
        work.commit()
    commands = AssetsCommands(
        work_factory, schema_loader=ProviderAssetSchemaLoader(), plugin_registry=None
    )
    return commands, work_factory, ControlPlaneActor.for_platform_admin("admin")
