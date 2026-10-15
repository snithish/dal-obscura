from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import event, select

from dal_obscura.adapters.duckdb.compiler import DuckDBProjectionCompiler
from dal_obscura.adapters.duckdb.transform import DuckDBRowTransformAdapter
from dal_obscura.adapters.sql.database.db import session_factory
from dal_obscura.adapters.sql.database.orm import AuditEventRecord, DataPlaneTicketRecord
from dal_obscura.adapters.sql.unit_of_work import SqlGovernanceUnitOfWorkFactory
from dal_obscura.application.governance.errors import ConfigurationConflictError
from dal_obscura.application.governance.policies.service import PoliciesCommands
from dal_obscura.kernel.identity.actor import ControlPlaneActor
from tests.support.control.workspace import _provision_asset


@pytest.mark.parametrize("failure", ["audit", "stale"])
def test_failed_policy_command_preserves_policy_ticket_and_audit(
    client_factory, db_engine, failure
):
    client = client_factory()
    asset_id = UUID(_provision_asset(client)["id"])
    factory = session_factory(db_engine)
    work_factory = SqlGovernanceUnitOfWorkFactory(factory)
    ticket_id = uuid4()
    with factory() as session:
        session.add(
            DataPlaneTicketRecord(
                ticket_id=ticket_id,
                asset_id=asset_id,
                target="default.users",
                principal_id="user1",
                policy_version=1,
                expires_at=int(datetime.now(UTC).timestamp()) + 900,
                max_exchanges=2,
                payload_hash="0" * 64,
                payload_json={},
            )
        )
        session.commit()
        audit_before = session.scalars(select(AuditEventRecord.id)).all()
    with work_factory() as work:
        policy_before = work.policies.list_policy_rules(asset_id)
        revision_before = work.assets.get_workspace_asset(asset_id)["policy_revision"]

    assert isinstance(revision_before, int)

    def fail_audit(conn, cursor, statement, parameters, context, many):
        if statement.lstrip().lower().startswith("insert into audit_events"):
            raise RuntimeError("audit unavailable")

    commands = PoliciesCommands(
        work_factory,
        schema_loader=Mock(),
        plugin_registry=None,
        compiler=DuckDBProjectionCompiler(),
        transform=DuckDBRowTransformAdapter(),
        identity_mapper=Mock(),
    )
    if failure == "audit":
        event.listen(db_engine, "before_cursor_execute", fail_audit)
    try:
        error = RuntimeError if failure == "audit" else ConfigurationConflictError
        with pytest.raises(error, match="audit unavailable" if failure == "audit" else "revision"):
            commands.replace_asset_policy(
                asset_id,
                [],
                expected_revision=revision_before if failure == "audit" else revision_before - 1,
                revoke_existing_tokens=True,
                actor=ControlPlaneActor.for_platform_admin("platform:admin"),
                correlation_id="transaction-check",
            )
    finally:
        if failure == "audit":
            event.remove(db_engine, "before_cursor_execute", fail_audit)
    with work_factory() as work:
        assert work.policies.list_policy_rules(asset_id) == policy_before
        assert work.assets.get_workspace_asset(asset_id)["policy_revision"] == revision_before
    with factory() as session:
        ticket = session.get(DataPlaneTicketRecord, ticket_id)
        assert ticket is not None
        assert ticket.revoked_at is None
        assert session.scalars(select(AuditEventRecord.id)).all() == audit_before
