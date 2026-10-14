from dal_obscura.control.errors import ConfigurationConflictError
from dal_obscura.storage import workspace as _db_workspace


def test_revision_conflict_carries_revision_independent_of_message():
    failure = ConfigurationConflictError("Please reload", current_revision=7)
    assert str(failure) == "Please reload"
    assert failure.current_revision == 7


def test_workspace_summary_uses_bounded_aggregate_queries(db_session):
    from sqlalchemy import event

    store = db_session
    _db_workspace.ensure_workspace(store)
    statements = []
    engine = db_session.get_bind()

    def record(connection, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", record)
    try:
        assert _db_workspace.get_workspace_summary(store)["asset_count"] == 0
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert len(statements) <= 2
