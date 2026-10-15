from contextlib import nullcontext

import pytest
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.adapters.sql.database.db import session_factory
from dal_obscura.adapters.sql.unit_of_work import SqlGovernanceUnitOfWorkFactory
from dal_obscura.application.governance.ports import GovernanceUnitOfWorkFactory


def test_governance_unit_of_work_commits_repository_writes(db_engine):
    factory: GovernanceUnitOfWorkFactory = SqlGovernanceUnitOfWorkFactory(
        session_factory(db_engine)
    )
    with factory() as work:
        work.workspace.ensure_workspace()
        work.workspace.upsert_runtime_settings(
            ticket_ttl_seconds=123, max_tickets=4, max_ticket_exchanges=2
        )
        work.commit()

    with factory() as work:
        assert work.workspace.get_runtime_settings() == {
            "ticket_ttl_seconds": 123,
            "max_tickets": 4,
            "max_ticket_exchanges": 2,
            "path_rules": [],
            "revision": 1,
        }


@pytest.mark.parametrize("body_failure", [False, True], ids=["no-commit", "body-failure"])
def test_governance_unit_of_work_rolls_back_uncommitted_writes(db_engine, body_failure):
    factory = SqlGovernanceUnitOfWorkFactory(session_factory(db_engine))
    with (
        pytest.raises(RuntimeError, match="command failed") if body_failure else nullcontext(),
        factory() as work,
    ):
        work.workspace.ensure_workspace()
        if body_failure:
            raise RuntimeError("command failed")

    with factory() as work:
        assert work.workspace.get_workspace() is None


@pytest.mark.parametrize("close_failure", [False, True], ids=["close", "close-failure"])
def test_governance_unit_of_work_closes_session_after_commit_failure(db_engine, close_failure):
    sessions = []

    class FailingSession(Session):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.close_count = 0
            sessions.append(self)

        def commit(self):
            raise RuntimeError("commit failed")

        def close(self):
            self.close_count += 1
            super().close()
            if close_failure:
                raise RuntimeError("close failed")

    factory = SqlGovernanceUnitOfWorkFactory(sessionmaker(bind=db_engine, class_=FailingSession))
    with pytest.raises(RuntimeError, match="commit failed"), factory() as work:
        work.workspace.ensure_workspace()
        work.commit()

    assert sessions[0].close_count == 1
    with SqlGovernanceUnitOfWorkFactory(session_factory(db_engine))() as work:
        assert work.workspace.get_workspace() is None


def test_repository_cannot_reopen_session_after_unit_of_work_exit(db_engine):
    factory = SqlGovernanceUnitOfWorkFactory(session_factory(db_engine))
    with factory() as work:
        repository = work.workspace
    with pytest.raises(RuntimeError, match="not active"):
        repository.get_workspace()
