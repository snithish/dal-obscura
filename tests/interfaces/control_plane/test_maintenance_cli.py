from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.common.config_store.db import (
    create_engine_from_url,
    migrate_config_store,
    session_factory,
)
from dal_obscura.common.config_store.orm import CellRecord
from dal_obscura.common.ticket_delivery.models import TicketPayload
from dal_obscura.control_plane.application.access import ControlPlaneActor
from dal_obscura.control_plane.infrastructure.session_store import (
    BrowserSessionStore,
    LoginTransactionStore,
)
from dal_obscura.control_plane.interfaces.maintenance_cli import invalidate_access
from dal_obscura.data_plane.infrastructure.adapters.ticket_store_sqlalchemy import (
    SqlAlchemyTicketStore,
)


def _session_maker(tmp_path) -> sessionmaker[Session]:
    engine = create_engine_from_url(f"sqlite+pysqlite:///{tmp_path / 'control.db'}")
    migrate_config_store(engine)
    return session_factory(engine)


def test_invalidate_access_revokes_sessions_and_replayable_artifacts(tmp_path):
    session_maker = _session_maker(tmp_path)
    cell_id = uuid4()
    with session_maker() as session:
        session.add(CellRecord(id=cell_id, name="cell", region="local"))
        actor = ControlPlaneActor(principal="user:alice", groups=())
        browser_token, _csrf = BrowserSessionStore(session).issue_with_csrf(
            actor,
            ttl_seconds=3600,
        )
        LoginTransactionStore(session).issue(
            state="state",
            nonce="nonce",
            code_verifier="verifier",
            redirect_uri="http://localhost/callback",
        )
        session.commit()

    ticket_id = "00000000-0000-0000-0000-000000000001"
    SqlAlchemyTicketStore(session_maker, cell_id=cell_id).store(
        TicketPayload(
            asset_id="00000000-0000-4000-8000-000000000001",
            ticket_id=ticket_id,
            catalog="analytics",
            target="default.users",
            tenant_id="default",
            columns=["id"],
            scan={
                "authorization_columns": ["id", "region"],
                "read_payload": "payload",
                "full_row_filter": None,
                "masks": {},
            },
            policy_version=1,
            principal_id="user:alice",
            expires_at=2_000_000_000,
            nonce="nonce",
        ),
        max_exchanges=1,
    )

    counts = invalidate_access(session_maker, cell_id=cell_id)

    assert counts.sessions == 1
    assert counts.login_transactions == 1
    assert counts.tickets == 1
    with session_maker() as session:
        assert BrowserSessionStore(session).resolve(browser_token) is None
    with pytest.raises(LookupError):
        SqlAlchemyTicketStore(session_maker, cell_id=cell_id).load(ticket_id)
