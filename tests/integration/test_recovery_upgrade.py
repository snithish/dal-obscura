"""Recovery and upgrade acceptance probes.

The PostgreSQL case is intentionally opt-in: it must run against a disposable
operator-provided database rather than silently substituting SQLite.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
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
from dal_obscura.control_plane.infrastructure.session_store import BrowserSessionStore
from dal_obscura.control_plane.interfaces.maintenance_cli import invalidate_access
from dal_obscura.data_plane.infrastructure.adapters.ticket_hmac import HmacTicketCodecAdapter
from dal_obscura.data_plane.infrastructure.adapters.ticket_store_sqlalchemy import (
    SqlAlchemyTicketStore,
)

pytestmark = pytest.mark.integration

_FIXTURE = Path(__file__).parents[1] / "acceptance" / "fixtures" / "ticket_payload_v1.json"


def test_trusted_ticket_fixture_survives_bounded_key_rotation() -> None:
    """Old trusted ticket payloads remain verifiable during planned overlap."""

    payload = TicketPayload.from_dict(json.loads(_FIXTURE.read_text()))
    old = HmacTicketCodecAdapter("o" * 32)
    rotated = HmacTicketCodecAdapter("n" * 32, previous_secrets=("o" * 32,))

    old_ticket = old.sign_payload(payload)
    assert rotated.verify(old_ticket).ticket_id == payload.ticket_id


def test_restore_helper_requires_explicit_isolated_confirmation() -> None:
    """A restore command cannot proceed toward a live database by omission."""

    script = Path(__file__).parents[2] / "scripts" / "restore_postgres.sh"
    result = subprocess.run(
        ["sh", str(script), "/tmp/missing-backup.age"],
        env={
            **os.environ,
            "DAL_OBSCURA_DATABASE_URL": "postgresql+psycopg://unused",
            "DAL_OBSCURA_AGE_IDENTITY": "/tmp/missing-identity",
            "DAL_OBSCURA_RESTORE_CONFIRM": "NO",
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert "I_UNDERSTAND_ISOLATED_RESTORE" in result.stderr


@pytest.fixture()
def postgres_sessions() -> sessionmaker[Session]:
    database_url = os.getenv("DAL_OBSCURA_POSTGRES_TEST_URL", "").strip()
    if not database_url:
        pytest.skip("set DAL_OBSCURA_POSTGRES_TEST_URL for the PostgreSQL recovery drill")
    if not database_url.startswith(("postgresql://", "postgresql+")):
        pytest.fail("DAL_OBSCURA_POSTGRES_TEST_URL must point to PostgreSQL")
    engine = create_engine_from_url(database_url)
    migrate_config_store(engine)
    return session_factory(engine)


def test_postgres_restore_invalidation_removes_replayable_access(
    postgres_sessions: sessionmaker[Session],
) -> None:
    """The post-restore invalidation command revokes sessions and tickets."""

    cell_id = uuid4()
    with postgres_sessions() as session:
        session.add(CellRecord(id=cell_id, name=f"recovery-{cell_id.hex}", region="test"))
        browser_token, _ = BrowserSessionStore(session).issue_with_csrf(
            ControlPlaneActor(principal="user:recovery", groups=()),
            ttl_seconds=3600,
        )
        session.commit()

    ticket_id = str(uuid4())
    SqlAlchemyTicketStore(postgres_sessions, cell_id=cell_id).store(
        TicketPayload(
            ticket_id=ticket_id,
            catalog="recovery",
            target="default.table",
            tenant_id="default",
            columns=["id"],
            scan={"read_payload": "payload", "full_row_filter": None, "masks": {}},
            policy_version=1,
            principal_id="user:recovery",
            expires_at=2_000_000_000,
            nonce="recovery",
        ),
        max_exchanges=1,
    )

    counts = invalidate_access(postgres_sessions, cell_id=cell_id)
    assert counts.sessions >= 1
    assert counts.tickets >= 1
    with postgres_sessions() as session:
        assert BrowserSessionStore(session).resolve(browser_token) is None
    with pytest.raises(LookupError):
        SqlAlchemyTicketStore(postgres_sessions, cell_id=cell_id).load(ticket_id)
