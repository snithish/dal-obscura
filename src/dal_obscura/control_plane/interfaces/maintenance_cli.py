"""Operational access invalidation for restore and emergency recovery.

Example:
    ```bash
    dal-obscura-maintenance invalidate-access --database-url "$DAL_OBSCURA_DATABASE_URL"
    ```
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast
from uuid import UUID

from sqlalchemy import delete, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session, sessionmaker

from dal_obscura.common.config_store.db import create_engine_from_url, session_factory
from dal_obscura.common.config_store.orm import (
    BrowserSessionRecord,
    DataPlaneTicketRecord,
    LoginTransactionRecord,
    utcnow,
)


@dataclass(frozen=True)
class InvalidationCounts:
    """Rows invalidated by one restore/emergency operation."""

    sessions: int
    login_transactions: int
    tickets: int


def invalidate_access(
    session_maker: sessionmaker[Session],
    *,
    cell_id: UUID | None = None,
) -> InvalidationCounts:
    """Revokes browser credentials and removes replayable access artifacts."""

    now = utcnow()
    with session_maker() as session:
        session_count = (
            cast(
                CursorResult,
                session.execute(
                    update(BrowserSessionRecord)
                    .where(BrowserSessionRecord.revoked_at.is_(None))
                    .values(revoked_at=now)
                ),
            ).rowcount
            or 0
        )
        login_count = (
            cast(CursorResult, session.execute(delete(LoginTransactionRecord))).rowcount or 0
        )
        ticket_delete = delete(DataPlaneTicketRecord)
        if cell_id is not None:
            ticket_delete = ticket_delete.where(DataPlaneTicketRecord.cell_id == cell_id)
        ticket_count = cast(CursorResult, session.execute(ticket_delete)).rowcount or 0
        session.commit()
    return InvalidationCounts(
        sessions=int(session_count),
        login_transactions=int(login_count),
        tickets=int(ticket_count),
    )


def main() -> None:
    raise SystemExit(run())


def run(
    argv: Sequence[str] | None = None,
    environment: dict[str, str] | None = None,
) -> int:
    args = _parser().parse_args(argv)
    values = os.environ if environment is None else environment
    database_url = args.database_url or values.get("DAL_OBSCURA_DATABASE_URL", "").strip()
    if not database_url:
        print(
            "DAL_OBSCURA_DATABASE_URL is required unless --database-url is set",
            file=sys.stderr,
        )
        return 2
    try:
        cell_id = UUID(args.cell_id) if args.cell_id else None
        counts = invalidate_access(
            session_factory(create_engine_from_url(database_url)),
            cell_id=cell_id,
        )
    except (ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        "invalidated access artifacts: "
        f"sessions={counts.sessions} "
        f"login_transactions={counts.login_transactions} "
        f"tickets={counts.tickets}"
    )
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dal-obscura-maintenance")
    subparsers = parser.add_subparsers(dest="command", required=True)
    invalidate = subparsers.add_parser(
        "invalidate-access",
        help="revoke sessions and remove replayable login/ticket artifacts",
    )
    invalidate.add_argument("--database-url", help="SQLAlchemy database URL")
    invalidate.add_argument(
        "--cell-id",
        help="only delete Flight tickets for this cell (sessions remain global)",
    )
    return parser
