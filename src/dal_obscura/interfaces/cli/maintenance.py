"""Operational access invalidation for restore and emergency recovery."""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

from dal_obscura.bootstrap.maintenance import invalidate_database_access


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
        print("DAL_OBSCURA_DATABASE_URL is required unless --database-url is set", file=sys.stderr)
        return 2
    try:
        counts = invalidate_database_access(database_url)
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
        "invalidate-access", help="revoke sessions and remove replayable login/ticket artifacts"
    )
    invalidate.add_argument("--database-url", help="SQLAlchemy database URL")
    return parser
