from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Sequence

from alembic.runtime.migration import MigrationContext

from dal_obscura.common.config_store.db import (
    ConfigStoreMigrationRequired,
    check_config_store_schema,
    create_engine_from_url,
    migrate_config_store,
)


def main() -> None:
    raise SystemExit(run())


def run(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    database_url = args.database_url or os.getenv("DAL_OBSCURA_DATABASE_URL")
    if not database_url:
        print(
            "DAL_OBSCURA_DATABASE_URL is required unless --database-url is set",
            file=sys.stderr,
        )
        return 2

    engine = create_engine_from_url(database_url)
    if args.command == "upgrade":
        migrate_config_store(engine, revision=args.revision)
        print(f"config-store schema upgraded to {args.revision}")
        return 0
    if args.command == "check":
        try:
            check_config_store_schema(engine)
        except ConfigStoreMigrationRequired as exc:
            print(str(exc), file=sys.stderr)
            return 1
        print("config-store schema is current")
        return 0
    if args.command == "current":
        with engine.connect() as connection:
            context = MigrationContext.configure(connection)
            heads = tuple(sorted(context.get_current_heads()))
        print(", ".join(heads) if heads else "none")
        return 0
    parser.error(f"unsupported command {args.command!r}")
    return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dal-obscura-migrate")
    subparsers = parser.add_subparsers(dest="command", required=True)

    upgrade = subparsers.add_parser("upgrade", help="apply config-store migrations")
    upgrade.add_argument("--database-url", help="SQLAlchemy database URL")
    upgrade.add_argument("--revision", default="head", help="Alembic target revision")

    check = subparsers.add_parser("check", help="verify schema is at packaged head")
    check.add_argument("--database-url", help="SQLAlchemy database URL")

    current = subparsers.add_parser("current", help="print current database revision")
    current.add_argument("--database-url", help="SQLAlchemy database URL")

    return parser
