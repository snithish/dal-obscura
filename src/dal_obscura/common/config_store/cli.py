"""Command-line entry point for explicit config-store migrations.

Example:
    ```python
    exit_code = run(["check", "--database-url", "sqlite+pysqlite:///:memory:"])
    ```
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence

from alembic.runtime.migration import MigrationContext
from sqlalchemy.orm import Session

from dal_obscura.common.config_store.db import (
    ConfigStoreMigrationRequired,
    check_config_store_schema,
    create_engine_from_url,
    migrate_config_store,
)
from dal_obscura.common.config_store.plugin_bindings import (
    apply_plugin_bindings,
    inspect_plugin_bindings,
)


def main() -> None:
    """Runs the `dal-obscura-migrate` console script.

    Example:
        ```python
        main()
        ```
    """

    raise SystemExit(run())


def run(argv: Sequence[str] | None = None) -> int:
    """Runs a config-store migration command and returns a process exit code.

    Example:
        ```python
        code = run(["current", "--database-url", database_url])
        ```
    """

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
    if args.command == "plugin-bindings":
        with Session(engine) as session:
            if args.apply:
                with session.begin():
                    report = inspect_plugin_bindings(session)
                    applied = apply_plugin_bindings(session, report)
            else:
                report = inspect_plugin_bindings(session)
                applied = 0
        payload = report.to_dict()
        payload["applied"] = applied
        print(json.dumps(payload, sort_keys=True, separators=(",", ":")))
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

    bindings = subparsers.add_parser(
        "plugin-bindings",
        help="report or explicitly populate exact built-in plugin identities",
    )
    bindings.add_argument("--database-url", help="SQLAlchemy database URL")
    bindings.add_argument(
        "--apply",
        action="store_true",
        help="apply only exact built-in mappings from the dry-run report",
    )

    return parser
