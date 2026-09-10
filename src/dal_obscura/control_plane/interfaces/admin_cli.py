"""Minimal operator commands for inspecting governed gateway publications."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from uuid import UUID, uuid4

from dal_obscura.common.config_store.db import (
    ConfigStoreSchemaError,
    check_config_store_schema,
    create_engine_from_url,
    session_factory,
)
from dal_obscura.control_plane.application.errors import PublicationConflictError
from dal_obscura.control_plane.application.operator_manifest import (
    ManifestValidationError,
    compile_manifest,
    load_manifest,
)
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore


def main() -> None:
    """Runs the ``dal-obscura-admin`` console script."""

    raise SystemExit(run())


def run(argv: Sequence[str] | None = None) -> int:
    """Runs a non-web operator command and returns its process exit code."""

    parser = _parser()
    args = parser.parse_args(argv)
    if args.command == "validate":
        return _validate(args.manifest)
    database_url = args.database_url or os.getenv("DAL_OBSCURA_DATABASE_URL")
    if not database_url:
        print(
            "DAL_OBSCURA_DATABASE_URL is required unless --database-url is set",
            file=sys.stderr,
        )
        return 2
    engine = create_engine_from_url(database_url)
    try:
        check_config_store_schema(engine)
    except ConfigStoreSchemaError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    if args.command == "status":
        _print_status(engine)
        return 0
    if args.command == "publish":
        return _publish(engine, args.manifest, args.expected_generation)
    parser.error(f"unsupported command {args.command!r}")
    return 2


def _print_status(engine) -> None:
    with session_factory(engine)() as session:
        store = PublicationStore(session)
        context = store.get_default_workspace_context()
        if context is None:
            payload = {"workspace": None, "active_publication": None}
        else:
            try:
                active = store.get_active_publication_summary(context.cell_id)
            except LookupError:
                active = None
            payload = {
                "workspace": {
                    "cell_id": str(context.cell_id),
                    "tenant_id": str(context.tenant_id),
                },
                "active_publication": active,
            }
    print(json.dumps(payload, sort_keys=True))


def _validate(path: str) -> int:
    try:
        compiled = compile_manifest(load_manifest(Path(path)))
    except ManifestValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "asset_count": len(compiled.assets),
                "catalog_count": len(compiled.catalogs),
                "manifest_hash": compiled.manifest_hash,
            },
            sort_keys=True,
        )
    )
    return 0


def _publish(engine, path: str, expected_generation: str) -> int:
    try:
        compiled = compile_manifest(load_manifest(Path(path)))
        expected_id = UUID(expected_generation)
    except (ManifestValidationError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    publication_id = uuid4()
    try:
        with session_factory(engine)() as session, session.begin():
            store = PublicationStore(session)
            store.insert_compiled_publication(publication_id=publication_id, compiled=compiled)
            store.activate_publication_if_current(
                cell_id=compiled.cell_id,
                publication_id=publication_id,
                expected_publication_id=expected_id,
            )
    except (LookupError, PublicationConflictError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "manifest_hash": compiled.manifest_hash,
                "previous_generation": str(expected_id),
                "publication_id": str(publication_id),
            },
            sort_keys=True,
        )
    )
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dal-obscura-admin")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate", help="validate an Iceberg gateway JSON manifest")
    validate.add_argument("manifest", help="path to a JSON manifest")
    publish = subparsers.add_parser("publish", help="activate a compiled manifest with CAS")
    publish.add_argument("manifest", help="path to a JSON manifest")
    publish.add_argument("--database-url", help="SQLAlchemy database URL")
    publish.add_argument(
        "--expected-generation", required=True, help="currently active publication UUID"
    )
    status = subparsers.add_parser("status", help="show the active publication generation")
    status.add_argument("--database-url", help="SQLAlchemy database URL")
    return parser
