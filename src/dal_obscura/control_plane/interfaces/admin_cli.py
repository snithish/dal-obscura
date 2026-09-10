"""Minimal operator commands for inspecting governed gateway publications."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

from dal_obscura.common.access_control.compiled_policy import CompiledPolicy
from dal_obscura.common.access_control.models import Principal
from dal_obscura.common.access_control.policy_resolution import resolve_access
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
    if args.command == "preview":
        return _preview(args.manifest, args.personas)
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


def _preview(manifest_path: str, personas_path: str) -> int:
    """Evaluates caller-supplied personas offline; this never authenticates callers."""

    try:
        compiled = compile_manifest(load_manifest(Path(manifest_path)))
        personas = _load_personas(Path(personas_path))
        assets = {(asset.catalog, asset.target): asset for asset in compiled.assets}
        results = [_preview_persona(persona, assets) for persona in personas]
    except (ManifestValidationError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps({"results": results}, sort_keys=True))
    return 0


def _load_personas(path: Path) -> list[dict[str, object]]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid personas file: {exc}") from exc
    if not isinstance(value, list):
        raise ValueError("personas must be a JSON list")
    if len(value) > 1000:
        raise ValueError("personas exceeds 1000 entry limit")
    if not all(isinstance(item, dict) for item in value):
        raise ValueError("each persona must be an object")
    return [dict(item) for item in value]


def _preview_persona(persona: dict[str, object], assets) -> dict[str, object]:
    required = {"id", "groups", "attributes", "catalog", "target", "columns"}
    if set(persona) != required:
        raise ValueError("persona has missing or unsupported fields")
    identifier = _text(persona["id"], "persona.id")
    groups = _text_list(persona["groups"], "persona.groups")
    attributes = _text_mapping(persona["attributes"], "persona.attributes")
    catalog = _text(persona["catalog"], "persona.catalog")
    target = _text(persona["target"], "persona.target")
    columns = _text_list(persona["columns"], "persona.columns")
    asset = assets.get((catalog, target))
    if asset is None:
        return {"id": identifier, "status": "denied"}
    policy = CompiledPolicy.from_json(
        asset.compiled_config["policy"],
        version=asset.policy_version,
        catalog=catalog,
        target=target,
    ).to_policy()
    try:
        allowed, _, _ = resolve_access(
            policy,
            Principal(id=identifier, groups=groups, attributes=attributes),
            target,
            catalog,
            columns,
        )
    except PermissionError:
        return {"id": identifier, "status": "denied"}
    return {"id": identifier, "status": "allowed", "allowed_columns": allowed}


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be non-empty text")
    return value


def _text_list(value: object, label: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise ValueError(f"{label} must be a list of non-empty text")
    return cast(list[str], value)


def _text_mapping(value: object, label: str) -> dict[str, str]:
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise ValueError(f"{label} must map text to text")
    return cast(dict[str, str], value)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dal-obscura-admin")
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate", help="validate an Iceberg gateway JSON manifest")
    validate.add_argument("manifest", help="path to a JSON manifest")
    preview = subparsers.add_parser("preview", help="evaluate caller-supplied personas offline")
    preview.add_argument("manifest", help="path to a JSON manifest")
    preview.add_argument("--personas", required=True, help="path to a JSON persona list")
    publish = subparsers.add_parser("publish", help="activate a compiled manifest with CAS")
    publish.add_argument("manifest", help="path to a JSON manifest")
    publish.add_argument("--database-url", help="SQLAlchemy database URL")
    publish.add_argument(
        "--expected-generation", required=True, help="currently active publication UUID"
    )
    status = subparsers.add_parser("status", help="show the active publication generation")
    status.add_argument("--database-url", help="SQLAlchemy database URL")
    return parser
