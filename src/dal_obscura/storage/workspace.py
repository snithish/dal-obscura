from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast
from uuid import uuid4

from sqlalchemy import case, exists, func, or_, select
from sqlalchemy.orm import Session

from dal_obscura.control.errors import (
    ConfigurationConflictError,
    RevisionPreconditionRequired,
)
from dal_obscura.storage.database.orm import (
    AssetGrantRecord,
    AssetOwnerRecord,
    AssetRecord,
    AuthProviderRecord,
    CatalogRecord,
    PolicyRuleRecord,
    RuntimeSettingsRecord,
    WorkspaceRecord,
)


def get_workspace(session: Session) -> WorkspaceRecord | None:
    """Return the deployment's singleton configuration record."""
    return session.get(WorkspaceRecord, 1)


def ensure_workspace(session: Session) -> WorkspaceRecord:
    """Initialize the deployment configuration if not yet configured."""
    workspace = get_workspace(
        session,
    )
    if workspace is None:
        workspace = WorkspaceRecord(id=1)
        session.add(workspace)
        session.flush()
    return workspace


def upsert_runtime_settings(
    session: Session,
    *,
    ticket_ttl_seconds: int,
    max_tickets: int,
    max_ticket_exchanges: int,
    path_rules: list[dict[str, Any]] | None = None,
    expected_revision: int | None = None,
) -> None:
    ensure_workspace(
        session,
    )
    existing = session.scalar(
        select(RuntimeSettingsRecord)
        .where(RuntimeSettingsRecord.id == 1)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if existing is None:
        if expected_revision not in (None, 0):
            raise ConfigurationConflictError(
                "Runtime settings revision changed "
                f"(expected {expected_revision}, current 0); reread before writing.",
                current_revision=0,
            )
        session.add(
            RuntimeSettingsRecord(
                revision=1,
                ticket_ttl_seconds=ticket_ttl_seconds,
                max_tickets=max_tickets,
                max_ticket_exchanges=max_ticket_exchanges,
                path_rules_json=[dict(rule) for rule in (path_rules or [])],
                id=1,
            )
        )
    else:
        if expected_revision is None:
            raise RevisionPreconditionRequired(
                "Runtime settings revision is required for updates "
                f"(current {existing.revision}); reread before writing.",
                current_revision=existing.revision,
            )
        if existing.revision != expected_revision:
            raise ConfigurationConflictError(
                "Runtime settings revision changed "
                f"(expected {expected_revision}, current {existing.revision}); "
                "reread before writing.",
                current_revision=existing.revision,
            )
        changed = (
            existing.ticket_ttl_seconds != ticket_ttl_seconds
            or existing.max_tickets != max_tickets
            or existing.max_ticket_exchanges != max_ticket_exchanges
            or existing.path_rules_json != list(path_rules or [])
        )
        existing.ticket_ttl_seconds = ticket_ttl_seconds
        existing.max_tickets = max_tickets
        existing.max_ticket_exchanges = max_ticket_exchanges
        existing.path_rules_json = [dict(rule) for rule in (path_rules or [])]
        if changed:
            existing.revision += 1
    session.flush()


def replace_auth_providers(
    session: Session,
    *,
    providers: list[dict[str, Any]],
    expected_revision: int | None = None,
) -> None:
    lock_auth_providers_for_update(
        session,
    )
    workspace = session.get(WorkspaceRecord, 1)
    if workspace is None:
        raise LookupError("Workspace is not configured")
    existing = list(session.scalars(select(AuthProviderRecord)))
    current_revision = workspace.auth_provider_revision
    if (existing or current_revision > 0) and expected_revision is None:
        raise RevisionPreconditionRequired(
            "Authentication provider revision is required for updates "
            f"(current {current_revision}); reread before writing.",
            current_revision=current_revision,
        )
    if (existing or current_revision > 0) and expected_revision != current_revision:
        raise ConfigurationConflictError(
            "Authentication provider revision changed "
            f"(expected {expected_revision}, current {current_revision}); "
            "reread before writing.",
            current_revision=current_revision,
        )
    existing_args = {record.ordinal: dict(record.args_json) for record in existing}
    new_revision = 0 if not existing and current_revision == 0 else current_revision + 1
    for record in existing:
        session.delete(record)
    session.flush()
    for raw in providers:
        session.add(
            AuthProviderRecord(
                id=uuid4(),
                ordinal=int(raw["ordinal"]),
                module=str(raw["module"]),
                args_json=_preserve_redacted_args(
                    dict(raw.get("args", {})), existing_args.get(int(raw["ordinal"]))
                ),
                enabled=bool(raw.get("enabled", True)),
                revision=new_revision,
            )
        )
    session.flush()
    workspace.auth_provider_revision = new_revision
    session.flush()


def get_auth_provider_revision(session: Session) -> int:
    workspace = get_workspace(
        session,
    )
    return 0 if workspace is None else workspace.auth_provider_revision


def lock_auth_providers_for_update(session: Session) -> None:
    """Serialize authentication-provider collection edits, including first creation."""
    ensure_workspace(
        session,
    )
    record = session.scalar(
        select(WorkspaceRecord)
        .where(WorkspaceRecord.id == 1)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if record is None:
        raise LookupError("Workspace is not configured")


def get_runtime_settings(
    session: Session,
) -> dict[str, object] | None:
    record = session.get(RuntimeSettingsRecord, 1)
    if record is None:
        return None
    return {
        "ticket_ttl_seconds": record.ticket_ttl_seconds,
        "max_tickets": record.max_tickets,
        "max_ticket_exchanges": record.max_ticket_exchanges,
        "path_rules": [dict(rule) for rule in record.path_rules_json],
        "revision": record.revision,
    }


def list_auth_providers(
    session: Session,
) -> list[dict[str, object]]:
    return [
        {
            "id": str(record.id),
            "ordinal": record.ordinal,
            "module": record.module,
            "args": dict(record.args_json),
            "enabled": record.enabled,
            "revision": record.revision,
        }
        for record in session.scalars(
            select(AuthProviderRecord).order_by(AuthProviderRecord.ordinal)
        )
    ]


def get_workspace_summary(
    session: Session, principals: set[str] | None = None
) -> dict[str, object]:
    """Count visible configuration in SQL without materializing listings."""
    owners = exists().where(AssetOwnerRecord.asset_id == AssetRecord.id)
    rules = exists().where(PolicyRuleRecord.asset_id == AssetRecord.id)
    assets = select(
        func.count().label("asset_count"),
        func.count(func.distinct(AssetRecord.catalog_id)).label("visible_catalog_count"),
        func.coalesce(func.sum(case((~owners, 1), else_=0)), 0).label("unowned_asset_count"),
        func.coalesce(func.sum(case((~rules, 1), else_=0)), 0).label("missing_policy_count"),
    ).select_from(AssetRecord)
    if principals is not None:
        owned = exists().where(
            AssetOwnerRecord.asset_id == AssetRecord.id,
            AssetOwnerRecord.principal.in_(principals),
        )
        granted = exists().where(
            AssetGrantRecord.asset_id == AssetRecord.id,
            AssetGrantRecord.principal.in_(principals),
            AssetGrantRecord.capability == "read",
        )
        assets = assets.where(or_(owned, granted))
    counts = assets.subquery()
    row = (
        session.execute(
            select(
                exists().where(WorkspaceRecord.id == 1).label("configured"),
                (
                    select(func.count()).select_from(CatalogRecord).scalar_subquery()
                    if principals is None
                    else counts.c.visible_catalog_count
                ).label("catalog_count"),
                counts.c.asset_count,
                counts.c.unowned_asset_count,
                counts.c.missing_policy_count,
                exists().where(RuntimeSettingsRecord.id == 1).label("runtime_configured"),
                select(func.count())
                .select_from(AuthProviderRecord)
                .where(AuthProviderRecord.enabled.is_(True))
                .scalar_subquery()
                .label("enabled_auth_provider_count"),
            )
        )
        .mappings()
        .one()
    )
    if not row["configured"]:
        return _empty_workspace_summary()
    return {
        "catalog_count": row["catalog_count"],
        "asset_count": row["asset_count"],
        "unowned_asset_count": row["unowned_asset_count"],
        "missing_policy_count": row["missing_policy_count"],
        "runtime_configured": bool(row["runtime_configured"]) if principals is None else False,
        "enabled_auth_provider_count": row["enabled_auth_provider_count"]
        if principals is None
        else 0,
    }


def _empty_workspace_summary() -> dict[str, object]:
    return {
        "catalog_count": 0,
        "asset_count": 0,
        "unowned_asset_count": 0,
        "missing_policy_count": 0,
        "runtime_configured": False,
        "enabled_auth_provider_count": 0,
    }


def _preserve_redacted_args(
    incoming: Mapping[str, object], existing: Mapping[str, object] | None
) -> dict[str, object]:
    """Keep server-held secret values when a redacted GET is round-tripped."""

    if not existing:
        return dict(incoming)
    result: dict[str, object] = {}
    for key, value in incoming.items():
        old = existing.get(key)
        if value == "[redacted]" and old is not None:
            result[key] = old
        elif isinstance(value, Mapping) and isinstance(old, Mapping):
            result[key] = _preserve_redacted_args(
                cast(Mapping[str, object], value), cast(Mapping[str, object], old)
            )
        elif isinstance(value, list) and isinstance(old, list):
            result[key] = [
                _preserve_redacted_args(
                    cast(Mapping[str, object], item), cast(Mapping[str, object], old[index])
                )
                if isinstance(item, Mapping)
                and index < len(old)
                and isinstance(old[index], Mapping)
                else item
                for index, item in enumerate(value)
            ]
        else:
            result[key] = value
    return result
