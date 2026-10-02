from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import String, and_, exists, func, or_, select
from sqlalchemy import cast as sql_cast
from sqlalchemy.orm import Session

from dal_obscura.control.context import current_request_id
from dal_obscura.storage.database.orm import (
    AssetGrantRecord,
    AssetOwnerRecord,
    AssetRecord,
    AuditEventRecord,
)
from dal_obscura.storage.encoding import _isoformat


def record_asset_audit_event(
    session: Session,
    *,
    asset_id: UUID,
    actor_principal: str,
    action: str,
    outcome: str = "success",
    details: dict[str, object] | None = None,
    correlation_id: str | None = None,
) -> None:
    """Records a safe asset-scoped event in the current transaction."""

    asset = session.get(AssetRecord, asset_id)
    if asset is None:
        raise LookupError(f"No asset {asset_id}")
    session.add(
        AuditEventRecord(
            id=uuid4(),
            actor_principal=actor_principal,
            action=action,
            resource_type="asset",
            resource_id=str(asset_id),
            outcome=outcome,
            details_json=dict(details or {}),
            correlation_id=correlation_id or current_request_id(),
        )
    )
    session.flush()


def record_workspace_audit_event(
    session: Session,
    *,
    actor_principal: str,
    action: str,
    resource_type: str,
    resource_id: str,
    details: dict[str, object] | None = None,
) -> None:
    """Records a asset-authorized event for workspace-level operations."""

    session.add(
        AuditEventRecord(
            id=uuid4(),
            actor_principal=actor_principal,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            outcome="success",
            details_json=dict(details or {}),
            correlation_id=current_request_id(),
        )
    )
    session.flush()


def list_audit_events_page(  # noqa: C901
    session: Session,
    *,
    asset_id: UUID | None = None,
    principals: set[str] | None = None,
    actor: str | None = None,
    action: str | None = None,
    resource_type: str | None = None,
    outcome: str | None = None,
    correlation_id: str | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
    limit: int = 100,
    cursor: str | None = None,
) -> AuditEventPage:
    """Returns a bounded, asset-authorized audit page using stable keyset order."""

    if limit <= 0:
        raise ValueError("Audit page limit must be positive")
    query = select(AuditEventRecord)
    if asset_id is not None:
        query = query.where(
            AuditEventRecord.resource_type == "asset",
            AuditEventRecord.resource_id == str(asset_id),
        )
    if actor:
        query = query.where(AuditEventRecord.actor_principal == actor)
    if action:
        query = query.where(AuditEventRecord.action == action)
    if resource_type:
        query = query.where(AuditEventRecord.resource_type == resource_type)
    if outcome:
        query = query.where(AuditEventRecord.outcome == outcome)
    if correlation_id:
        query = query.where(AuditEventRecord.correlation_id == correlation_id)
    if created_after is not None:
        query = query.where(AuditEventRecord.created_at >= created_after)
    if created_before is not None:
        query = query.where(AuditEventRecord.created_at <= created_before)
    if principals is not None:
        if not principals:
            return AuditEventPage(items=[], next_cursor=None)
        visible_asset = exists(
            select(1).where(
                func.replace(sql_cast(AssetRecord.id, String), "-", "")
                == func.replace(AuditEventRecord.resource_id, "-", ""),
                or_(
                    exists(
                        select(1).where(
                            AssetOwnerRecord.asset_id == AssetRecord.id,
                            AssetOwnerRecord.principal.in_(principals),
                        )
                    ),
                    exists(
                        select(1).where(
                            AssetGrantRecord.asset_id == AssetRecord.id,
                            AssetGrantRecord.principal.in_(principals),
                            AssetGrantRecord.capability == "read",
                        )
                    ),
                ),
            )
        )
        query = query.where(AuditEventRecord.resource_type == "asset", visible_asset)
    if cursor:
        created_at, event_id = _decode_audit_cursor(cursor)
        query = query.where(
            or_(
                AuditEventRecord.created_at < created_at,
                and_(AuditEventRecord.created_at == created_at, AuditEventRecord.id < event_id),
            )
        )
    records = list(
        session.scalars(
            query.order_by(AuditEventRecord.created_at.desc(), AuditEventRecord.id.desc()).limit(
                limit + 1
            )
        )
    )
    next_cursor = None
    if len(records) > limit:
        records = records[:limit]
        last = records[-1]
        next_cursor = _encode_audit_cursor(last.created_at, last.id)
    return AuditEventPage(
        items=[
            {
                "id": str(record.id),
                "actor": record.actor_principal,
                "action": record.action,
                "resource_type": record.resource_type,
                "resource_id": record.resource_id,
                "outcome": record.outcome,
                "details": dict(record.details_json),
                "correlation_id": record.correlation_id,
                "created_at": _isoformat(record.created_at),
            }
            for record in records
        ],
        next_cursor=next_cursor,
    )


@dataclass(frozen=True)
class AuditEventPage:
    """Cursor-paginated audit events."""

    items: list[dict[str, object]]
    next_cursor: str | None


def _encode_audit_cursor(created_at: datetime, event_id: UUID) -> str:
    raw = json.dumps(
        {"created_at": _isoformat(created_at), "id": str(event_id)}, separators=(",", ":")
    )
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def _decode_audit_cursor(value: str) -> tuple[datetime, UUID]:
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        data = json.loads(raw)
        return (
            datetime.fromisoformat(str(data["created_at"]).replace("Z", "+00:00")),
            UUID(str(data["id"])),
        )
    except Exception as exc:
        raise ValueError("Invalid audit cursor") from exc
