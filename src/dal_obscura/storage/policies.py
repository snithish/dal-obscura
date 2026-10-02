from __future__ import annotations

from typing import Any, Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from dal_obscura.control.errors import (
    ConfigurationConflictError,
)
from dal_obscura.storage.database.orm import (
    DataPlaneTicketRecord,
    PolicyRuleRecord,
    utcnow,
)


def replace_policy_rules(
    session: Session,
    *,
    asset_id: UUID,
    rules: list[dict[str, Any]],
    expected_revision: int,
) -> int:
    from dal_obscura.storage import assets as _assets

    asset = _assets._locked_asset(session, asset_id)
    if asset is None:
        raise LookupError(f"No asset {asset_id}")
    if asset.policy_revision != expected_revision:
        raise ConfigurationConflictError(
            "Policy revision changed "
            f"(expected {expected_revision}, current {asset.policy_revision}); "
            "reread before writing.",
            current_revision=asset.policy_revision,
        )
    normalized_rules = [_normalize_policy_rule(raw) for raw in rules]
    for record in session.scalars(
        select(PolicyRuleRecord).where(PolicyRuleRecord.asset_id == asset_id)
    ):
        session.delete(record)
    session.flush()
    for raw in normalized_rules:
        session.add(
            PolicyRuleRecord(
                id=uuid4(),
                asset_id=asset_id,
                ordinal=int(raw["ordinal"]),
                effect=raw["effect"],
                name=raw.get("name", ""),
                description=raw.get("description", ""),
                principals_json=list(raw.get("principals", [])),
                when_json=dict(raw.get("when", {})),
                columns_json=list(raw.get("columns", [])),
                masks_json=dict(raw.get("masks", {})),
                row_filter_sql=raw.get("row_filter"),
            )
        )
    session.flush()
    asset.policy_revision += 1
    session.flush()
    return asset.policy_revision


def revoke_asset_tickets(session: Session, *, asset_id: UUID) -> int:
    """Revokes every unexpired token issued for an asset for this asset."""
    from dal_obscura.storage import assets as _assets

    asset = _assets._locked_asset(session, asset_id)
    if asset is None:
        raise LookupError(f"No asset {asset_id}")
    result = session.execute(
        update(DataPlaneTicketRecord)
        .where(DataPlaneTicketRecord.revoked_at.is_(None))
        .where(DataPlaneTicketRecord.expires_at >= int(utcnow().timestamp()))
        .where(DataPlaneTicketRecord.asset_id == asset_id)
        .values(revoked_at=utcnow())
    )
    return int(getattr(result, "rowcount", 0) or 0)


def list_policy_rules(session: Session, asset_id: UUID) -> list[dict[str, object]]:
    return [
        {
            "id": str(record.id),
            "asset_id": str(record.asset_id),
            "ordinal": record.ordinal,
            "effect": record.effect,
            "name": record.name,
            "description": record.description,
            "principals": list(record.principals_json),
            "when": dict(record.when_json),
            "columns": list(record.columns_json),
            "masks": dict(record.masks_json),
            "row_filter": record.row_filter_sql,
        }
        for record in session.scalars(
            select(PolicyRuleRecord)
            .where(PolicyRuleRecord.asset_id == asset_id)
            .order_by(PolicyRuleRecord.ordinal)
        )
    ]


def _normalize_policy_rule(raw: dict[str, Any]) -> dict[str, Any]:
    effect = _normalize_policy_rule_effect(str(raw.get("effect", "allow")))
    return {**raw, "effect": effect}


def _normalize_policy_rule_effect(effect: str) -> Literal["allow", "allow_all"]:
    if effect not in {"allow", "allow_all"}:
        raise ValueError("Policy rules are explicit grants")
    return cast(Literal["allow", "allow_all"], effect)
