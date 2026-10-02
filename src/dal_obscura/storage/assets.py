from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any, cast
from uuid import UUID, uuid4

from sqlalchemy import and_, or_, select, tuple_
from sqlalchemy.orm import Session

from dal_obscura.control.errors import (
    ConfigurationConflictError,
    RevisionPreconditionRequired,
)
from dal_obscura.policy.schema_identity import canonical_provider_field_id
from dal_obscura.storage.database.orm import (
    AssetGrantRecord,
    AssetOwnerRecord,
    AssetRecord,
    AssetSchemaFieldRecord,
    CatalogRecord,
    PolicyRuleRecord,
)
from dal_obscura.storage.encoding import _escape_like


def upsert_asset(
    session: Session,
    *,
    catalog: str,
    target: str,
    backend: str,
    table_identifier: str | None,
    options: dict[str, Any],
    expected_revision: int | None = None,
) -> UUID:
    from dal_obscura.storage import catalogs as _catalogs

    catalog_record = _catalogs._catalog_by_name(session, name=catalog)
    existing = session.scalar(
        select(AssetRecord)
        .where(AssetRecord.catalog_id == catalog_record.id, AssetRecord.target == target)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if existing is None:
        _assert_new_asset_revision(expected_revision)
        asset_id = uuid4()
        session.add(
            AssetRecord(
                id=asset_id,
                catalog_id=catalog_record.id,
                target=target,
                backend=backend,
                table_identifier=table_identifier,
                options_json=options,
            )
        )
    else:
        _assert_asset_revision(existing, expected_revision)
        asset_id = existing.id
        existing.backend = backend
        existing.table_identifier = table_identifier
        existing.options_json = options
        existing.revision += 1
    session.flush()
    return asset_id


def replace_asset_owners(
    session: Session,
    *,
    asset_id: UUID,
    owners: list[str],
    expected_revision: int | None = None,
) -> list[str]:
    asset = _locked_asset(session, asset_id)
    if asset is None:
        raise LookupError(f"No asset {asset_id}")
    _assert_asset_revision(asset, expected_revision)
    normalized = _normalize_principals(owners)
    for record in session.scalars(
        select(AssetOwnerRecord).where(AssetOwnerRecord.asset_id == asset_id)
    ):
        session.delete(record)
    session.flush()
    for ordinal, principal in enumerate(normalized, start=1):
        session.add(
            AssetOwnerRecord(id=uuid4(), asset_id=asset_id, ordinal=ordinal, principal=principal)
        )
    session.flush()
    asset.revision += 1
    session.flush()
    return normalized


def replace_asset_schema_fields(
    session: Session,
    *,
    asset_id: UUID,
    fields: list[dict[str, Any]],
    expected_revision: int | None = None,
) -> list[dict[str, object]]:
    asset = _locked_asset(session, asset_id)
    if asset is None:
        raise LookupError(f"No asset {asset_id}")
    _assert_asset_revision(asset, expected_revision)
    normalized = _normalize_schema_fields(fields)
    for record in session.scalars(
        select(AssetSchemaFieldRecord).where(AssetSchemaFieldRecord.asset_id == asset_id)
    ):
        session.delete(record)
    session.flush()
    for ordinal, field in enumerate(normalized, start=1):
        session.add(
            AssetSchemaFieldRecord(
                id=uuid4(),
                asset_id=asset_id,
                ordinal=ordinal,
                name=str(field["name"]),
                field_id=str(field["field_id"]),
                path_json=list(cast(list[str], field["path"])),
                type=str(field["type"]),
                nullable=bool(field["nullable"]),
            )
        )
    session.flush()
    asset.revision += 1
    session.flush()
    return normalized


def list_assets(
    session: Session,
) -> list[dict[str, object]]:
    catalog_records = list(session.scalars(select(CatalogRecord)))
    catalog_by_id = {record.id: record for record in catalog_records}
    assets = []
    for record in session.scalars(select(AssetRecord).order_by(AssetRecord.target)):
        catalog = catalog_by_id[record.catalog_id]
        assets.append(
            {
                "id": str(record.id),
                "catalog_id": str(record.catalog_id),
                "catalog": catalog.name,
                "target": record.target,
                "backend": record.backend,
                "table_identifier": record.table_identifier,
                "options": dict(record.options_json),
            }
        )
    return assets


def list_workspace_assets(
    session: Session,
) -> list[dict[str, object]]:
    records = list(
        session.scalars(select(AssetRecord).order_by(AssetRecord.target, AssetRecord.id))
    )
    return _workspace_asset_rows(session, records)


def list_workspace_assets_for_principals(
    session: Session,
    principals: set[str],
) -> list[dict[str, object]]:
    """Lists only assets owned by one of the supplied actor principals."""

    if not principals:
        return []
    records = list(
        session.scalars(
            select(AssetRecord)
            .outerjoin(AssetOwnerRecord, AssetOwnerRecord.asset_id == AssetRecord.id)
            .outerjoin(AssetGrantRecord, AssetGrantRecord.asset_id == AssetRecord.id)
            .where(
                or_(
                    AssetOwnerRecord.principal.in_(principals),
                    and_(
                        AssetGrantRecord.principal.in_(principals),
                        AssetGrantRecord.capability == "read",
                    ),
                )
            )
            .distinct()
            .order_by(AssetRecord.target, AssetRecord.id)
        )
    )
    return _workspace_asset_rows(session, records)


def list_workspace_assets_page(
    session: Session,
    *,
    limit: int,
    cursor: str | None = None,
    search: str | None = None,
    principals: set[str] | None = None,
) -> AssetPage:
    if limit <= 0:
        raise ValueError("Asset page limit must be positive")
    normalized_search = search.strip() if search else ""
    after = _decode_asset_cursor(cursor, expected_search=normalized_search) if cursor else None
    query = select(AssetRecord)
    if principals is not None:
        if not principals:
            return AssetPage(items=[], next_cursor=None)
        query = query.outerjoin(AssetOwnerRecord, AssetOwnerRecord.asset_id == AssetRecord.id)
        query = query.outerjoin(AssetGrantRecord, AssetGrantRecord.asset_id == AssetRecord.id)
        query = query.where(
            or_(
                AssetOwnerRecord.principal.in_(principals),
                and_(
                    AssetGrantRecord.principal.in_(principals),
                    AssetGrantRecord.capability == "read",
                ),
            )
        ).distinct()
    if normalized_search:
        escaped_search = _escape_like(normalized_search)
        pattern = f"%{escaped_search}%"
        query = query.where(
            or_(
                AssetRecord.target.ilike(pattern, escape="\\"),
                AssetRecord.table_identifier.ilike(pattern, escape="\\"),
                AssetRecord.backend.ilike(pattern, escape="\\"),
            )
        )
    if after is not None:
        query = query.where(tuple_(AssetRecord.target, AssetRecord.id) > after)
    records = list(
        session.scalars(query.order_by(AssetRecord.target, AssetRecord.id).limit(limit + 1))
    )
    next_cursor = None
    if len(records) > limit:
        records = records[:limit]
        next_cursor = _encode_asset_cursor(records[-1], search=normalized_search)
    return AssetPage(items=_workspace_asset_rows(session, records), next_cursor=next_cursor)


def get_workspace_asset(session: Session, asset_id: UUID) -> dict[str, object]:
    from dal_obscura.storage import policies as _policies

    record = session.get(AssetRecord, asset_id)
    if record is None:
        raise LookupError(f"No asset {asset_id}")
    catalog = session.get(CatalogRecord, record.catalog_id)
    if catalog is None:
        raise LookupError(f"No catalog {record.catalog_id}")
    return {
        **_workspace_asset_row(session, record, catalog),
        "revision": record.revision,
        "policy_revision": record.policy_revision,
        "options": dict(record.options_json),
        "schema_fields": list_asset_schema_fields(session, asset_id),
        "policy_rules": _policies.list_policy_rules(session, asset_id),
    }


def lock_asset_for_update(session: Session, asset_id: UUID) -> None:
    """Serializes live asset mutations for the duration of the transaction."""

    record = session.scalar(
        select(AssetRecord)
        .where(AssetRecord.id == asset_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if record is None:
        raise LookupError(f"No asset {asset_id}")


def _locked_asset(session: Session, asset_id: UUID) -> AssetRecord | None:
    return session.scalar(
        select(AssetRecord)
        .where(AssetRecord.id == asset_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )


def list_asset_owners(session: Session, asset_id: UUID) -> list[str]:
    return [
        record.principal
        for record in session.scalars(
            select(AssetOwnerRecord)
            .where(AssetOwnerRecord.asset_id == asset_id)
            .order_by(AssetOwnerRecord.ordinal)
        )
    ]


def list_asset_grants(session: Session, asset_id: UUID) -> list[dict[str, str]]:
    return [
        {
            "id": str(record.id),
            "asset_id": str(record.asset_id),
            "principal": record.principal,
            "capability": record.capability,
        }
        for record in session.scalars(
            select(AssetGrantRecord)
            .where(AssetGrantRecord.asset_id == asset_id)
            .order_by(AssetGrantRecord.principal, AssetGrantRecord.capability)
        )
    ]


def replace_asset_grants(
    session: Session,
    *,
    asset_id: UUID,
    grants: list[dict[str, str]],
    expected_revision: int | None = None,
) -> list[dict[str, str]]:
    asset = _locked_asset(session, asset_id)
    if asset is None:
        raise LookupError(f"No asset {asset_id}")
    _assert_asset_revision(asset, expected_revision)
    for record in session.scalars(
        select(AssetGrantRecord).where(AssetGrantRecord.asset_id == asset_id)
    ):
        session.delete(record)
    session.flush()
    asset.revision += 1
    session.flush()
    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for raw in grants:
        principal = str(raw["principal"]).strip()
        capability = str(raw["capability"]).strip()
        key = (principal, capability)
        if not principal or not capability or key in seen:
            continue
        seen.add(key)
        session.add(
            AssetGrantRecord(
                id=uuid4(), asset_id=asset_id, principal=principal, capability=capability
            )
        )
        normalized.append({"principal": principal, "capability": capability})
    session.flush()
    return normalized


def list_asset_schema_fields(session: Session, asset_id: UUID) -> list[dict[str, object]]:
    return [
        {
            "name": record.name,
            "field_id": record.field_id,
            "path": list(record.path_json),
            "type": record.type,
            "nullable": record.nullable,
        }
        for record in session.scalars(
            select(AssetSchemaFieldRecord)
            .where(AssetSchemaFieldRecord.asset_id == asset_id)
            .order_by(AssetSchemaFieldRecord.ordinal)
        )
    ]


def _workspace_asset_row(
    session: Session,
    record: AssetRecord,
    catalog: CatalogRecord,
) -> dict[str, object]:
    return _workspace_asset_rows(session, [record], catalog_by_id={catalog.id: catalog})[0]


def _workspace_asset_rows(
    session: Session,
    records: list[AssetRecord],
    *,
    catalog_by_id: dict[UUID, CatalogRecord] | None = None,
) -> list[dict[str, object]]:
    if not records:
        return []
    if catalog_by_id is None:
        catalog_ids = {record.catalog_id for record in records}
        catalog_by_id = {
            record.id: record
            for record in session.scalars(
                select(CatalogRecord).where(CatalogRecord.id.in_(catalog_ids))
            )
        }
    asset_ids = [record.id for record in records]
    owners_by_asset: dict[UUID, list[str]] = {asset_id: [] for asset_id in asset_ids}
    for asset_id, principal in session.execute(
        select(AssetOwnerRecord.asset_id, AssetOwnerRecord.principal)
        .where(AssetOwnerRecord.asset_id.in_(asset_ids))
        .order_by(AssetOwnerRecord.asset_id, AssetOwnerRecord.ordinal)
    ):
        owners_by_asset.setdefault(asset_id, []).append(principal)
    assets_with_rules = {
        asset_id
        for (asset_id,) in session.execute(
            select(PolicyRuleRecord.asset_id)
            .where(PolicyRuleRecord.asset_id.in_(asset_ids))
            .group_by(PolicyRuleRecord.asset_id)
        )
    }
    rows = []
    for record in records:
        catalog = catalog_by_id[record.catalog_id]
        owners = owners_by_asset.get(record.id, [])
        rows.append(
            {
                "id": str(record.id),
                "name": record.target,
                "catalog": catalog.name,
                "backend": record.backend,
                "table_identifier": record.table_identifier,
                "owner_count": len(owners),
                "owners": owners,
                "policy_status": "configured" if record.id in assets_with_rules else "missing",
                "policy_revision": record.policy_revision,
            }
        )
    return rows


@dataclass(frozen=True)
class AssetPage:
    """Cursor-paginated asset listing.

    Example:
        ```python
        page = store.list_assets()
        ```
    """

    items: list[dict[str, object]]
    next_cursor: str | None


def _assert_asset_revision(asset: AssetRecord, expected_revision: int | None) -> None:
    """Rejects stale metadata/binding writes after the asset row is locked."""

    if expected_revision is None:
        raise RevisionPreconditionRequired(
            "Asset revision is required for updates "
            f"(current {asset.revision}); reread before writing.",
            current_revision=asset.revision,
        )
    if asset.revision != expected_revision:
        raise ConfigurationConflictError(
            "Asset revision changed "
            f"(expected {expected_revision}, current {asset.revision}); reread before writing.",
            current_revision=asset.revision,
        )


def _assert_new_asset_revision(expected_revision: int | None) -> None:
    """Treat creation as a compare-and-set against the implicit revision zero."""

    if expected_revision is not None and expected_revision != 0:
        raise ConfigurationConflictError(
            "Asset revision changed "
            f"(expected {expected_revision}, current 0); reread before writing.",
            current_revision=0,
        )


def _encode_asset_cursor(record: AssetRecord, *, search: str = "") -> str:
    raw = json.dumps(
        {"target": record.target, "id": str(record.id), "search": search}, separators=(",", ":")
    )
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii").rstrip("=")


def _decode_asset_cursor(value: str, *, expected_search: str = "") -> tuple[str, UUID]:
    try:
        padded = value + "=" * (-len(value) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
        data = json.loads(raw)
        if str(data.get("search", "")) != expected_search:
            raise ValueError("Asset cursor does not match the requested search")
        return str(data["target"]), UUID(str(data["id"]))
    except Exception as exc:
        raise ValueError("Invalid asset cursor") from exc


def _normalize_principals(principals: list[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for principal in principals:
        value = principal.strip()
        if value and value not in seen:
            normalized.append(value)
            seen.add(value)
    return normalized


def _normalize_schema_fields(fields: list[dict[str, Any]]) -> list[dict[str, object]]:
    """Normalize schema identities without coercing unsafe caller values.

    These values are persisted as asset schema admission metadata and later used
    for schema-drift checks. Accept only bounded printable strings so direct
    service callers cannot bypass the HTTP model's limits.
    """

    normalized: list[dict[str, object]] = []
    seen_paths: set[tuple[str, ...]] = set()
    seen_ids: set[str] = set()
    for field in fields:
        raw_name = field.get("name")
        if not isinstance(raw_name, str):
            raise ValueError("Schema field name must be text")
        name = raw_name.strip()
        if not name:
            raise ValueError("Schema field name must be non-empty")
        _validate_schema_text(name, "Schema field name", max_length=256)
        raw_path = field.get("path")
        if (
            not isinstance(raw_path, list)
            or not raw_path
            or any(
                not isinstance(segment, str)
                or not segment.strip()
                or len(segment.strip()) > 256
                or any(ord(char) < 0x20 or ord(char) == 0x7F for char in segment)
                for segment in raw_path
            )
        ):
            raise ValueError("Schema field path must contain bounded printable text segments")
        path = [segment.strip() for segment in raw_path]
        path_key = tuple(path)
        if path_key in seen_paths:
            raise ValueError("Schema field paths must be unique")
        raw_field_id = field.get("field_id")
        if not isinstance(raw_field_id, str):
            raise ValueError("Schema field id must be text")
        field_id = raw_field_id.strip()
        _validate_schema_text(field_id, "Schema field id", max_length=128)
        if field_id.startswith("legacy:"):
            raise ValueError("Legacy schema field ids are unsupported")
        if not field_id.startswith("synthetic:"):
            field_id = canonical_provider_field_id(field_id)
        if not field_id:
            raise ValueError("Schema field id must be non-empty")
        if field_id in seen_ids:
            raise ValueError("Schema field ids must be unique")
        data_type, nullable = _schema_field_type(field)
        normalized.append(
            {
                "name": name,
                "field_id": field_id,
                "path": path,
                "type": data_type.strip(),
                "nullable": nullable,
            }
        )
        seen_paths.add(path_key)
        seen_ids.add(field_id)
    return normalized


def _schema_field_type(field: dict[str, Any]) -> tuple[str, bool]:
    data_type = field.get("type", "string")
    if not isinstance(data_type, str) or not data_type.strip():
        raise ValueError("Schema field type must be non-empty text")
    _validate_schema_text(data_type, "Schema field type", max_length=2 * 1024 * 1024)
    nullable = field.get("nullable", True)
    if type(nullable) is not bool:
        raise ValueError("Schema field nullability must be a boolean")
    return data_type.strip(), nullable


def _validate_schema_text(value: str, label: str, *, max_length: int) -> None:
    if not value or len(value) > max_length:
        raise ValueError(f"{label} must contain 1-{max_length} characters")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        raise ValueError(f"{label} must contain printable text")
