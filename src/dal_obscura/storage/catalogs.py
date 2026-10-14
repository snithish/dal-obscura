from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import func, or_, select
from sqlalchemy.orm import Session

from dal_obscura.control.errors import (
    ConfigurationConflictError,
    RevisionPreconditionRequired,
)
from dal_obscura.storage.database.orm import (
    AssetRecord,
    CatalogRecord,
)


def upsert_catalog(
    session: Session,
    *,
    name: str,
    plugin_id: str,
    options: dict[str, Any],
    expected_revision: int | None = None,
) -> UUID:
    existing = session.scalar(
        select(CatalogRecord)
        .where(CatalogRecord.name == name)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if existing is None:
        if expected_revision not in (None, 0):
            raise ConfigurationConflictError(
                "Catalog revision changed (expected "
                f"{expected_revision}, current 0); reread before writing.",
                current_revision=0,
            )
        catalog_id = uuid4()
        session.add(
            CatalogRecord(id=catalog_id, name=name, plugin_id=plugin_id, options_json=options)
        )
    else:
        if expected_revision is None:
            raise RevisionPreconditionRequired(
                "Catalog revision is required for updates "
                f"(current {existing.revision}); reread before writing.",
                current_revision=existing.revision,
            )
        if existing.revision != expected_revision:
            raise ConfigurationConflictError(
                "Catalog revision changed (expected "
                f"{expected_revision}, current {existing.revision}); reread before writing.",
                current_revision=existing.revision,
            )
        catalog_id = existing.id
        if existing.plugin_id != plugin_id or existing.options_json != options:
            existing.plugin_id = plugin_id
            existing.options_json = options
            existing.revision += 1
    session.flush()
    return catalog_id


def list_workspace_catalogs(
    session: Session,
) -> list[dict[str, object]]:
    assets_by_catalog: dict[UUID, int] = {}
    for row in session.execute(
        select(AssetRecord.catalog_id, func.count()).group_by(AssetRecord.catalog_id)
    ):
        assets_by_catalog[row[0]] = row[1]
    return [
        {
            "id": str(record.id),
            "name": record.name,
            "plugin_id": record.plugin_id,
            "options": dict(record.options_json),
            "status": "configured",
            "revision": record.revision,
            "discovered_table_count": 0,
            "governed_asset_count": assets_by_catalog.get(record.id, 0),
        }
        for record in session.scalars(select(CatalogRecord).order_by(CatalogRecord.name))
    ]


def get_workspace_catalog(session: Session, name: str) -> dict[str, object]:
    record = _catalog_by_name(session, name=name)
    return {
        "id": str(record.id),
        "name": record.name,
        "plugin_id": record.plugin_id,
        "options": dict(record.options_json),
        "revision": record.revision,
    }


def governed_table_identifiers(
    session: Session, *, catalog_id: UUID, identifiers: list[str]
) -> set[str]:
    """Match only the requested discovery page, including friendly asset aliases."""

    if not identifiers:
        return set()
    rows = session.execute(
        select(AssetRecord.target, AssetRecord.table_identifier).where(
            AssetRecord.catalog_id == catalog_id,
            or_(AssetRecord.target.in_(identifiers), AssetRecord.table_identifier.in_(identifiers)),
        )
    )
    requested = set(identifiers)
    return {value for row in rows for value in row if value in requested}


def _catalog_by_name(session: Session, *, name: str) -> CatalogRecord:
    catalog = session.scalar(select(CatalogRecord).where(CatalogRecord.name == name))
    if catalog is None:
        raise LookupError(f"No catalog {name!r}")
    return catalog
