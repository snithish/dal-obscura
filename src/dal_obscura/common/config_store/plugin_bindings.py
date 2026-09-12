"""Dry-run and explicit application of published plugin binding metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from dal_obscura.common.config_store.orm import PublishedAssetRecord, PublishedCatalogRecord

_ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)


@dataclass(frozen=True)
class PluginBindingCandidate:
    kind: Literal["catalog", "asset"]
    record_id: str
    status: Literal["bound", "migratable", "unsupported"]
    catalog_plugin_id: str | None = None
    format_plugin_id: str | None = None
    reason: str | None = None
    key: tuple[str, ...] = ()


@dataclass(frozen=True)
class PluginBindingReport:
    candidates: tuple[PluginBindingCandidate, ...]

    @property
    def migratable(self) -> tuple[PluginBindingCandidate, ...]:
        return tuple(item for item in self.candidates if item.status == "migratable")

    @property
    def unsupported(self) -> tuple[PluginBindingCandidate, ...]:
        return tuple(item for item in self.candidates if item.status == "unsupported")

    def to_dict(self) -> dict[str, object]:
        return {
            "total": len(self.candidates),
            "bound": sum(item.status == "bound" for item in self.candidates),
            "migratable": len(self.migratable),
            "unsupported": len(self.unsupported),
            "records": [
                {
                    "kind": item.kind,
                    "record_id": item.record_id,
                    "status": item.status,
                    **(
                        {"catalog_plugin_id": item.catalog_plugin_id}
                        if item.catalog_plugin_id is not None
                        else {}
                    ),
                    **(
                        {"format_plugin_id": item.format_plugin_id}
                        if item.format_plugin_id is not None
                        else {}
                    ),
                    **({"reason": item.reason} if item.reason is not None else {}),
                }
                for item in self.candidates
            ],
        }


def inspect_plugin_bindings(session: Session) -> PluginBindingReport:
    """Return a deterministic report without changing any publication row."""

    candidates: list[PluginBindingCandidate] = []
    catalogs = session.scalars(
        select(PublishedCatalogRecord).order_by(
            PublishedCatalogRecord.publication_id,
            PublishedCatalogRecord.tenant_id,
            PublishedCatalogRecord.catalog,
        )
    )
    for record in catalogs:
        if record.plugin_id is not None:
            candidates.append(
                PluginBindingCandidate(
                    kind="catalog",
                    record_id=f"{record.publication_id}:{record.tenant_id}:{record.catalog}",
                    status="bound",
                    catalog_plugin_id=record.plugin_id,
                    key=(str(record.publication_id), str(record.tenant_id), record.catalog),
                )
            )
            continue
        plugin_id = _catalog_plugin_id(record.config_json)
        candidates.append(
            PluginBindingCandidate(
                kind="catalog",
                record_id=f"{record.publication_id}:{record.tenant_id}:{record.catalog}",
                status="migratable" if plugin_id else "unsupported",
                catalog_plugin_id=plugin_id,
                reason=None if plugin_id else "catalog adapter identity is unknown",
                key=(str(record.publication_id), str(record.tenant_id), record.catalog),
            )
        )

    assets = session.scalars(
        select(PublishedAssetRecord).order_by(
            PublishedAssetRecord.publication_id,
            PublishedAssetRecord.tenant_id,
            PublishedAssetRecord.catalog,
            PublishedAssetRecord.target,
        )
    )
    for record in assets:
        if record.catalog_plugin_id is not None and record.format_plugin_id is not None:
            candidates.append(
                PluginBindingCandidate(
                    kind="asset",
                    record_id=f"{record.publication_id}:{record.tenant_id}:{record.catalog}:{record.target}",
                    status="bound",
                    catalog_plugin_id=record.catalog_plugin_id,
                    format_plugin_id=record.format_plugin_id,
                    key=(
                        str(record.publication_id),
                        str(record.tenant_id),
                        record.catalog,
                        record.target,
                    ),
                )
            )
            continue
        catalog_plugin_id = _catalog_plugin_id(record.compiled_config_json)
        format_plugin_id = _format_plugin_id(record.compiled_config_json, record.backend)
        migratable = catalog_plugin_id is not None and format_plugin_id is not None
        candidates.append(
            PluginBindingCandidate(
                kind="asset",
                record_id=f"{record.publication_id}:{record.tenant_id}:{record.catalog}:{record.target}",
                status="migratable" if migratable else "unsupported",
                catalog_plugin_id=catalog_plugin_id,
                format_plugin_id=format_plugin_id,
                reason=None if migratable else "catalog or table-format identity is unknown",
                key=(
                    str(record.publication_id),
                    str(record.tenant_id),
                    record.catalog,
                    record.target,
                ),
            )
        )
    return PluginBindingReport(tuple(candidates))


def apply_plugin_bindings(session: Session, report: PluginBindingReport) -> int:
    """Populate only exact built-in identities from a prior dry-run report."""

    applied = 0
    for candidate in report.migratable:
        if candidate.kind == "catalog" and len(candidate.key) == 3:
            publication_id, tenant_id, catalog = candidate.key
            record = session.get(
                PublishedCatalogRecord,
                {
                    "publication_id": UUID(publication_id),
                    "tenant_id": UUID(tenant_id),
                    "catalog": catalog,
                },
            )
            if record is not None and record.plugin_id is None and candidate.catalog_plugin_id:
                record.plugin_id = candidate.catalog_plugin_id
                applied += 1
        elif candidate.kind == "asset" and len(candidate.key) == 4:
            publication_id, tenant_id, catalog, target = candidate.key
            record = session.get(
                PublishedAssetRecord,
                {
                    "publication_id": UUID(publication_id),
                    "tenant_id": UUID(tenant_id),
                    "catalog": catalog,
                    "target": target,
                },
            )
            if record is not None:
                changed = False
                if record.catalog_plugin_id is None and candidate.catalog_plugin_id:
                    record.catalog_plugin_id = candidate.catalog_plugin_id
                    changed = True
                if record.format_plugin_id is None and candidate.format_plugin_id:
                    record.format_plugin_id = candidate.format_plugin_id
                    changed = True
                if changed:
                    applied += 1
    session.flush()
    return applied


def _catalog_plugin_id(config: dict[str, Any]) -> str | None:
    plugins = config.get("plugins")
    if isinstance(plugins, dict) and plugins.get("catalog") in {
        _ICEBERG_CATALOG_MODULE,
        "iceberg.sql",
    }:
        return "iceberg.sql"
    catalog = config.get("catalog")
    if isinstance(catalog, dict) and catalog.get("module") == _ICEBERG_CATALOG_MODULE:
        return "iceberg.sql"
    if config.get("module") == _ICEBERG_CATALOG_MODULE:
        return "iceberg.sql"
    return None


def _format_plugin_id(config: dict[str, Any], backend: str) -> str | None:
    plugins = config.get("plugins")
    if isinstance(plugins, dict) and plugins.get("table_format") == "iceberg":
        return "iceberg"
    return "iceberg" if backend == "iceberg" else None
