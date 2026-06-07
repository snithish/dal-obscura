from __future__ import annotations

import hashlib
import json
from uuid import UUID

from dal_obscura.common.access_control.filters import deserialize_row_filter
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.domain.models import (
    AssetDraft,
    CatalogDraft,
    CompiledAsset,
    CompiledCatalog,
    CompiledPublication,
    CompiledRuntime,
    PolicyRuleDraft,
    PublishDraft,
)

SUPPORTED_BACKENDS = frozenset(
    {"iceberg", "delta", "parquet", "csv", "json", "orc", "avro", "text"}
)


class PublicationCompiler:
    """Compiles mutable authoring resources into immutable published config rows."""

    def compile(self, draft: PublishDraft) -> CompiledPublication:
        catalog_by_id = {catalog.id: catalog for catalog in draft.catalogs}
        compiled_catalogs = [
            CompiledCatalog(
                tenant_id=catalog.tenant_id,
                catalog=catalog.name,
                config={"module": catalog.module, "options": dict(catalog.options)},
            )
            for catalog in draft.catalogs
        ]
        compiled_assets = [
            self._compile_asset(asset, catalog_by_id[asset.catalog_id]) for asset in draft.assets
        ]
        runtime = CompiledRuntime(
            auth_chain={
                "providers": [
                    {
                        "ordinal": provider.ordinal,
                        "module": provider.module,
                        "args": provider.args,
                        "enabled": provider.enabled,
                    }
                    for provider in sorted(draft.auth_providers, key=lambda item: item.ordinal)
                    if provider.enabled
                ]
            },
            ticket={
                "ttl_seconds": draft.runtime.ticket_ttl_seconds,
                "max_tickets": draft.runtime.max_tickets,
                "max_exchanges": draft.runtime.max_ticket_exchanges,
            },
            path_rules=list(draft.runtime.path_rules),
        )
        return CompiledPublication(
            cell_id=draft.cell_id,
            runtime=runtime,
            catalogs=compiled_catalogs,
            assets=compiled_assets,
            manifest_hash=_publication_hash(
                cell_id=draft.cell_id,
                runtime=runtime,
                catalogs=compiled_catalogs,
                assets=compiled_assets,
            ),
        )

    def compile_asset(self, asset: AssetDraft, catalog: CatalogDraft) -> CompiledAsset:
        return self._compile_asset(asset, catalog)

    def compile_policy_version(
        self,
        *,
        cell_id: UUID,
        runtime: CompiledRuntime,
        catalogs: list[CompiledCatalog],
        assets: list[CompiledAsset],
    ) -> CompiledPublication:
        return CompiledPublication(
            cell_id=cell_id,
            runtime=runtime,
            catalogs=list(catalogs),
            assets=list(assets),
            manifest_hash=_publication_hash(
                cell_id=cell_id,
                runtime=runtime,
                catalogs=catalogs,
                assets=assets,
            ),
        )

    def _compile_asset(self, asset: AssetDraft, catalog: CatalogDraft) -> CompiledAsset:
        if asset.backend not in SUPPORTED_BACKENDS:
            raise ValidationFailure(f"Unsupported backend {asset.backend!r}")
        rules = [
            self._compile_rule(rule) for rule in sorted(asset.rules, key=lambda item: item.ordinal)
        ]
        compiled_config = {
            "catalog": {"module": catalog.module, "options": dict(catalog.options)},
            "target": {
                "backend": asset.backend,
                "table": asset.table_identifier or asset.target,
                "options": dict(asset.options),
            },
            "policy": {"rules": rules},
        }
        return CompiledAsset(
            tenant_id=asset.tenant_id,
            catalog=asset.catalog_name,
            target=asset.target,
            backend=asset.backend,
            compiled_config=compiled_config,
            policy_version=_stable_int63(compiled_config["policy"]),
        )

    def _compile_rule(self, rule: PolicyRuleDraft) -> dict[str, object]:
        if rule.effect == "deny" and rule.masks:
            raise ValidationFailure("deny rules may not define masks")
        if rule.effect == "deny" and rule.row_filter:
            raise ValidationFailure("deny rules may not define row_filter")
        row_filter = _normalize_row_filter(rule.row_filter)
        return {
            "principals": list(rule.principals),
            "columns": list(rule.columns),
            "effect": rule.effect,
            "when": dict(rule.when),
            "masks": dict(rule.masks),
            "row_filter": row_filter,
        }


def _normalize_row_filter(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        deserialize_row_filter(normalized)
    except Exception as exc:
        raise ValidationFailure(f"Invalid row_filter SQL: {normalized}") from exc
    return normalized


def _stable_hash(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _stable_int63(value: object) -> int:
    return int(_stable_hash(value), 16) & ((1 << 63) - 1)


def _publication_hash(
    *,
    cell_id: UUID,
    runtime: CompiledRuntime,
    catalogs: list[CompiledCatalog],
    assets: list[CompiledAsset],
) -> str:
    return _stable_hash(
        {
            "cell_id": str(cell_id),
            "runtime": runtime,
            "catalogs": catalogs,
            "assets": assets,
        }
    )
