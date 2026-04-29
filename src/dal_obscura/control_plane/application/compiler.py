from __future__ import annotations

import hashlib
import json

import sqlglot

from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.domain.models import (
    AssetDraft,
    CompiledAsset,
    CompiledCatalog,
    CompiledPublication,
    CompiledRuntime,
    PolicyRuleDraft,
    PublishDraft,
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
            },
            path_rules=list(draft.runtime.path_rules),
        )
        manifest_payload = {
            "cell_id": str(draft.cell_id),
            "runtime": runtime,
            "catalogs": compiled_catalogs,
            "assets": compiled_assets,
        }
        return CompiledPublication(
            cell_id=draft.cell_id,
            runtime=runtime,
            catalogs=compiled_catalogs,
            assets=compiled_assets,
            manifest_hash=_stable_hash(manifest_payload),
        )

    def _compile_asset(self, asset: AssetDraft, catalog) -> CompiledAsset:
        if asset.backend != "iceberg":
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
        sqlglot.parse_one(normalized, read="duckdb")
    except Exception as exc:
        raise ValidationFailure(f"Invalid row_filter SQL: {normalized}") from exc
    return normalized


def _stable_hash(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _stable_int63(value: object) -> int:
    return int(_stable_hash(value), 16) & ((1 << 63) - 1)
