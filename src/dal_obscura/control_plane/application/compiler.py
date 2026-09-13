from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import Any, cast
from uuid import UUID

from dal_obscura.common.access_control.compiled_policy import (
    CompiledMaskRule,
    CompiledPolicy,
    CompiledPolicyRule,
)
from dal_obscura.common.access_control.filters import deserialize_row_filter
from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.common.query_planning.field_paths import (
    FieldPath,
    FieldPathSegment,
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    MapValueSegment,
    parse_field_path,
)
from dal_obscura.control_plane.application.auth_provider_validation import (
    validate_auth_provider_payloads,
)
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

SUPPORTED_BACKENDS = frozenset({"iceberg"})
_MASK_TYPES = frozenset({"null", "redact", "hash", "email", "keep_last", "default"})
_ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
def validate_policy_rule_payloads(rules: list[dict[str, Any]]) -> None:
    """Validate mutable policy-rule payloads before storing draft policy state."""
    compiler = PublicationCompiler()
    for index, raw in enumerate(rules, start=1):
        draft = _policy_rule_draft_from_payload(index, raw)
        compiler._compile_rule(draft)


class PublicationCompiler:
    """Compiles mutable authoring resources into immutable published config rows."""

    def __init__(self, plugin_registry: PluginRegistry | None = None) -> None:
        self._plugin_registry = plugin_registry

    def compile(self, draft: PublishDraft) -> CompiledPublication:
        self._validate_runtime_components(draft)
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

    def _validate_runtime_components(self, draft: PublishDraft) -> None:
        for catalog in draft.catalogs:
            self._validate_catalog_plugin(catalog.module)
        validate_auth_provider_payloads(
            [
                {
                    "ordinal": provider.ordinal,
                    "module": provider.module,
                    "args": provider.args,
                    "enabled": provider.enabled,
                }
                for provider in draft.auth_providers
            ]
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
        self._validate_format_plugin(asset.backend)
        if asset.backend not in SUPPORTED_BACKENDS and self._plugin_registry is None:
            raise ValidationFailure(f"Unsupported backend {asset.backend!r}")
        if not asset.table_identifier or not asset.table_identifier.strip():
            raise ValidationFailure("Asset requires a physical Iceberg identifier")
        rules = [
            self._compile_rule(_expand_schema_bound_rule(rule, asset.schema_fields))
            for rule in sorted(asset.rules, key=lambda item: item.ordinal)
        ]
        policy = CompiledPolicy(
            version=0,
            catalog=asset.catalog_name,
            target=asset.target,
            rules=rules,
        )
        policy_json = policy.to_json()
        target_options = dict(asset.options)
        target_config: dict[str, object] = {
            "backend": asset.backend,
            "table": asset.table_identifier,
            "options": target_options,
        }
        compiled_config: dict[str, object] = {
            "catalog": {"module": catalog.module, "options": dict(catalog.options)},
            "target": target_config,
            "policy": policy_json,
            # Keep the selected adapter identities explicit in the immutable
            # manifest so future plugin routing never infers them from options.
            "plugins": {
                "catalog": catalog.module,
                "table_format": asset.backend,
            },
        }
        if asset.schema_fields:
            schema_fields = [
                {
                    "name": str(field["name"]),
                    "field_id": str(field["field_id"]),
                    "path": [str(segment) for segment in cast(list[object], field["path"])],
                    "type": str(field["type"]),
                    "nullable": bool(field["nullable"]),
                }
                for field in asset.schema_fields
            ]
            stable_ids = not any(
                str(field["field_id"]).startswith(("synthetic:", "legacy:"))
                for field in schema_fields
            )
            compiled_config["schema"] = {
                "encoding": 1,
                "fields": schema_fields,
                "stable_ids": stable_ids,
                "digest": _stable_hash(schema_fields),
            }
        policy_version = _stable_int63(policy_json)
        compiled_config["policy"]["version"] = policy_version
        return CompiledAsset(
            tenant_id=asset.tenant_id,
            catalog=asset.catalog_name,
            target=asset.target,
            backend=asset.backend,
            compiled_config=compiled_config,
            policy_version=policy_version,
        )

    def _validate_catalog_plugin(self, module: str) -> None:
        if module == _ICEBERG_CATALOG_MODULE:
            return
        if self._plugin_registry is None or ("catalog", module) not in self._admitted_plugins():
            raise ValidationFailure("Unsupported catalog module; only admitted plugins are allowed")

    def _validate_format_plugin(self, plugin_id: str) -> None:
        if plugin_id in SUPPORTED_BACKENDS:
            return
        if self._plugin_registry is None or (
            "table_format",
            plugin_id,
        ) not in self._admitted_plugins():
            raise ValidationFailure(f"Unsupported backend {plugin_id!r}")

    def _admitted_plugins(self) -> dict[tuple[str, str], object]:
        assert self._plugin_registry is not None
        admitted = self._plugin_registry.admitted()
        if not admitted:
            admitted = self._plugin_registry.reload()
        return cast(dict[tuple[str, str], object], admitted)


    def _compile_rule(self, rule: PolicyRuleDraft) -> CompiledPolicyRule:
        if rule.effect != "allow":
            raise ValidationFailure(
                "Policy rules are explicit grants; use effect='allow' or omit deny rules."
            )
        row_filter = _normalize_row_filter(rule.row_filter)
        return CompiledPolicyRule(
            ordinal=rule.ordinal,
            principals=list(rule.principals),
            columns=list(rule.columns),
            effect="allow",
            when=dict(rule.when),
            masks={column: _compile_mask_rule(column, mask) for column, mask in rule.masks.items()},
            row_filter=row_filter,
        )


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


def _expand_schema_bound_rule(
    rule: PolicyRuleDraft,
    schema_fields: list[dict[str, object]],
) -> PolicyRuleDraft:
    """Freezes wildcard/parent selections to the reviewed schema leaves."""

    if not schema_fields:
        return rule
    columns = _expand_schema_bound_paths(rule.columns, schema_fields)
    masks: dict[str, object] = {}
    for path, mask in rule.masks.items():
        expanded = _expand_schema_bound_paths([path], schema_fields)
        for candidate in expanded:
            masks[candidate] = mask
    return replace(rule, columns=columns, masks=masks)


def _expand_schema_bound_paths(
    requested: list[str],
    schema_fields: list[dict[str, object]],
) -> list[str]:
    admitted: list[tuple[str, tuple[FieldPathSegment, ...], str]] = []
    aliases: dict[str, str] = {}
    for field in schema_fields:
        raw_path = field.get("path")
        if not isinstance(raw_path, list) or not raw_path:
            continue
        path = tuple(_schema_path_segments(cast(list[object], raw_path)))
        canonical = FieldPath(path).to_human()
        raw_name = str(field.get("name", "")).strip()
        admitted.append((canonical, path, raw_name))
        if raw_name:
            aliases[raw_name] = canonical

    expanded: list[str] = []
    seen: set[str] = set()
    for value in requested:
        if value == "*":
            candidates = [item[0] for item in admitted]
        elif value in aliases:
            candidates = [aliases[value]]
        else:
            try:
                parsed = parse_field_path(value)
            except ValueError:
                candidates = [value]
            else:
                candidates = [
                    canonical
                    for canonical, path, _ in admitted
                    if len(parsed.segments) <= len(path)
                    and tuple(parsed.segments) == path[: len(parsed.segments)]
                ]
                if not candidates:
                    candidates = [value]
        for candidate in candidates:
            if candidate not in seen:
                expanded.append(candidate)
                seen.add(candidate)
    return expanded


def _schema_path_segments(path: list[object]) -> list[FieldPathSegment]:
    segments: list[FieldPathSegment] = []
    for segment in path:
        if not isinstance(segment, str) or not segment.strip():
            raise ValidationFailure("Schema field paths must contain non-empty strings")
        if segment == "$element":
            segments.append(ListElementSegment())
        elif segment == "$key":
            segments.append(MapKeySegment())
        elif segment == "$value":
            segments.append(MapValueSegment())
        else:
            segments.append(FieldSegment(segment))
    if not segments or not isinstance(segments[0], FieldSegment):
        raise ValidationFailure("Schema field paths must begin with a field segment")
    return segments


def _policy_rule_draft_from_payload(index: int, raw: dict[str, Any]) -> PolicyRuleDraft:
    try:
        effect = str(raw.get("effect", "allow"))
        if effect != "allow":
            raise ValidationFailure(
                "Policy rules are explicit grants; use effect='allow' or omit deny rules."
            )
        row_filter = raw.get("row_filter")
        if row_filter is not None and not isinstance(row_filter, str):
            raise ValueError("row_filter must be a string")
        when = raw.get("when", {})
        if not isinstance(when, dict):
            raise ValueError("when must be an object")
        masks = raw.get("masks", {})
        if not isinstance(masks, dict):
            raise ValueError("masks must be an object")
        return PolicyRuleDraft(
            ordinal=int(raw["ordinal"]),
            effect="allow",
            principals=[str(item) for item in _list(raw.get("principals"))],
            when=cast(dict[str, str | list[str]], dict(when)),
            columns=[str(item) for item in _list(raw.get("columns"))],
            masks=dict(masks),
            row_filter=row_filter,
        )
    except ValidationFailure:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationFailure(f"Invalid policy rule {index}") from exc


def _list(value: object) -> list[object]:
    if isinstance(value, list):
        return list(value)
    return []


def _stable_hash(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _stable_int63(value: object) -> int:
    return int(_stable_hash(value), 16) & ((1 << 63) - 1)


def _mask_dict(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def _compile_mask_rule(column: str, raw_mask: object) -> CompiledMaskRule:
    """Validate a mask at publication time so invalid rules cannot disappear."""
    mask = _mask_dict(raw_mask)
    if set(mask) - {"type", "value"}:
        raise ValidationFailure(f"Invalid mask for column {column!r}")
    mask_type = mask.get("type")
    if not isinstance(mask_type, str) or mask_type.lower() not in _MASK_TYPES:
        raise ValidationFailure(f"Invalid mask for column {column!r}")

    normalized_type = mask_type.lower()
    value = mask.get("value")
    if normalized_type == "redact" and not isinstance(value, str):
        raise ValidationFailure(f"Invalid mask for column {column!r}")
    if normalized_type == "keep_last" and (
        isinstance(value, bool) or not isinstance(value, int) or value < 0
    ):
        raise ValidationFailure(f"Invalid mask for column {column!r}")
    if normalized_type == "default":
        if value is None or isinstance(value, (dict, list, tuple, set)):
            raise ValidationFailure(f"Invalid mask for column {column!r}")
        if isinstance(value, float) and (value != value or value in {float("inf"), float("-inf")}):
            raise ValidationFailure(f"Invalid mask for column {column!r}")

    return CompiledMaskRule(type=normalized_type, value=value)


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
