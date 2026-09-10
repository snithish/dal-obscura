"""Strict JSON manifest parsing for the non-web operator workflow."""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast
from uuid import NAMESPACE_URL, UUID, uuid5

from dal_obscura.control_plane.application.compiler import PublicationCompiler
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.domain.models import (
    AssetDraft,
    AuthProviderDraft,
    CatalogDraft,
    CellRuntimeDraft,
    PolicyRuleDraft,
    PublishDraft,
)

_ICEBERG_CATALOG_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog"
)
_OIDC_IDENTITY_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
)
_MAX_MANIFEST_BYTES = 1_000_000


class ManifestValidationError(ValueError):
    """Raised when an operator manifest is malformed or unsupported."""


def load_manifest(path: Path) -> dict[str, object]:
    """Loads one bounded JSON manifest while rejecting duplicate keys."""

    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise ManifestValidationError(f"cannot read manifest: {exc}") from exc
    if len(raw) > _MAX_MANIFEST_BYTES:
        raise ManifestValidationError("manifest exceeds 1000000 byte limit")
    try:
        value = json.loads(raw, object_pairs_hook=_unique_object)
    except (json.JSONDecodeError, UnicodeDecodeError, ManifestValidationError) as exc:
        raise ManifestValidationError(f"invalid JSON manifest: {exc}") from exc
    return _object(value, "manifest")


def compile_manifest(manifest: dict[str, object]):
    """Validates a manifest by compiling the exact publication it describes."""

    try:
        return PublicationCompiler().compile(_draft_from_manifest(manifest))
    except ValidationFailure as exc:
        raise ManifestValidationError(str(exc)) from exc


def _draft_from_manifest(manifest: dict[str, object]) -> PublishDraft:
    _reject_unknown(
        manifest,
        {"version", "cell_id", "tenant_id", "runtime", "auth_providers", "catalogs", "assets"},
        "manifest",
    )
    if manifest.get("version") != 1:
        raise ManifestValidationError("manifest.version must be 1")
    cell_id = _uuid(manifest.get("cell_id"), "cell_id", "cell")
    tenant_id = _uuid(manifest.get("tenant_id"), "tenant_id", "tenant")
    runtime = _object(manifest.get("runtime"), "runtime")
    _reject_unknown(
        runtime,
        {"ticket_ttl_seconds", "max_tickets", "max_ticket_exchanges"},
        "runtime",
    )
    runtime_draft = CellRuntimeDraft(
        ticket_ttl_seconds=_positive_int(
            runtime.get("ticket_ttl_seconds"), "runtime.ticket_ttl_seconds"
        ),
        max_tickets=_positive_int(runtime.get("max_tickets"), "runtime.max_tickets"),
        max_ticket_exchanges=_positive_int(
            runtime.get("max_ticket_exchanges"), "runtime.max_ticket_exchanges"
        ),
    )
    catalogs = _catalogs(manifest.get("catalogs"), cell_id, tenant_id)
    catalog_ids = {catalog.name: catalog.id for catalog in catalogs}
    return PublishDraft(
        cell_id=cell_id,
        tenants=[tenant_id],
        runtime=runtime_draft,
        auth_providers=_providers(manifest.get("auth_providers")),
        catalogs=catalogs,
        assets=_assets(manifest.get("assets"), cell_id, tenant_id, catalog_ids),
    )


def _providers(value: object) -> list[AuthProviderDraft]:
    providers = _list(value, "auth_providers")
    result: list[AuthProviderDraft] = []
    for ordinal, raw in enumerate(providers, start=1):
        provider = _object(raw, f"auth_providers[{ordinal - 1}]")
        _reject_unknown(provider, {"args", "enabled"}, f"auth_providers[{ordinal - 1}]")
        result.append(
            AuthProviderDraft(
                ordinal=ordinal,
                module=_OIDC_IDENTITY_MODULE,
                args=_object(provider.get("args"), f"auth_providers[{ordinal - 1}].args"),
                enabled=_bool(
                    provider.get("enabled", True),
                    f"auth_providers[{ordinal - 1}].enabled",
                ),
            )
        )
    return result


def _catalogs(value: object, cell_id: UUID, tenant_id: UUID) -> list[CatalogDraft]:
    catalogs = _list(value, "catalogs")
    result: list[CatalogDraft] = []
    names: set[str] = set()
    for index, raw in enumerate(catalogs):
        catalog = _object(raw, f"catalogs[{index}]")
        _reject_unknown(catalog, {"name", "options"}, f"catalogs[{index}]")
        name = _text(catalog.get("name"), f"catalogs[{index}].name")
        if name in names:
            raise ManifestValidationError(f"duplicate catalog {name!r}")
        names.add(name)
        result.append(
            CatalogDraft(
                id=uuid5(NAMESPACE_URL, f"dal-obscura/catalog/{name}"),
                cell_id=cell_id,
                tenant_id=tenant_id,
                name=name,
                module=_ICEBERG_CATALOG_MODULE,
                options=_object(catalog.get("options"), f"catalogs[{index}].options"),
            )
        )
    return result


def _assets(
    value: object, cell_id: UUID, tenant_id: UUID, catalog_ids: dict[str, UUID]
) -> list[AssetDraft]:
    assets = _list(value, "assets")
    result: list[AssetDraft] = []
    targets: set[tuple[str, str]] = set()
    for index, raw in enumerate(assets):
        asset = _object(raw, f"assets[{index}]")
        _reject_unknown(
            asset,
            {"catalog", "target", "table_identifier", "options", "rules"},
            f"assets[{index}]",
        )
        catalog = _text(asset.get("catalog"), f"assets[{index}].catalog")
        target = _text(asset.get("target"), f"assets[{index}].target")
        if catalog not in catalog_ids:
            raise ManifestValidationError(f"assets[{index}] references unknown catalog {catalog!r}")
        if (catalog, target) in targets:
            raise ManifestValidationError(f"duplicate asset {catalog}/{target}")
        targets.add((catalog, target))
        result.append(
            AssetDraft(
                id=uuid5(NAMESPACE_URL, f"dal-obscura/asset/{catalog}/{target}"),
                cell_id=cell_id,
                tenant_id=tenant_id,
                catalog_id=catalog_ids[catalog],
                catalog_name=catalog,
                target=target,
                backend="iceberg",
                table_identifier=_text(
                    asset.get("table_identifier"), f"assets[{index}].table_identifier"
                ),
                options=_object(asset.get("options", {}), f"assets[{index}].options"),
                rules=_rules(asset.get("rules", []), index),
            )
        )
    return result


def _rules(value: object, asset_index: int) -> list[PolicyRuleDraft]:
    rules = _list(value, f"assets[{asset_index}].rules")
    result: list[PolicyRuleDraft] = []
    for ordinal, raw in enumerate(rules, start=1):
        rule = _object(raw, f"assets[{asset_index}].rules[{ordinal - 1}]")
        _reject_unknown(rule, {"principals", "when", "columns", "masks", "row_filter"}, "rule")
        result.append(
            PolicyRuleDraft(
                ordinal=ordinal,
                effect="allow",
                principals=[
                    _text(item, "rule.principals[]")
                    for item in _list(rule.get("principals"), "rule.principals")
                ],
                when=cast(dict[str, str | list[str]], _object(rule.get("when", {}), "rule.when")),
                columns=[
                    _text(item, "rule.columns[]")
                    for item in _list(rule.get("columns"), "rule.columns")
                ],
                masks=_object(rule.get("masks", {}), "rule.masks"),
                row_filter=_optional_text(rule.get("row_filter"), "rule.row_filter"),
            )
        )
    return result


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ManifestValidationError(f"duplicate key {key!r}")
        result[key] = value
    return result


def _reject_unknown(value: dict[str, object], allowed: set[str], label: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ManifestValidationError(f"{label} has unsupported keys: {', '.join(unknown)}")


def _object(value: object, label: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ManifestValidationError(f"{label} must be an object")
    return cast(dict[str, object], value)


def _list(value: object, label: str) -> list[object]:
    if not isinstance(value, list):
        raise ManifestValidationError(f"{label} must be a list")
    return cast(list[object], value)


def _text(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ManifestValidationError(f"{label} must be non-empty text")
    return value


def _optional_text(value: object, label: str) -> str | None:
    if value is None:
        return None
    return _text(value, label)


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ManifestValidationError(f"{label} must be a positive integer")
    return value


def _bool(value: object, label: str) -> bool:
    if not isinstance(value, bool):
        raise ManifestValidationError(f"{label} must be a boolean")
    return value


def _uuid(value: object, label: str, fallback: str) -> UUID:
    if value is None:
        return uuid5(NAMESPACE_URL, f"dal-obscura/{fallback}")
    if not isinstance(value, str):
        raise ManifestValidationError(f"{label} must be a UUID string")
    try:
        return UUID(value)
    except ValueError as exc:
        raise ManifestValidationError(f"{label} must be a UUID string") from exc
