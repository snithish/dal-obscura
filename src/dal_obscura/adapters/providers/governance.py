"""Startup-admitted provider schema IO and synthetic identity mapping."""

from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager
from threading import BoundedSemaphore, Lock
from typing import Any, cast

import pyarrow as pa
from dal_obscura_plugin_api import DiscoveryPage

from dal_obscura.adapters.providers.discovery import discover_public_catalog_tables
from dal_obscura.adapters.providers.schema import load_source_schema
from dal_obscura.application.governance.catalogs.validation import validate_catalog_options
from dal_obscura.application.governance.errors import ValidationFailure
from dal_obscura.application.governance.values import AssetSchemaDiscovery, CatalogDiscovery
from dal_obscura.identity.claims import PrincipalClaimMapper
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.plugins import PluginRegistry
from dal_obscura.runtime.secrets import SecretProvider, resolve_secret_refs


class ProviderAssetSchemaLoader:
    def __init__(
        self,
        *,
        egress_allowlist: tuple[str, ...] = (),
        plugin_registry: PluginRegistry | None = None,
        secret_provider: SecretProvider | None = None,
    ) -> None:
        self.egress_allowlist = egress_allowlist
        self.plugin_registry = plugin_registry
        self.secret_provider = secret_provider

    def load_asset_schema(self, captured: AssetSchemaDiscovery) -> pa.Schema:
        """Loads one captured authoritative source schema without a database session."""
        config = captured.catalog.config
        asset: dict[str, object] = {
            "name": captured.name,
            "backend": captured.backend,
            "table_identifier": captured.table_identifier,
        }
        catalog: dict[str, object] = {
            "name": config.instance_id,
            "plugin_id": config.plugin_id,
            "revision": config.revision,
        }
        options = cast(
            dict[str, object],
            resolve_secret_refs(
                (
                    self.plugin_registry.options.for_plugin(config.plugin_id, config.options)
                    if self.plugin_registry is not None
                    else config.options
                ),
                provider=self.secret_provider,
                expected_scope=f"catalog:{config.instance_id}",
            ),
        )
        try:
            return load_source_schema(
                asset=asset,
                catalog=catalog,
                options=options,
                plugin_registry=self.plugin_registry,
                validate_metadata=lambda metadata: validate_catalog_options(
                    metadata, egress_allowlist=self.egress_allowlist
                ),
            )
        except ValidationFailure:
            raise
        except Exception as exc:
            # Provider errors can contain credentials and internal paths.
            raise ValidationFailure("Schema discovery failed") from exc


class SyntheticPreviewIdentityMapper:
    def map_attributes(
        self, provider_args: dict[str, Any], claims: dict[str, object]
    ) -> dict[str, str]:
        mapper = PrincipalClaimMapper(
            attribute_claims=provider_args.get("attribute_claims"),
            attribute_definitions=provider_args.get("attribute_definitions"),
        )
        return mapper.map_attributes(claims)

    def map_claims(self, provider: dict[str, object], claims: dict[str, object]) -> Principal:
        args = cast(dict[str, Any], provider["args"])
        mapper = PrincipalClaimMapper(
            subject_claim=args.get("subject_claim", "sub"),
            group_claims=args.get("group_claims", []),
            attribute_claims=args.get("attribute_claims", {}),
            attribute_definitions=args.get("attribute_definitions", {}),
        )
        return mapper.map_claims(claims)


DEFAULT_MAX_ACTIVE_DISCOVERIES_PER_SESSION = 2


class _SessionDiscoverySlot:
    def __init__(self) -> None:
        self.semaphore = BoundedSemaphore(DEFAULT_MAX_ACTIVE_DISCOVERIES_PER_SESSION)
        self.active = 0


_SESSION_DISCOVERY_SLOTS: dict[str, _SessionDiscoverySlot] = {}
_SESSION_DISCOVERY_LOCK = Lock()


@contextmanager
def _admit_session_discovery(session_key: str | None) -> Generator[None]:
    """Admit at most two concurrent discoveries for one authenticated session."""

    if not session_key:
        yield
        return
    with _SESSION_DISCOVERY_LOCK:
        slot = _SESSION_DISCOVERY_SLOTS.setdefault(session_key, _SessionDiscoverySlot())
        if not slot.semaphore.acquire(blocking=False):
            raise ValidationFailure("Catalog discovery session capacity is exhausted; retry later")
        slot.active += 1
    try:
        yield
    finally:
        with _SESSION_DISCOVERY_LOCK:
            slot.semaphore.release()
            slot.active -= 1
            if slot.active == 0:
                _SESSION_DISCOVERY_SLOTS.pop(session_key, None)


def _resolve_catalog_secrets(
    options: dict[str, Any],
    *,
    scope: str | None = None,
    provider: SecretProvider | None = None,
) -> dict[str, Any]:
    try:
        resolved = resolve_secret_refs(
            options,
            provider=provider,
            expected_scope=scope,
        )
    except ValueError as exc:
        raise ValidationFailure("Catalog secret could not be resolved") from exc
    return cast(dict[str, Any], resolved)


class ProviderCatalogBrowser:
    def __init__(
        self, *, plugin_registry: PluginRegistry | None, secret_provider: SecretProvider | None
    ) -> None:
        self.plugin_registry = plugin_registry
        self.secret_provider = secret_provider

    def discover(
        self,
        captured: CatalogDiscovery,
        *,
        session_key: str,
        continuation: str | None = None,
        limit: int = 100,
    ) -> DiscoveryPage:
        """Read one provider page without retaining a database session."""

        config = captured.config
        configured = (
            self.plugin_registry.options.for_plugin(config.plugin_id, config.options)
            if self.plugin_registry is not None
            else config.options
        )
        options = _resolve_catalog_secrets(
            dict(configured), scope=f"catalog:{config.instance_id}", provider=self.secret_provider
        )
        try:
            with _admit_session_discovery(session_key):
                return discover_public_catalog_tables(
                    config.instance_id,
                    config.plugin_id,
                    options,
                    revision=config.revision,
                    plugin_registry=self.plugin_registry,
                    continuation=continuation,
                    limit=limit,
                )
        except ValidationFailure:
            raise
        except Exception as exc:
            raise ValidationFailure("Catalog discovery failed") from exc
