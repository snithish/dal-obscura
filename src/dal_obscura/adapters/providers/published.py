from __future__ import annotations

import json
import re
from collections import OrderedDict
from collections.abc import Generator, Iterable, Mapping
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from threading import Condition
from time import monotonic
from typing import Any, cast

import pyarrow as pa
from dal_obscura_plugin_api import ExecutionContext
from dal_obscura_plugin_api.execution import close_all_preserving_error, close_preserving_error
from dal_obscura_plugin_api.options import PluginOptions
from dal_obscura_plugin_api.paths import (
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    parse_field_path,
    resolve_schema_path,
)
from dal_obscura_plugin_api.schema.identity import (
    schema_field_id,
    schema_has_stable_ids,
    schema_scope_digest,
)
from dal_obscura_plugin_api.schema.index import walk_schema_fields
from dal_obscura_plugin_api.storage import PathRuleEnforcer

from dal_obscura.adapters.providers.adapters import PublicPluginTableFormat
from dal_obscura.adapters.providers.catalogs import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from dal_obscura.application.reads.sources import (
    AccessContext,
    AccessContextUnavailable,
    AdmittedPluginSnapshot,
    PublishedSnapshotReader,
    new_execution_context,
)
from dal_obscura.kernel.identity.principal import Principal
from dal_obscura.kernel.plans.snapshot import PublishedAssetSnapshot
from dal_obscura.kernel.policy.models import AccessDecision
from dal_obscura.kernel.policy.policy_resolution import resolve_access
from dal_obscura.plugins import PluginRegistry
from dal_obscura.runtime.secrets import (
    SecretProvider,
    resolve_secret_refs,
)


def _authorize_asset(
    asset: PublishedAssetSnapshot,
    principal: Principal,
    target: str,
    catalog: str | None,
    requested_columns: Iterable[str],
) -> AccessDecision:
    if catalog != asset.catalog.name or target != asset.target:
        raise ValueError("Authorization context does not match the requested asset")
    policy = asset.policy.to_policy()
    allowed_columns, masks, row_filter = resolve_access(
        policy, principal, target, catalog, requested_columns
    )
    return AccessDecision(
        allowed_columns=allowed_columns,
        masks=masks,
        row_filter=row_filter,
        policy_version=asset.policy.version,
        asset_id=None if asset.asset_id is None else str(asset.asset_id),
    )


@dataclass(frozen=True)
class PublishedPolicy:
    asset: PublishedAssetSnapshot

    def authorize(
        self,
        principal: Principal,
        target: str,
        catalog: str | None,
        requested_columns: Iterable[str],
    ) -> AccessDecision:
        return _authorize_asset(self.asset, principal, target, catalog, requested_columns)


@dataclass
class _ProviderEntry:
    registry: CatalogRegistry | None = None
    users: int = 0
    retiring: bool = False


class LiveConfigCatalogRegistry:
    """Request snapshots with bounded, leased catalog-provider reuse.

    Only provider instances are cached. Policy and schema admission always come
    from a fresh database snapshot. Leases cover discovery and planning; an
    evicted or closed provider cannot be disposed while a request still uses it.
    """

    def __init__(
        self,
        store: PublishedSnapshotReader,
        *,
        secret_provider: SecretProvider | None = None,
        plugin_registry: PluginRegistry,
        path_enforcer: PathRuleEnforcer | None = None,
        max_cached_providers: int = 32,
        provider_wait_seconds: float = 5.0,
    ) -> None:
        if max_cached_providers < 1:
            raise ValueError("Provider cache capacity must be positive")
        if provider_wait_seconds <= 0:
            raise ValueError("Provider wait timeout must be positive")
        self._provider_wait_seconds = provider_wait_seconds
        self._store = store
        self._secret_provider = secret_provider
        self._plugin_registry = plugin_registry
        self._path_enforcer = path_enforcer
        self._max_cached_providers = max_cached_providers
        self._providers: OrderedDict[str, _ProviderEntry] = OrderedDict()
        self._condition = Condition()
        self._closed = False

    def close(self) -> None:
        """Reject new leases, close idle providers, defer active ones to release."""
        with self._condition:
            self._closed = True
            idle = [key for key, entry in self._providers.items() if entry.users == 0]
            registries = [self._providers.pop(key).registry for key in idle]
            self._condition.notify_all()
        close_all_preserving_error(registries)

    @contextmanager
    def open(
        self, catalog: str | None, target: str, *, context: ExecutionContext | None = None
    ) -> Generator[AccessContext]:
        context = context or new_execution_context()
        context.check_active()
        if catalog is None:
            raise ValueError("Catalog name is required to resolve a target")
        with self._condition:
            if self._closed:
                raise RuntimeError("Live catalog registry is closed")
        asset = self._store.get_asset_and_catalog(catalog=catalog, target=target)
        config = _catalog_config_for_asset(
            asset,
            plugin_registry=self._plugin_registry,
            path_enforcer=self._path_enforcer,
        )
        config = CatalogConfig(
            name=config.name,
            options=self._plugin_registry.options.for_plugin(config.plugin_id, config.options),
            path_enforcer=config.path_enforcer,
            plugin_id=config.plugin_id,
            revision=config.revision,
        )
        if self._secret_provider is not None:
            config = CatalogConfig(
                name=config.name,
                options=cast(
                    dict[str, Any],
                    resolve_secret_refs(
                        config.options,
                        provider=self._secret_provider,
                        expected_scope=f"catalog:{config.name}",
                    ),
                ),
                path_enforcer=config.path_enforcer,
                plugin_id=config.plugin_id,
                revision=config.revision,
            )
        # Resource revision is part of the public plugin handle identity. It is
        # not a deployment counter; unrelated asset/policy edits never affect it.
        key = sha256(
            json.dumps(
                {
                    "name": config.name,
                    "plugin_id": config.plugin_id,
                    "revision": config.revision,
                    "options": PluginOptions({config.plugin_id: config.options}).to_json()[
                        config.plugin_id
                    ],
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        entry = self._acquire(key, catalog, config, context)
        try:
            assert entry.registry is not None
            table_format = entry.registry.resolve(
                catalog, _asset_table_identifier(asset), context=context
            )
            scope = (
                table_format.open(context=context)
                if isinstance(table_format, PublicPluginTableFormat)
                else nullcontext(table_format)
            )
            with scope as source:
                schema = source.get_schema()
                _validate_schema_admission(asset, schema)
                yield AccessContext(source, PublishedPolicy(asset), schema)
        finally:
            self._release(key, entry)

    def _acquire(
        self, key: str, catalog: str, config: CatalogConfig, context: ExecutionContext
    ) -> _ProviderEntry:
        deadline = monotonic() + self._provider_wait_seconds
        while True:
            context.check_active()
            with self._condition:
                if self._closed:
                    raise RuntimeError("Live catalog registry is closed")
                entry = self._providers.get(key)
                if entry is not None and entry.registry is not None and not entry.retiring:
                    self._providers.move_to_end(key)
                    entry.users += 1
                    return entry
                if entry is None and len(self._providers) < self._max_cached_providers:
                    # Reserve capacity and coalesce same-key misses.
                    entry = _ProviderEntry(users=1)
                    self._providers[key] = entry
                    break
                idle_key = (
                    None
                    if entry is not None
                    else next((k for k, item in self._providers.items() if item.users == 0), None)
                )
                if idle_key is None:
                    remaining = min(
                        deadline - monotonic(),
                        (context.deadline - datetime.now(UTC)).total_seconds(),
                    )
                    if remaining <= 0:
                        context.check_active()
                        raise AccessContextUnavailable(
                            "Catalog provider capacity is unavailable; retry later"
                        )
                    self._condition.wait(
                        timeout=min(remaining, 0.1) if context.cancel_check else remaining
                    )
                    continue
                idle = self._providers[idle_key]
                idle.users = 1
                idle.retiring = True
            # Keep the retiring slot reserved until close completes, without
            # making other warm providers wait on potentially blocking IO.
            try:
                assert idle.registry is not None
                idle.registry.close()
            finally:
                with self._condition:
                    self._providers.pop(idle_key)
                    self._condition.notify_all()
        # Construct outside the lock so unrelated cold and warm requests proceed.
        try:
            registry = CatalogRegistry(
                ServiceConfig(catalogs={catalog: config}),
                plugin_registry=self._plugin_registry,
                context=context,
            )
        except BaseException:
            with self._condition:
                self._providers.pop(key)
                self._condition.notify_all()
            raise
        with self._condition:
            entry.registry = registry
            closed = self._closed
            self._condition.notify_all()
        if closed:
            self._release(key, entry)
            raise RuntimeError("Live catalog registry is closed")
        return entry

    def _release(self, key: str, entry: _ProviderEntry) -> None:
        with self._condition:
            entry.users -= 1
            close = self._closed and entry.users == 0
            if close:
                self._providers.pop(key)
            self._condition.notify_all()
        if close:
            assert entry.registry is not None
            close_preserving_error(entry.registry)


def _catalog_config_for_asset(
    asset: PublishedAssetSnapshot,
    *,
    plugin_registry: AdmittedPluginSnapshot | None = None,
    path_enforcer: PathRuleEnforcer | None = None,
) -> CatalogConfig:
    if plugin_registry is None:
        raise ValueError("Live plugin binding is unsupported without an admitted registry")
    admitted = plugin_registry.admitted()
    if ("catalog", asset.catalog.plugin_id) not in admitted or (
        "table_format",
        asset.source.backend,
    ) not in admitted:
        raise ValueError("Live plugin binding is not admitted")
    return CatalogConfig(
        name=asset.catalog.name,
        options=asset.catalog.options,
        plugin_id=asset.catalog.plugin_id,
        revision=asset.catalog.revision,
        path_enforcer=path_enforcer,
    )


def _asset_table_identifier(asset: PublishedAssetSnapshot) -> str:
    table = asset.source.table
    if not isinstance(table, str) or not table.strip():
        raise ValueError(f"Live asset {asset.catalog.name}/{asset.target} has no table identifier")
    return table


def _validate_schema_admission(asset: PublishedAssetSnapshot, schema: pa.Schema) -> None:
    """Rejects a live asset when an admitted field identity has drifted."""

    admission = asset.admitted_schema
    if admission is None or not admission.fields:
        _reject_unbound_broad_policy(asset, schema)
        return
    fields = admission.fields
    stable_ids = admission.stable_ids
    if stable_ids is True and not schema_has_stable_ids(schema):
        raise ValueError("Schema admission requires stable provider field IDs.")
    digest = admission.digest
    if digest is not None:
        if not isinstance(digest, str):
            raise ValueError("Stored schema admission digest is invalid")
        encoded = json.dumps(
            admission.to_json()["fields"], sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
        if sha256(encoded).hexdigest() != digest:
            raise ValueError("Stored schema admission digest does not match its fields")
    identities = _schema_identities(schema)
    for raw in fields:
        if not isinstance(raw, Mapping):
            raise ValueError("Stored schema admission is invalid")
        path = raw.get("path")
        field_id = raw.get("field_id")
        if (
            not isinstance(path, tuple)
            or not path
            or any(not isinstance(segment, str) for segment in path)
            or not isinstance(field_id, str)
        ):
            raise ValueError("Stored schema admission is invalid")
        identity = (cast(tuple[str, ...], path), field_id)
        actual = identities.get(identity)
        if actual is None:
            raise ValueError("Stored schema admission no longer matches the live table.")
        expected_type = raw.get("type")
        if (
            isinstance(expected_type, str)
            and expected_type
            and _canonical_type_name(expected_type) != _canonical_type_name(actual)
        ):
            raise ValueError(
                "Live schema field type no longer matches its admission. "
                f"Expected {expected_type!r}, live value is {actual!r}."
            )


def _reject_unbound_broad_policy(asset: PublishedAssetSnapshot, schema: pa.Schema) -> None:
    """Prevent unbound wildcard/parent grants from expanding on schema drift."""

    for rule in asset.policy.rules:
        if rule.effect == "allow_all":
            continue
        selectors = [*rule.columns, *rule.masks]
        for selector in selectors:
            if not isinstance(selector, str) or selector == "*":
                raise ValueError("Broad live policy requires schema admission.")
            try:
                field = resolve_schema_path(schema, parse_field_path(selector))
            except ValueError:
                continue
            if pa.types.is_nested(field.type):
                raise ValueError("Parent-field live policy requires schema admission.")


def _schema_identities(schema: pa.Schema) -> dict[tuple[tuple[str, ...], str], str]:
    result: dict[tuple[tuple[str, ...], str], str] = {}
    seen_ids: set[str] = set()
    scope_digest = schema_scope_digest(schema)

    for field, typed_path in walk_schema_fields(schema):
        path = tuple(
            segment.name
            if isinstance(segment, FieldSegment)
            else "$element"
            if isinstance(segment, ListElementSegment)
            else "$key"
            if isinstance(segment, MapKeySegment)
            else "$value"
            for segment in typed_path.segments
        )
        field_id, _stable = schema_field_id(field, path, scope_digest=scope_digest)
        if field_id in seen_ids:
            raise ValueError("Live schema contains a duplicate field identity")
        seen_ids.add(field_id)
        result[(path, field_id)] = str(field.type)

    return result


def _canonical_type_name(value: str) -> str:
    aliases = {
        "large_string": "string",
        "large_binary": "binary",
        "long": "int64",
        "integer": "int32",
        "float": "float32",
        "double": "double",
        "boolean": "bool",
    }
    normalized = value.strip().lower()
    return re.sub(
        r"\b(?:large_string|large_binary|long|integer|float|double|boolean)\b",
        lambda match: aliases[match.group(0)],
        normalized,
    )
