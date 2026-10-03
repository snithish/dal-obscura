from __future__ import annotations

import json
import re
from collections import OrderedDict
from collections.abc import Iterable, Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from hashlib import sha256
from threading import Condition
from time import monotonic
from typing import Any, cast

import pyarrow as pa

from dal_obscura.policy.models import AccessDecision, AssetPolicy, Policy, Principal
from dal_obscura.policy.paths import (
    FieldSegment,
    ListElementSegment,
    MapKeySegment,
    parse_field_path,
    resolve_schema_path,
)
from dal_obscura.policy.policy_resolution import resolve_access
from dal_obscura.policy.schema_identity import (
    schema_field_id,
    schema_has_stable_ids,
    schema_scope_digest,
)
from dal_obscura.policy.schema_index import walk_schema_fields
from dal_obscura.sources.access import (
    AccessContext,
    AccessContextUnavailable,
)
from dal_obscura.sources.catalogs import (
    CatalogConfig,
    CatalogRegistry,
    ServiceConfig,
)
from dal_obscura.sources.contracts import Source
from dal_obscura.sources.paths import PathRuleEnforcer
from dal_obscura.sources.plugin_runtime import (
    PublicPluginTableFormat,
    _BoundSource,
    _close_plugin_preserving_error,
)
from dal_obscura.sources.plugins import PluginRegistry
from dal_obscura.sources.secrets import (
    SecretProvider,
    resolve_secret_refs,
)
from dal_obscura.storage.snapshots import (
    AdmittedPluginSnapshot,
    LiveAsset,
    LiveCatalog,
    LiveConfigStore,
    _mapping,
)


def _authorize_asset(
    asset: LiveAsset,
    principal: Principal,
    target: str,
    catalog: str | None,
    requested_columns: Iterable[str],
) -> AccessDecision:
    if catalog != asset.catalog or target != asset.target:
        raise ValueError("Authorization context does not match the requested asset")
    policy = _policy_from_asset(asset)
    allowed_columns, masks, row_filter = resolve_access(
        policy, principal, target, catalog, requested_columns
    )
    return AccessDecision(
        allowed_columns=allowed_columns,
        masks=masks,
        row_filter=row_filter,
        policy_version=_effective_policy_version(asset),
        asset_id=None if asset.asset_id is None else str(asset.asset_id),
    )


@dataclass(frozen=True)
class PublishedPolicy:
    asset: LiveAsset

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
        store: LiveConfigStore,
        *,
        secret_provider: SecretProvider | None = None,
        plugin_registry: PluginRegistry | AdmittedPluginSnapshot | None = None,
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
        first_error: Exception | None = None
        for registry in registries:
            try:
                assert registry is not None
                registry.close()
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error

    @contextmanager
    def open(self, catalog: str | None, target: str) -> Iterator[AccessContext]:
        if catalog is None:
            raise ValueError("Catalog name is required to resolve a target")
        with self._condition:
            if self._closed:
                raise RuntimeError("Live catalog registry is closed")
        asset, live_catalog = self._store.get_asset_and_catalog(catalog=catalog, target=target)
        config = _catalog_config_for_asset(
            live_catalog,
            asset,
            plugin_registry=self._plugin_registry,
            path_enforcer=self._path_enforcer,
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
                    "options": config.options,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        entry = self._acquire(key, catalog, config)
        try:
            assert entry.registry is not None
            table_format = entry.registry.describe(catalog, _asset_table_identifier(asset))
            scope = (
                table_format.open()
                if isinstance(table_format, PublicPluginTableFormat)
                else nullcontext(table_format)
            )
            with scope as source:
                schema = source.get_schema()
                _validate_schema_admission(asset, schema)
                yield AccessContext(source, PublishedPolicy(asset), schema)
        finally:
            self._release(key, entry)

    def describe(self, catalog: str | None, target: str) -> Source:
        """Resolve a detached table format for direct catalog consumers."""
        with self.open(catalog, target) as context:
            source = context.table_format
            return source.source if isinstance(source, _BoundSource) else source

    def _acquire(self, key: str, catalog: str, config: CatalogConfig) -> _ProviderEntry:
        deadline = monotonic() + self._provider_wait_seconds
        while True:
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
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        raise AccessContextUnavailable(
                            "Catalog provider capacity is unavailable; retry later"
                        )
                    self._condition.wait(timeout=remaining)
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
                plugin_registry=self._plugin_registry
                if isinstance(self._plugin_registry, PluginRegistry)
                else None,
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
            _close_plugin_preserving_error(entry.registry)


def _effective_policy_version(asset: LiveAsset) -> int:
    """The policy resource's CAS revision, without global invalidation."""
    return asset.policy_version


def _policy_from_asset(asset: LiveAsset) -> Policy:
    return AssetPolicy.from_json(_mapping(asset.compiled_config.get("policy"))).to_policy()


def _catalog_config_from_live_catalog(
    catalog: LiveCatalog,
    *,
    plugin_id: str,
    path_enforcer: PathRuleEnforcer | None = None,
) -> CatalogConfig:
    config = _mapping(catalog.config)
    if set(config) - {"plugin_id", "options", "revision"}:
        raise ValueError("Catalog config contains unsupported fields")
    options = dict(_mapping(config.get("options")))
    raw_revision = config.get("revision")
    revision = (
        raw_revision
        if isinstance(raw_revision, int)
        and not isinstance(raw_revision, bool)
        and raw_revision >= 0
        else catalog.plugin_revision
        if catalog.plugin_revision is not None
        else 0
    )
    return CatalogConfig(
        name=catalog.catalog,
        options=options,
        path_enforcer=path_enforcer,
        plugin_id=plugin_id,
        revision=revision,
    )


def _catalog_config_for_asset(
    catalog: LiveCatalog,
    asset: LiveAsset,
    *,
    plugin_registry: PluginRegistry | AdmittedPluginSnapshot | None = None,
    path_enforcer: PathRuleEnforcer | None = None,
) -> CatalogConfig:
    """Build the runtime catalog config with the live asset as its source of truth."""
    _validate_plugin_binding(asset, plugin_registry=plugin_registry)
    raw_plugins = asset.compiled_config.get("plugins")
    catalog_plugin_id = catalog.plugin_id or ""
    if isinstance(raw_plugins, dict) and isinstance(raw_plugins.get("catalog"), str):
        catalog_plugin_id = str(raw_plugins["catalog"])
    config = _catalog_config_from_live_catalog(
        catalog, plugin_id=catalog_plugin_id, path_enforcer=path_enforcer
    )
    target = _mapping(asset.compiled_config.get("target"))
    backend = str(target.get("backend") or asset.backend).lower()
    if config.plugin_id == "iceberg.sql":
        if backend != "iceberg":
            raise ValueError("Configured Iceberg catalogs require Iceberg assets")
        return config
    if plugin_registry is None:
        raise ValueError("Configured external catalog plugins require an admitted registry")
    return config


def _validate_plugin_binding(
    asset: LiveAsset,
    *,
    plugin_registry: PluginRegistry | AdmittedPluginSnapshot | None = None,
) -> None:
    """Rejects plugin identities the configured runtime cannot honor."""

    raw_plugins = asset.compiled_config.get("plugins")
    if not isinstance(raw_plugins, dict):
        raise ValueError("Live plugin binding is missing")
    catalog_plugin = raw_plugins.get("catalog")
    format_plugin = raw_plugins.get("table_format")
    if not isinstance(catalog_plugin, str) or not isinstance(format_plugin, str):
        raise ValueError("Live plugin binding is unsupported")
    if catalog_plugin == "iceberg.sql" and format_plugin != "iceberg":
        raise ValueError("Live plugin binding is unsupported")
    if catalog_plugin != "iceberg.sql" and plugin_registry is None:
        raise ValueError("Live plugin binding is unsupported without an admitted registry")
    if plugin_registry is not None:
        admitted = plugin_registry.admitted()
        if ("catalog", catalog_plugin) not in admitted or (
            "table_format",
            format_plugin,
        ) not in admitted:
            raise ValueError("Live plugin binding is not admitted")


def _asset_table_identifier(asset: LiveAsset) -> str:
    target = _mapping(asset.compiled_config.get("target"))
    table = target.get("table")
    if not isinstance(table, str) or not table.strip():
        raise ValueError(f"Live asset {asset.catalog}/{asset.target} has no table identifier")
    return table


def _validate_schema_admission(asset: LiveAsset, schema: pa.Schema) -> None:
    """Rejects a live asset when an admitted field identity has drifted."""

    admission = _mapping(asset.compiled_config.get("schema"))
    fields = admission.get("fields")
    if not isinstance(fields, list) or not fields:
        _reject_unbound_broad_policy(asset, schema)
        return
    stable_ids = admission.get("stable_ids")
    if stable_ids is True and not schema_has_stable_ids(schema):
        raise ValueError("Schema admission requires stable provider field IDs.")
    digest = admission.get("digest")
    if digest is not None:
        if not isinstance(digest, str):
            raise ValueError("Stored schema admission digest is invalid")
        encoded = json.dumps(fields, sort_keys=True, default=str, separators=(",", ":")).encode(
            "utf-8"
        )
        if sha256(encoded).hexdigest() != digest:
            raise ValueError("Stored schema admission digest does not match its fields")
    identities = _schema_identities(schema)
    for raw in fields:
        if not isinstance(raw, dict):
            raise ValueError("Stored schema admission is invalid")
        path = raw.get("path")
        field_id = raw.get("field_id")
        if (
            not isinstance(path, list)
            or not path
            or any(not isinstance(segment, str) for segment in path)
            or not isinstance(field_id, str)
        ):
            raise ValueError("Stored schema admission is invalid")
        identity = (tuple(path), field_id)
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


def _reject_unbound_broad_policy(asset: LiveAsset, schema: pa.Schema) -> None:
    """Prevent unbound wildcard/parent grants from expanding on schema drift."""

    policy = asset.compiled_config.get("policy")
    if not isinstance(policy, dict):
        return
    rules = policy.get("rules")
    if not isinstance(rules, list):
        return
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        if rule.get("effect") == "allow_all":
            # Explicit bypass intentionally includes every current/future column.
            continue
        selectors: list[object] = []
        columns = rule.get("columns")
        if isinstance(columns, list):
            selectors.extend(columns)
        masks = rule.get("masks")
        if isinstance(masks, dict):
            selectors.extend(masks)
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
