"""Startup secret backend binding and per-reference scope authorization."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import cast

from dal_obscura_plugin_api import ExecutionContext
from dal_obscura_plugin_api.execution import close_preserving_error
from dal_obscura_plugin_api.options import PluginOptions
from dal_obscura_plugin_api.secrets import SecretBackend

from dal_obscura.plugins import PluginRegistry
from dal_obscura.plugins.bootstrap import create_plugin_registry


class SecretProvider:
    """Authorize references with operator grants independently of backend code."""

    def __init__(self, backend: SecretBackend, scope_grants: object) -> None:
        self._backend = backend
        self._scope_grants = _parse_scope_grants(scope_grants)

    def get_secret(self, key: str) -> str | None:
        return self._backend.get_secret(key)

    def is_allowed(self, key: str, scope: str) -> bool:
        return self._scope_grants is not None and key in self._scope_grants.get(scope, frozenset())

    def close(self) -> None:
        self._backend.close()


@dataclass(frozen=True)
class SecretProviderConfig:
    """Fixed provider kind and startup configuration."""

    kind: str = "environment"
    config: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "config", PluginOptions({self.kind: self.config}).for_plugin(self.kind)
        )


def load_secret_provider(
    provider_config: SecretProviderConfig,
    *,
    registry: PluginRegistry | None = None,
) -> SecretProvider:
    """Bind one admitted backend at startup; references never select factories."""
    registry = registry if registry is not None else create_plugin_registry()
    options = dict(provider_config.config)
    grants = options.pop("scope_grants", None)
    _parse_scope_grants(grants)
    context = ExecutionContext(datetime.now(UTC) + timedelta(seconds=30), "secret-backend-startup")
    backend = registry.load("secret_backend", provider_config.kind)(options, context)
    try:
        context.check_active()
        if (
            backend.descriptor.kind != "secret_backend"
            or backend.descriptor.plugin_id != provider_config.kind
        ):
            raise ValueError("Secret backend returned a mismatched descriptor")
        if not all(callable(getattr(backend, name, None)) for name in ("get_secret", "close")):
            raise ValueError("Secret backend does not implement the SDK lifecycle")
        return SecretProvider(backend, grants)
    except BaseException:
        close_preserving_error(backend)
        raise


def load_secret_provider_from_environment(
    environment: Mapping[str, str] | None = None,
    *,
    require_scope_grants: bool = False,
    registry: PluginRegistry | None = None,
) -> SecretProvider:
    """Bind the startup backend and plugin options from one environment mapping."""

    provider_config = secret_provider_config_from_environment(environment)
    scope_grants = provider_config.config.get("scope_grants")
    if require_scope_grants and (not isinstance(scope_grants, Mapping) or not scope_grants):
        raise ValueError(
            "Production requires non-empty DAL_OBSCURA_SECRET_PROVIDER_CONFIG scope_grants"
        )
    registry = registry if registry is not None else create_plugin_registry(environment=environment)
    return load_secret_provider(provider_config, registry=registry)


def secret_provider_config_from_environment(
    environment: Mapping[str, str] | None = None,
) -> SecretProviderConfig:
    """Parse the same admitted startup configuration for both service planes."""
    source = os.environ if environment is None else environment
    if "DAL_OBSCURA_SECRET_PROVIDER_SECRETS" in source:
        raise ValueError("DAL_OBSCURA_SECRET_PROVIDER_SECRETS is unsupported")
    if "DAL_OBSCURA_SECRET_PROVIDER_MODULE" in source:
        raise ValueError("DAL_OBSCURA_SECRET_PROVIDER_MODULE is unsupported; use provider KIND")
    kind = source.get("DAL_OBSCURA_SECRET_PROVIDER_KIND", "environment").strip()
    if not re.fullmatch(r"[a-z][a-z0-9_.-]{0,63}", kind):
        raise ValueError("DAL_OBSCURA_SECRET_PROVIDER_KIND must be a plugin ID")
    config = _json_object_value(
        source.get("DAL_OBSCURA_SECRET_PROVIDER_CONFIG"), "DAL_OBSCURA_SECRET_PROVIDER_CONFIG"
    )
    return SecretProviderConfig(kind=kind, config=config)


def resolve_secret_refs(
    value: object,
    *,
    provider: SecretProvider | None,
    expected_scope: str | None = None,
) -> object:
    """Recursively resolves explicit secret references in provider configuration.

    Every reference must carry an explicit ``scope``. It is usable only by the
    matching caller scope; callers that do not declare a scope fail closed
    instead of silently widening the reference.
    """
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        secret_key = mapping.get("secret")
        reference_scope = mapping.get("scope")
        # A mapping that names ``secret`` is always a secret reference. Never
        # let malformed values fall through as ordinary provider options: that
        # would make validation and resolution disagree at the security
        # boundary.
        if "secret" in mapping and not isinstance(secret_key, Mapping):
            if set(mapping) != {"secret", "scope"} or not isinstance(secret_key, str):
                raise ValueError("Secret reference scope is required")
            if not secret_key.strip():
                raise ValueError("Secret reference name is required")
            if (
                not isinstance(reference_scope, str)
                or not reference_scope.strip()
                or expected_scope is None
                or reference_scope != expected_scope
            ):
                raise ValueError("Secret reference scope does not match the requesting scope")
            is_allowed = getattr(provider, "is_allowed", None)
            if provider is None or not callable(is_allowed):
                raise ValueError("Secret provider does not implement scope authorization")
            if not is_allowed(secret_key, reference_scope):
                raise ValueError("Secret reference is not granted to the requesting scope")
            resolved = provider.get_secret(secret_key)
            if resolved is None or not resolved:
                raise ValueError(f"Secret {secret_key!r} could not be resolved")
            return resolved
        return {
            str(key): resolve_secret_refs(nested, provider=provider, expected_scope=expected_scope)
            for key, nested in mapping.items()
        }
    if isinstance(value, (list, tuple)):
        return [
            resolve_secret_refs(item, provider=provider, expected_scope=expected_scope)
            for item in value
        ]
    return value


def _json_object_value(raw: str | None, name: str) -> dict[str, object]:
    if raw is None or not raw.strip():
        return {}
    value = json.loads(raw, object_pairs_hook=_unique_json_object)
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return {str(key): item for key, item in value.items()}


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate keys so secret scope policy cannot be overwritten."""

    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _parse_scope_grants(raw: object) -> dict[str, frozenset[str]] | None:
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise ValueError("Secret provider scope_grants must be a JSON object")
    grants: dict[str, frozenset[str]] = {}
    for raw_scope, raw_keys in raw.items():
        if not isinstance(raw_scope, str) or not raw_scope.strip():
            raise ValueError("Secret provider scope grant names must be non-empty strings")
        if not isinstance(raw_keys, (list, tuple)) or any(
            not isinstance(key, str) or not key.strip() for key in raw_keys
        ):
            raise ValueError(f"Secret provider scope grant {raw_scope!r} must list secret names")
        grants[raw_scope] = frozenset(cast(list[str], raw_keys))
    return grants
