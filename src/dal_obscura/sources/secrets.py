from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import cast

ENV_SECRET_PROVIDER_MODULE = "dal_obscura.sources.secrets.EnvSecretProvider"


class SecretProvider(ABC):
    """Interface for resolving named secrets from external sources."""

    @abstractmethod
    def get_secret(self, key: str) -> str | None:
        """Returns the secret value for `key`, or `None` when it is unavailable."""

    @abstractmethod
    def is_allowed(self, key: str, scope: str) -> bool:
        """Returns whether the operator granted ``key`` to ``scope``.

        Implementations must enforce authorization for every reference.
        """


class EnvSecretProvider(SecretProvider):
    """Secret provider that reads secrets from environment variables."""

    def __init__(
        self,
        *,
        config: Mapping[str, object] | None = None,
    ) -> None:
        raw_prefix = config.get("prefix") if config is not None else None
        self._prefix = str(raw_prefix if raw_prefix is not None else "")
        self._scope_grants = _parse_scope_grants(config.get("scope_grants") if config else None)

    def get_secret(self, key: str) -> str | None:
        return os.getenv(f"{self._prefix}{key}")

    def is_allowed(self, key: str, scope: str) -> bool:
        if self._scope_grants is None:
            return False
        return key in self._scope_grants.get(scope, frozenset())


@dataclass(frozen=True)
class SecretProviderContext:
    """Host-level context passed to a module-loaded secret provider."""

    database_url: str


@dataclass(frozen=True)
class SecretProviderConfig:
    """Module and config used to instantiate a secret provider."""

    module: str = ENV_SECRET_PROVIDER_MODULE
    config: dict[str, object] = field(default_factory=dict)


def load_secret_provider(
    provider_config: SecretProviderConfig,
    *,
    context: SecretProviderContext,
) -> SecretProvider:
    """Builds the fixed environment provider from startup-only configuration."""
    del context
    if provider_config.module != ENV_SECRET_PROVIDER_MODULE:
        raise ValueError("Unsupported secret provider; only environment secrets are supported")
    return EnvSecretProvider(config=provider_config.config)


def load_secret_provider_from_environment(
    environment: Mapping[str, str] | None = None,
    *,
    require_scope_grants: bool = False,
) -> SecretProvider:
    """Load the admitted environment-backed provider for control-plane callers.

    The control plane must use the same startup-selected provider configuration
    as the data plane when it resolves catalog references. This helper keeps
    environment parsing in one place while retaining the explicit provider
    module allowlist enforced by :func:`load_secret_provider`.
    """

    source = os.environ if environment is None else environment
    if "DAL_OBSCURA_SECRET_PROVIDER_SECRETS" in source:
        raise ValueError("DAL_OBSCURA_SECRET_PROVIDER_SECRETS is unsupported")
    module = source.get("DAL_OBSCURA_SECRET_PROVIDER_MODULE", ENV_SECRET_PROVIDER_MODULE).strip()
    config = _json_object_value(
        source.get("DAL_OBSCURA_SECRET_PROVIDER_CONFIG"), "DAL_OBSCURA_SECRET_PROVIDER_CONFIG"
    )
    scope_grants = config.get("scope_grants")
    if require_scope_grants and (not isinstance(scope_grants, Mapping) or not scope_grants):
        raise ValueError(
            "Production requires non-empty DAL_OBSCURA_SECRET_PROVIDER_CONFIG scope_grants"
        )
    return load_secret_provider(
        SecretProviderConfig(module=module, config=config),
        context=SecretProviderContext(database_url="control-plane"),
    )


def resolve_secret_refs(
    value: object,
    *,
    provider: SecretProvider,
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
            if not callable(is_allowed):
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
    if isinstance(value, list):
        return [
            resolve_secret_refs(item, provider=provider, expected_scope=expected_scope)
            for item in value
        ]
    return value


def _json_object_env(name: str) -> dict[str, object]:
    return _json_object_value(os.getenv(name), name)


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
        if not isinstance(raw_keys, list) or any(
            not isinstance(key, str) or not key.strip() for key in raw_keys
        ):
            raise ValueError(f"Secret provider scope grant {raw_scope!r} must list secret names")
        grants[raw_scope] = frozenset(cast(list[str], raw_keys))
    return grants
