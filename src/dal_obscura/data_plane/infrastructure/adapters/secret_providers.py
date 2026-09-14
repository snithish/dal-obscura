from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast
from uuid import UUID

ENV_SECRET_PROVIDER_MODULE = (
    "dal_obscura.data_plane.infrastructure.adapters.secret_providers.EnvSecretProvider"
)


class SecretProvider(ABC):
    """Interface for resolving named secrets from external sources."""

    @abstractmethod
    def get_secret(self, key: str) -> str | None:
        """Returns the secret value for `key`, or `None` when it is unavailable."""


class EnvSecretProvider(SecretProvider):
    """Secret provider that reads secrets from environment variables."""

    def __init__(
        self,
        *,
        prefix: str = "",
        config: Mapping[str, object] | None = None,
        secrets: Mapping[str, str] | None = None,
        **_: object,
    ) -> None:
        del secrets
        raw_prefix = config.get("prefix") if config is not None else None
        self._prefix = str(raw_prefix if raw_prefix is not None else prefix)

    def get_secret(self, key: str) -> str | None:
        return os.getenv(f"{self._prefix}{key}")


@dataclass(frozen=True)
class SecretProviderContext:
    """Host-level context passed to a module-loaded secret provider."""

    database_url: str
    cell_id: UUID


@dataclass(frozen=True)
class SecretProviderConfig:
    """Module and bootstrap configuration used to instantiate a secret provider."""

    module: str = ENV_SECRET_PROVIDER_MODULE
    config: dict[str, object] = field(default_factory=dict)
    secrets: dict[str, object] = field(default_factory=dict)


def load_secret_provider(
    provider_config: SecretProviderConfig,
    *,
    context: SecretProviderContext,
) -> SecretProvider:
    """Builds the fixed environment provider from startup-only configuration."""
    del context
    if provider_config.module != ENV_SECRET_PROVIDER_MODULE:
        raise ValueError("Unsupported secret provider; only environment secrets are supported")
    if provider_config.secrets:
        _resolve_bootstrap_secrets(provider_config.secrets)
    return EnvSecretProvider(config=provider_config.config)


def load_secret_provider_from_environment(
    environment: Mapping[str, str] | None = None,
) -> SecretProvider:
    """Load the admitted environment-backed provider for control-plane callers.

    The control plane must use the same startup-selected provider configuration
    as the data plane when it resolves catalog references. This helper keeps
    environment parsing in one place while retaining the explicit provider
    module allowlist enforced by :func:`load_secret_provider`.
    """

    source = os.environ if environment is None else environment
    module = source.get("DAL_OBSCURA_SECRET_PROVIDER_MODULE", ENV_SECRET_PROVIDER_MODULE).strip()
    config = _json_object_value(
        source.get("DAL_OBSCURA_SECRET_PROVIDER_CONFIG"),
        "DAL_OBSCURA_SECRET_PROVIDER_CONFIG",
    )
    secrets = _json_object_value(
        source.get("DAL_OBSCURA_SECRET_PROVIDER_SECRETS"),
        "DAL_OBSCURA_SECRET_PROVIDER_SECRETS",
    )
    return load_secret_provider(
        SecretProviderConfig(module=module, config=config, secrets=secrets),
        context=SecretProviderContext(database_url="control-plane", cell_id=UUID(int=0)),
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
        if isinstance(secret_key, str) and set(mapping) != {"secret", "scope"}:
            raise ValueError("Secret reference scope is required")
        if set(mapping) == {"secret", "scope"} and isinstance(secret_key, str):
            if (
                not isinstance(reference_scope, str)
                or not reference_scope.strip()
                or expected_scope is None
                or reference_scope != expected_scope
            ):
                raise ValueError("Secret reference scope does not match the requesting scope")
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


def _resolve_bootstrap_secrets(raw: Mapping[str, object]) -> dict[str, str]:
    return {str(name): _resolve_bootstrap_secret(str(name), value) for name, value in raw.items()}


def _resolve_bootstrap_secret(name: str, value: object) -> str:
    if isinstance(value, str):
        if not value:
            raise ValueError(f"Missing bootstrap secret {name!r}")
        return value
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        env_name = mapping.get("env")
        if set(mapping) == {"env"} and isinstance(env_name, str):
            secret = os.getenv(env_name)
            if secret is None or not secret:
                raise ValueError(f"Missing bootstrap secret {name!r} from env {env_name!r}")
            return secret
        file_path = mapping.get("file")
        if set(mapping) == {"file"} and isinstance(file_path, str):
            secret = Path(file_path).read_text(encoding="utf-8").removesuffix("\n")
            if not secret:
                raise ValueError(f"Missing bootstrap secret {name!r} from file {file_path!r}")
            return secret
    raise ValueError(
        f"Bootstrap secret {name!r} must be a string, {{'env': 'NAME'}}, or {{'file': 'PATH'}}"
    )


def _json_object_env(name: str) -> dict[str, object]:
    return _json_object_value(os.getenv(name), name)


def _json_object_value(raw: str | None, name: str) -> dict[str, object]:
    if raw is None or not raw.strip():
        return {}
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be a JSON object")
    return {str(key): item for key, item in value.items()}
