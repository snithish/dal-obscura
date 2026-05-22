from __future__ import annotations

import importlib
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
    """Loads one explicitly configured secret provider module."""
    provider_cls = _load_class(provider_config.module)
    provider = provider_cls(
        database_url=context.database_url,
        cell_id=context.cell_id,
        config=provider_config.config,
        secrets=_resolve_bootstrap_secrets(provider_config.secrets),
    )
    get_secret = getattr(provider, "get_secret", None)
    if not callable(get_secret):
        raise ValueError(f"Secret provider {provider_config.module!r} must define get_secret(key)")
    return cast(SecretProvider, provider)


def resolve_secret_refs(value: object, *, provider: SecretProvider) -> object:
    """Recursively resolves explicit secret references in provider configuration."""
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        secret_key = mapping.get("secret")
        if set(mapping) == {"secret"} and isinstance(secret_key, str):
            resolved = provider.get_secret(secret_key)
            if resolved is None or not resolved:
                raise ValueError(f"Secret {secret_key!r} could not be resolved")
            return resolved
        return {
            str(key): resolve_secret_refs(nested, provider=provider)
            for key, nested in mapping.items()
        }
    if isinstance(value, list):
        return [resolve_secret_refs(item, provider=provider) for item in value]
    return value


def _load_class(module_path: str) -> type:
    module_name, class_name = module_path.rsplit(".", 1)
    module = importlib.import_module(module_name)
    provider_cls = getattr(module, class_name, None)
    if not isinstance(provider_cls, type):
        raise ValueError(f"Secret provider {module_path!r} must be a class")
    return provider_cls


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
