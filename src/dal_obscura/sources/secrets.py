from __future__ import annotations

import json
import os
import re
import stat
from abc import ABC, abstractmethod
from collections.abc import Mapping
from contextlib import ExitStack
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

_MOUNTED_SECRET_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,254}\Z")
MAX_MOUNTED_SECRET_BYTES = 64 * 1024


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


class MountedFileSecretProvider(SecretProvider):
    """Read scoped flat files from a fixed operator-owned POSIX mount."""

    def __init__(self, *, config: Mapping[str, object]) -> None:
        if set(config) - {"directory", "scope_grants"}:
            raise ValueError(
                "Mounted secret configuration supports directory and scope_grants only"
            )
        directory = config.get("directory")
        if not isinstance(directory, str) or not Path(directory).is_absolute():
            raise ValueError("Mounted secret directory must be an absolute directory")
        if os.open not in os.supports_dir_fd or not all(
            hasattr(os, flag) for flag in ("O_DIRECTORY", "O_NOFOLLOW", "O_NONBLOCK")
        ):
            raise ValueError("Mounted secrets require secure POSIX file operations")
        try:
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                root = os.fstat(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            raise ValueError("Mounted secret directory is unavailable") from None
        self._directory = Path(directory)
        self._root_identity = (root.st_dev, root.st_ino)
        self._scope_grants = _parse_scope_grants(config.get("scope_grants"))
        if self._scope_grants and any(
            not _MOUNTED_SECRET_NAME.fullmatch(key)
            for keys in self._scope_grants.values()
            for key in keys
        ):
            raise ValueError("Mounted secret grants must use flat filenames")

    def get_secret(self, key: str) -> str | None:
        if not _MOUNTED_SECRET_NAME.fullmatch(key):
            return None
        try:
            with ExitStack() as opened:
                directory_fd = os.open(
                    self._directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
                )
                opened.callback(os.close, directory_fd)
                root = os.fstat(directory_fd)
                if (root.st_dev, root.st_ino) != self._root_identity:
                    return None
                secret_fd = os.open(
                    key, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd
                )
                opened.callback(os.close, secret_fd)
                info = os.fstat(secret_fd)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink > 1:
                    return None
                payload = bytearray()
                while len(payload) <= MAX_MOUNTED_SECRET_BYTES:
                    chunk = os.read(secret_fd, MAX_MOUNTED_SECRET_BYTES + 1 - len(payload))
                    if not chunk:
                        break
                    payload.extend(chunk)
                if not payload or len(payload) > MAX_MOUNTED_SECRET_BYTES:
                    return None
                return payload.decode("utf-8")
        except (OSError, UnicodeError):
            return None

    def is_allowed(self, key: str, scope: str) -> bool:
        return self._scope_grants is not None and key in self._scope_grants.get(scope, frozenset())


@dataclass(frozen=True)
class SecretProviderConfig:
    """Fixed provider kind and startup configuration."""

    kind: str = "environment"
    config: dict[str, object] = field(default_factory=dict)


def load_secret_provider(provider_config: SecretProviderConfig) -> SecretProvider:
    """Construct one fixed provider without dynamic module loading."""
    if provider_config.kind == "environment":
        return EnvSecretProvider(config=provider_config.config)
    if provider_config.kind == "mounted_file":
        return MountedFileSecretProvider(config=provider_config.config)
    raise ValueError("Unsupported secret provider kind")


def load_secret_provider_from_environment(
    environment: Mapping[str, str] | None = None,
    *,
    require_scope_grants: bool = False,
) -> SecretProvider:
    """Load the admitted startup-selected provider for control-plane callers.

    The control plane must use the same startup-selected provider configuration
    as the data plane when it resolves catalog references. This helper keeps
    environment parsing in one place while retaining the explicit provider
    kind allowlist enforced by :func:`load_secret_provider`.
    """

    provider_config = secret_provider_config_from_environment(environment)
    scope_grants = provider_config.config.get("scope_grants")
    if require_scope_grants and (not isinstance(scope_grants, Mapping) or not scope_grants):
        raise ValueError(
            "Production requires non-empty DAL_OBSCURA_SECRET_PROVIDER_CONFIG scope_grants"
        )
    return load_secret_provider(provider_config)


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
    if kind not in {"environment", "mounted_file"}:
        raise ValueError("DAL_OBSCURA_SECRET_PROVIDER_KIND must be environment or mounted_file")
    config = _json_object_value(
        source.get("DAL_OBSCURA_SECRET_PROVIDER_CONFIG"), "DAL_OBSCURA_SECRET_PROVIDER_CONFIG"
    )
    return SecretProviderConfig(kind=kind, config=config)


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
