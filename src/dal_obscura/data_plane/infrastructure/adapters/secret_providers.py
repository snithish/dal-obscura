from __future__ import annotations

import os
from abc import ABC, abstractmethod


class SecretProvider(ABC):
    """Interface for resolving named secrets from external sources."""

    @abstractmethod
    def get_secret(self, key: str) -> str | None:
        """Returns the secret value for `key`, or `None` when it is unavailable."""


class EnvSecretProvider(SecretProvider):
    """Secret provider that reads secrets from environment variables."""

    def __init__(self, *, prefix: str = "") -> None:
        self._prefix = prefix

    def get_secret(self, key: str) -> str | None:
        return os.getenv(f"{self._prefix}{key}")
