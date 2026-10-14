"""Startup-selected secret backend contracts; authorization belongs to the host."""

from collections.abc import Mapping
from typing import Protocol

from dal_obscura_plugin_api.contracts import ExecutionContext, PluginDescriptor


class SecretBackend(Protocol):
    descriptor: PluginDescriptor

    def get_secret(self, key: str) -> str | None: ...
    def close(self) -> None: ...


class SecretBackendFactory(Protocol):
    def __call__(
        self, options: Mapping[str, object], context: ExecutionContext
    ) -> SecretBackend: ...
