from __future__ import annotations

import threading
from collections.abc import Callable
from importlib import metadata
from typing import cast

from dal_obscura.domain.format_handler.ports import FormatHandler

_FORMAT_PLUGIN_GROUP = "dal_obscura.format_handlers"


class DynamicFormatRegistry:
    """Stores format-specific handlers and looks them up by format name."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._handlers: dict[str, FormatHandler] = {}

        self._register_builtin_handlers()
        self._register_entrypoints()

    def register_handler(
        self, format_name: str, implementation: FormatHandler | Callable[[], FormatHandler]
    ) -> None:
        """Registers a handler factory for a specific data format."""
        handler = _coerce_format_handler(implementation)
        with self._lock:
            self._handlers[format_name] = handler

    def get_handler(self, format_name: str) -> FormatHandler:
        """Looks up the handler instance for a given format string."""
        with self._lock:
            handler = self._handlers.get(format_name)
            if handler is None:
                raise ValueError(f"Format {format_name!r} is unsupported")
            return handler

    def _register_builtin_handlers(self) -> None:
        """Registers the built-in handlers available without plugins."""
        try:
            from dal_obscura.infrastructure.adapters.iceberg_handler import IcebergHandler

            self.register_handler("iceberg", IcebergHandler)
        except ImportError:
            pass

    def _register_entrypoints(self) -> None:
        """Loads optional format handler plugins from Python entry points."""
        for entrypoint in _entry_points_for_group(_FORMAT_PLUGIN_GROUP):
            self.register_handler(entrypoint.name, entrypoint.load())


def _coerce_format_handler(
    candidate: FormatHandler | Callable[[], FormatHandler] | type,
) -> FormatHandler:
    """Instantiates or validates a format handler registered by the runtime."""
    if isinstance(candidate, type):
        instance = candidate()
        if (
            hasattr(instance, "plan")
            and hasattr(instance, "execute")
            and hasattr(instance, "get_schema")
        ):
            return cast(FormatHandler, instance)

    # We duck-type against FormatHandler protocol roughly
    if (
        hasattr(candidate, "plan")
        and hasattr(candidate, "execute")
        and hasattr(candidate, "get_schema")
    ):
        return cast(FormatHandler, candidate)

    if callable(candidate):
        instance = candidate()
        if hasattr(instance, "plan") and hasattr(instance, "execute"):
            return cast(FormatHandler, instance)

    raise TypeError(
        "Format handler implementation must provide get_schema(), plan(), and execute()"
    )


def _entry_points_for_group(group: str):
    """Handles `importlib.metadata.entry_points()` API differences across Python versions."""
    discovered = metadata.entry_points()
    if hasattr(discovered, "select"):
        return list(discovered.select(group=group))
    return list(discovered.get(group, []))
