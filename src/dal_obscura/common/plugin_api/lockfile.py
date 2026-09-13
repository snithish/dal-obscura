"""Bounded loading of operator-mounted immutable plugin locks."""

from __future__ import annotations

import json
import stat
from pathlib import Path
from typing import cast

from dal_obscura.common.plugin_api.contracts import PluginKind
from dal_obscura.common.plugin_api.registry import PluginAdmissionError, PluginLock

MAX_PLUGIN_LOCK_BYTES = 1_048_576
MAX_PLUGIN_LOCK_ENTRIES = 256


def load_plugin_lock_file(path: str | Path) -> dict[tuple[PluginKind, str], PluginLock]:  # noqa: C901
    """Read a bounded lock file without importing any plugin factories."""

    lock_path = Path(path)
    if not str(lock_path).strip():
        raise PluginAdmissionError("Plugin lock path must be non-empty")
    try:
        if lock_path.is_symlink():
            raise PluginAdmissionError("Plugin lock must not be a symbolic link")
        mode = lock_path.stat().st_mode
        if mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise PluginAdmissionError("Plugin lock must not be group- or world-writable")
        raw = lock_path.read_bytes()
    except PluginAdmissionError:
        raise
    except OSError as exc:
        raise PluginAdmissionError("Plugin lock is unavailable") from exc
    if len(raw) > MAX_PLUGIN_LOCK_BYTES:
        raise PluginAdmissionError("Plugin lock is too large")
    try:
        document = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PluginAdmissionError("Plugin lock is not valid JSON") from exc
    if not isinstance(document, dict) or document.get("version") != 1:
        raise PluginAdmissionError("Plugin lock version is unsupported")
    entries = document.get("plugins")
    if not isinstance(entries, list) or not entries or len(entries) > MAX_PLUGIN_LOCK_ENTRIES:
        raise PluginAdmissionError("Plugin lock must contain a bounded plugin list")
    result: dict[tuple[PluginKind, str], PluginLock] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise PluginAdmissionError("Plugin lock entry is invalid")
        kind = entry.get("kind")
        plugin_id = entry.get("plugin_id")
        lock = entry.get("lock")
        if kind not in {"catalog", "table_format"} or not isinstance(plugin_id, str):
            raise PluginAdmissionError("Plugin lock entry identity is invalid")
        if not isinstance(lock, list) or len(lock) != 5 or any(
            not isinstance(value, str) or not value for value in lock
        ):
            raise PluginAdmissionError("Plugin lock entry must contain a five-part lock")
        key = (cast(PluginKind, kind), plugin_id)
        if key in result:
            raise PluginAdmissionError("Plugin lock contains duplicate identities")
        result[key] = cast(PluginLock, tuple(lock))
    return result
