"""Bounded loading of operator-mounted immutable plugin locks."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import cast

from dal_obscura_plugin_api import PluginKind

from dal_obscura.kernel.plugins import PluginAdmissionError, PluginLock
from dal_obscura.plugins.artifacts import read_metadata_file

MAX_PLUGIN_LOCK_BYTES = 1_048_576
MAX_PLUGIN_LOCK_ENTRIES = 256
_PLUGIN_ID = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")


def load_plugin_lock_file(path: str | Path) -> dict[tuple[PluginKind, str], PluginLock]:
    """Read a bounded lock file without importing any plugin factories."""

    lock_path = Path(path)
    if not str(lock_path).strip():
        raise PluginAdmissionError("Plugin lock path must be non-empty")
    raw = read_metadata_file(lock_path, MAX_PLUGIN_LOCK_BYTES, "Plugin lock", immutable=True)
    try:
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise PluginAdmissionError("Plugin lock is not valid JSON") from exc
    if (
        not isinstance(document, dict)
        or type(document.get("version")) is not int
        or document["version"] != 1
    ):
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
        if (
            kind not in {"catalog", "table_format", "secret_backend"}
            or not isinstance(plugin_id, str)
            or not _PLUGIN_ID.fullmatch(plugin_id)
        ):
            raise PluginAdmissionError("Plugin lock entry identity is invalid")
        parsed_lock = PluginLock.from_json(lock)
        key = (cast(PluginKind, kind), plugin_id)
        if key in result:
            raise PluginAdmissionError("Plugin lock contains duplicate identities")
        result[key] = parsed_lock
    return result


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate keys instead of allowing last-key-wins ambiguity."""

    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result
