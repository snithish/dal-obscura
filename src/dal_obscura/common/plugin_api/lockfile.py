"""Bounded loading of operator-mounted immutable plugin locks."""

from __future__ import annotations

import json
import re
import stat
from pathlib import Path
from typing import cast

from dal_obscura_plugin_api import PluginKind

from dal_obscura.common.plugin_api.registry import PluginAdmissionError, PluginLock

MAX_PLUGIN_LOCK_BYTES = 1_048_576
MAX_PLUGIN_LOCK_ENTRIES = 256
_PLUGIN_ID = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")
_HEX_DIGEST = re.compile(r"[0-9a-f]{64}\Z")


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
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
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
        if (
            kind not in {"catalog", "table_format"}
            or not isinstance(plugin_id, str)
            or not _PLUGIN_ID.fullmatch(plugin_id)
        ):
            raise PluginAdmissionError("Plugin lock entry identity is invalid")
        if (
            not isinstance(lock, list)
            or len(lock) != 5
            or any(not isinstance(value, str) or not value for value in lock)
        ):
            raise PluginAdmissionError("Plugin lock entry must contain a five-part lock")
        distribution, version, api_version, descriptor_digest, artifact_digest = lock
        if any(
            any(ord(char) < 0x20 or ord(char) == 0x7F for char in value) or len(value) > 256
            for value in (distribution, version, api_version)
        ):
            raise PluginAdmissionError("Plugin lock identity values must be printable and bounded")
        if not _HEX_DIGEST.fullmatch(descriptor_digest) or not _HEX_DIGEST.fullmatch(
            artifact_digest
        ):
            raise PluginAdmissionError("Plugin lock digests must be lowercase SHA-256 values")
        key = (cast(PluginKind, kind), plugin_id)
        if key in result:
            raise PluginAdmissionError("Plugin lock contains duplicate identities")
        result[key] = cast(PluginLock, tuple(lock))
    return result


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate keys instead of allowing last-key-wins ambiguity."""

    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result
