"""Admission-controlled entry-point discovery for trusted plugins."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from importlib import metadata
from pathlib import Path
from threading import RLock
from typing import Any, Literal, cast

from dal_obscura_plugin_api import (
    SUPPORTED_PLUGIN_API_VERSIONS,
    SUPPORTED_PLUGIN_CONFIG_VERSIONS,
    PluginDescriptor,
    PluginKind,
)

from dal_obscura.common.plugin_api.lifecycle import (
    PluginLifecycleState,
    transition_plugin_lifecycle,
)

ENTRY_POINT_GROUPS: dict[PluginKind, str] = {
    "catalog": "dal_obscura.catalogs.v1",
    "table_format": "dal_obscura.table_formats.v1",
}
STATIC_DESCRIPTOR_FILENAME = "dal_obscura-plugin.json"
MAX_STATIC_DESCRIPTOR_BYTES = 65_536
_PLUGIN_ID = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")
_MODULE_PATH = re.compile(r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*\Z")


class PluginAdmissionError(ValueError):
    """Raised when installed plugin metadata is not explicitly admitted."""


FactoryLoader = Callable[[metadata.EntryPoint], object]
BuiltinRegistration = tuple[PluginDescriptor, object]
DescriptorLoader = Callable[[metadata.EntryPoint], PluginDescriptor]
PluginLock = tuple[str, str, str, str, str]
PluginStatus = Literal["enabled", "not_installed", "incompatible"]


class PluginRegistry:
    """Discovers only operator-allowlisted installed entry points."""

    def __init__(
        self,
        *,
        allowlist: Mapping[tuple[PluginKind, str], PluginLock] | None = None,
        entry_points_fn: Callable[[], metadata.EntryPoints] | None = None,
        factory_loader: FactoryLoader | None = None,
        builtins: Mapping[tuple[PluginKind, str], BuiltinRegistration] | None = None,
        descriptor_loader: DescriptorLoader | None = None,
    ) -> None:
        self._allowlist = dict(allowlist or {})
        self._entry_points_fn = entry_points_fn or metadata.entry_points
        self._factory_loader = factory_loader or (lambda entry: entry.load())
        self._descriptor_loader = descriptor_loader
        self._builtins = dict(builtins or {})
        for key, (descriptor, _) in self._builtins.items():
            if key != (descriptor.kind, descriptor.plugin_id):
                raise ValueError("Built-in plugin registration key does not match descriptor")
        self._snapshot: dict[tuple[PluginKind, str], PluginDescriptor] = {}
        self._snapshot_entries: dict[tuple[PluginKind, str], metadata.EntryPoint] = {}
        self._snapshot_builtins: dict[tuple[PluginKind, str], object] = {}
        self._snapshot_lock = RLock()
        self._lifecycle: dict[tuple[PluginKind, str], PluginLifecycleState] = {}

    def discover(self) -> dict[tuple[PluginKind, str], PluginDescriptor]:
        """Reads entry-point metadata without importing factories."""
        descriptors, _, _ = self._discover_with_entries()
        return descriptors

    def load(self, kind: PluginKind, plugin_id: str) -> object:
        """Loads one admitted factory; never accepts a request import string."""

        self._validate_id(plugin_id)
        key = (kind, plugin_id)
        with self._snapshot_lock:
            lifecycle = self._lifecycle.get(key, PluginLifecycleState.ENABLED)
            if lifecycle is not PluginLifecycleState.ENABLED:
                raise PluginAdmissionError(
                    f"Plugin is {lifecycle.value}; new admissions are disabled: {kind}:{plugin_id}"
                )
            entry = self._snapshot_entries.get(key)
            builtin = self._snapshot_builtins.get(key)
            admitted = key in self._snapshot
        if entry is None and not admitted:
            # Preserve the convenient first-use behavior while still making
            # the resulting entry point part of one immutable generation.
            self.reload()
            with self._snapshot_lock:
                entry = self._snapshot_entries.get(key)
                builtin = self._snapshot_builtins.get(key)
                admitted = key in self._snapshot
        if entry is None or not admitted:
            if builtin is not None:
                return builtin
            raise PluginAdmissionError(f"Plugin is not admitted: {kind}:{plugin_id}")
        if builtin is not None:
            return builtin
        return self._factory_loader(entry)

    def set_lifecycle(
        self,
        kind: PluginKind,
        plugin_id: str,
        target: PluginLifecycleState,
    ) -> PluginLifecycleState:
        """Apply an explicit admission transition for an admitted plugin."""

        self._validate_id(plugin_id)
        key = (kind, plugin_id)
        with self._snapshot_lock:
            if (
                key not in self._snapshot
                and key not in self._builtins
                and key not in self._allowlist
            ):
                raise PluginAdmissionError(f"Plugin is not configured: {kind}:{plugin_id}")
            current = self._lifecycle.get(key, PluginLifecycleState.ENABLED)
            state = transition_plugin_lifecycle(current, target)
            self._lifecycle[key] = state
            return state

    def lifecycle_state(self, kind: PluginKind, plugin_id: str) -> PluginLifecycleState:
        """Return the current admission state without rebuilding the registry."""

        self._validate_id(plugin_id)
        with self._snapshot_lock:
            return self._lifecycle.get((kind, plugin_id), PluginLifecycleState.ENABLED)

    def reload(self) -> dict[tuple[PluginKind, str], PluginDescriptor]:
        """Builds a complete admission snapshot before atomically swapping it."""

        candidate, entries, builtins = self._discover_with_entries()
        with self._snapshot_lock:
            self._snapshot = dict(candidate)
            self._snapshot_entries = dict(entries)
            self._snapshot_builtins = dict(builtins)
            return dict(self._snapshot)

    def admitted(self) -> dict[tuple[PluginKind, str], PluginDescriptor]:
        """Returns the last valid snapshot without rebuilding or importing factories."""

        with self._snapshot_lock:
            return dict(self._snapshot)

    def status_report(self) -> tuple[dict[str, object], ...]:
        """Reports allowlisted plugin lifecycle state without importing factories.

        Only operator-expected IDs are returned.  An unallowlisted installed
        distribution therefore cannot become an enumeration or an implicit
        capability advertisement through this diagnostic surface.
        """

        with self._snapshot_lock:
            admitted = set(self._snapshot)
        entry_by_key: dict[tuple[PluginKind, str], metadata.EntryPoint] = {}
        for kind, group in ENTRY_POINT_GROUPS.items():
            for entry in self._select(group):
                plugin_id = str(entry.name)
                if _PLUGIN_ID.fullmatch(plugin_id):
                    entry_by_key[(kind, plugin_id)] = entry

        rows: list[dict[str, object]] = []
        expected = set(self._allowlist) | set(self._builtins)
        for kind, plugin_id in sorted(expected):
            key = (kind, plugin_id)
            if key in admitted or key in self._builtins:
                status: PluginStatus = "enabled"
                reason = None
                lifecycle = self._lifecycle.get(key, PluginLifecycleState.ENABLED)
            else:
                entry = entry_by_key.get(key)
                if entry is None:
                    status = "not_installed"
                    reason = "allowlisted distribution is not installed"
                else:
                    status = "incompatible"
                    reason = _status_incompatibility(entry, self._allowlist.get(key))
            row: dict[str, object] = {
                "kind": kind,
                "plugin_id": plugin_id,
                "status": status,
            }
            if (
                key in admitted or key in self._builtins
            ) and lifecycle is not PluginLifecycleState.ENABLED:
                row["lifecycle"] = lifecycle.value
            if reason is not None:
                row["reason"] = reason
            rows.append(row)
        return tuple(rows)

    def _select(self, group: str) -> list[metadata.EntryPoint]:
        points: Any = self._entry_points_fn()
        selected = (
            points.select(group=group) if hasattr(points, "select") else points.get(group, ())
        )
        return list(cast(Any, selected))

    def _discover_with_entries(  # noqa: C901
        self,
    ) -> tuple[
        dict[tuple[PluginKind, str], PluginDescriptor],
        dict[tuple[PluginKind, str], metadata.EntryPoint],
        dict[tuple[PluginKind, str], object],
    ]:
        descriptors = {key: descriptor for key, (descriptor, _) in self._builtins.items()}
        entries: dict[tuple[PluginKind, str], metadata.EntryPoint] = {}
        builtins = {key: factory for key, (_, factory) in self._builtins.items()}
        for kind, group in ENTRY_POINT_GROUPS.items():
            for entry in self._select(group):
                plugin_id = str(entry.name)
                # Ignore malformed, unallowlisted metadata. An installed
                # third-party distribution must not be able to make reload
                # fail or cause a denial of service before admission is even
                # considered.
                if not _PLUGIN_ID.fullmatch(plugin_id):
                    continue
                key = (kind, plugin_id)
                if key in descriptors:
                    if key in self._builtins:
                        # Built-ins are explicit registrations owned by the
                        # server.  An installed wheel may expose the same
                        # entry point for discovery, but it must not shadow
                        # or create an ambiguity with that trusted factory.
                        continue
                    raise PluginAdmissionError(f"Duplicate plugin ID: {kind}:{plugin_id}")
                admitted = self._allowlist.get(key)
                if admitted is None:
                    continue
                if len(admitted) != 5:
                    raise PluginAdmissionError(f"Invalid plugin lock for {kind}:{plugin_id}")
                distribution, version, api_version, descriptor_digest, artifact_digest = admitted
                if entry.dist is None:
                    raise PluginAdmissionError(
                        f"Plugin provenance is unavailable for {kind}:{plugin_id}"
                    )
                actual_distribution = entry.dist.name
                actual_version = entry.dist.version
                if (actual_distribution, actual_version) != (distribution, version):
                    raise PluginAdmissionError(f"Plugin lock mismatch for {kind}:{plugin_id}")
                if self._descriptor_loader is not None:
                    descriptor = self._descriptor_loader(entry)
                else:
                    # The static descriptor is the only factory-free source of
                    # plugin metadata.  The former three-part fallback was
                    # removed because it fabricated capabilities and config.
                    descriptor = load_static_plugin_descriptor(entry)
                if (
                    descriptor.kind != kind
                    or descriptor.plugin_id != plugin_id
                    or descriptor.api_version != api_version
                    or descriptor.distribution != distribution
                    or descriptor.version != version
                ):
                    raise PluginAdmissionError(f"Plugin descriptor mismatch for {kind}:{plugin_id}")
                if descriptor.api_version not in SUPPORTED_PLUGIN_API_VERSIONS:
                    raise PluginAdmissionError(
                        f"Plugin descriptor uses unsupported API version for {kind}:{plugin_id}"
                    )
                if descriptor.config_version not in SUPPORTED_PLUGIN_CONFIG_VERSIONS:
                    raise PluginAdmissionError(
                        f"Plugin descriptor uses unsupported config version for {kind}:{plugin_id}"
                    )
                if _descriptor_digest(descriptor) != descriptor_digest:
                    raise PluginAdmissionError(
                        f"Plugin descriptor digest mismatch for {kind}:{plugin_id}"
                    )
                if _artifact_digest(entry) != artifact_digest:
                    raise PluginAdmissionError(
                        f"Plugin artifact digest mismatch for {kind}:{plugin_id}"
                    )
                descriptors[key] = descriptor
                entries[key] = entry
        return descriptors, entries, builtins

    @staticmethod
    def _validate_id(plugin_id: str) -> None:
        if not _PLUGIN_ID.fullmatch(plugin_id):
            raise PluginAdmissionError(f"Invalid plugin ID: {plugin_id!r}")


def load_static_plugin_descriptor(entry: metadata.EntryPoint) -> PluginDescriptor:  # noqa: C901
    """Loads a plugin descriptor from distribution metadata without importing code.

    Qualified wheels may include ``dal_obscura-plugin.json`` at their root.  The
    descriptor is parsed from the installed distribution's metadata and its
    identity is tied to the entry-point group/name and package provenance.  A
    missing or malformed file fails closed; callers that need legacy entry-point
    compatibility is intentionally unsupported; every admitted wheel must
    provide a static descriptor.
    """

    distribution = entry.dist
    if distribution is None:
        raise PluginAdmissionError("Plugin provenance is unavailable")
    try:
        raw = distribution.read_text(STATIC_DESCRIPTOR_FILENAME)
        if raw is None:
            # setuptools package-data is conventionally stored beneath the
            # entry-point's top-level package rather than at distribution root.
            # Read only that deterministic package-local path; never inspect or
            # import arbitrary paths supplied by plugin metadata.
            module_path = str(entry.value).split(":", 1)[0]
            if not _MODULE_PATH.fullmatch(module_path):
                raise PluginAdmissionError("Plugin entry-point module is invalid")
            package = module_path.split(".", 1)[0]
            package_path = f"{package}/{STATIC_DESCRIPTOR_FILENAME}"
            raw = distribution.read_text(package_path)
            if raw is None:
                # ``PathDistribution.read_text`` may return ``None`` for
                # package-data paths even when the file is present in the
                # installed wheel.  Resolve only the exact path advertised
                # by ``files``; never walk or import arbitrary distribution
                # content.
                files = distribution.files
                locate_file = getattr(distribution, "locate_file", None)
                if (
                    files is not None
                    and callable(locate_file)
                    and any(str(path) == package_path for path in files)
                ):
                    raw = locate_file(package_path).read_text(encoding="utf-8")
    except (AttributeError, OSError, UnicodeError) as exc:
        raise PluginAdmissionError("Plugin static descriptor is unreadable") from exc
    if raw is None:
        raise PluginAdmissionError("Plugin static descriptor is missing")
    if len(raw.encode("utf-8")) > MAX_STATIC_DESCRIPTOR_BYTES:
        raise PluginAdmissionError("Plugin static descriptor is too large")
    try:
        payload = json.loads(raw, object_pairs_hook=_unique_json_object)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise PluginAdmissionError("Plugin static descriptor is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise PluginAdmissionError("Plugin static descriptor must be an object")
    expected_kind = next(
        (kind for kind, group in ENTRY_POINT_GROUPS.items() if group == entry.group),
        None,
    )
    if expected_kind is None:
        raise PluginAdmissionError("Plugin entry-point group is unsupported")
    allowed = {
        "kind",
        "plugin_id",
        "api_version",
        "config_version",
        "capabilities",
        "output_formats",
        "handle_versions",
        "config_schema",
        "display_name",
        "descriptors",
    }
    if set(payload) - allowed:
        raise PluginAdmissionError("Plugin static descriptor contains unknown fields")
    descriptor_payload: Mapping[str, object]
    raw_descriptors = payload.get("descriptors")
    if raw_descriptors is not None:
        if not isinstance(raw_descriptors, list) or not raw_descriptors:
            raise PluginAdmissionError("Plugin static descriptors are invalid")
        candidates = [
            item
            for item in raw_descriptors
            if isinstance(item, Mapping)
            and item.get("kind") == expected_kind
            and item.get("plugin_id") == str(entry.name)
        ]
        if len(candidates) != 1:
            raise PluginAdmissionError("Plugin static descriptor identity is ambiguous")
        descriptor_payload = candidates[0]
        descriptor_allowed = allowed - {"descriptors"}
        if set(descriptor_payload) - descriptor_allowed:
            raise PluginAdmissionError("Plugin static descriptor contains unknown fields")
    else:
        descriptor_payload = payload
    kind = descriptor_payload.get("kind")
    plugin_id = descriptor_payload.get("plugin_id")
    api_version = descriptor_payload.get("api_version")
    config_version = descriptor_payload.get("config_version")
    capabilities = descriptor_payload.get("capabilities", [])
    output_formats = descriptor_payload.get("output_formats", [])
    handle_versions = descriptor_payload.get("handle_versions", [1])
    config_schema = descriptor_payload.get("config_schema", {})
    display_name = descriptor_payload.get("display_name", "")
    if kind != expected_kind or plugin_id != str(entry.name):
        raise PluginAdmissionError("Plugin static descriptor identity mismatch")
    if not isinstance(api_version, str) or not isinstance(config_version, int):
        raise PluginAdmissionError("Plugin static descriptor version fields are invalid")
    if not isinstance(capabilities, list) or any(
        not isinstance(item, str) for item in capabilities
    ):
        raise PluginAdmissionError("Plugin static descriptor capabilities are invalid")
    if not isinstance(output_formats, list) or any(
        not isinstance(item, str) for item in output_formats
    ):
        raise PluginAdmissionError("Plugin static descriptor output formats are invalid")
    if not isinstance(handle_versions, list) or any(
        not isinstance(item, int) or isinstance(item, bool) for item in handle_versions
    ):
        raise PluginAdmissionError("Plugin static descriptor handle versions are invalid")
    if not isinstance(config_schema, Mapping) or not isinstance(display_name, str):
        raise PluginAdmissionError("Plugin static descriptor fields are invalid")
    capability_values = cast(list[str], capabilities)
    output_format_values = cast(list[str], output_formats)
    handle_version_values = cast(list[int], handle_versions)
    return PluginDescriptor(
        kind=cast(PluginKind, kind),
        plugin_id=plugin_id,
        api_version=api_version,
        config_version=config_version,
        distribution=distribution.name,
        version=distribution.version,
        capabilities=frozenset(capability_values),
        output_formats=frozenset(output_format_values),
        handle_versions=frozenset(handle_version_values),
        config_schema=config_schema,
        display_name=display_name,
    )


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate descriptor keys instead of silently choosing one."""

    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _status_incompatibility(
    entry: metadata.EntryPoint,
    lock: PluginLock | None,
) -> str:
    if lock is None:
        return "plugin is installed but not allowlisted"
    if len(lock) != 5:
        return "plugin lock is invalid"
    if entry.dist is None:
        return "plugin provenance is unavailable"
    distribution, version, _api_version, _descriptor_digest_value, _artifact_digest_value = lock
    if (entry.dist.name, entry.dist.version) != (distribution, version):
        return "installed distribution does not match the plugin lock"
    if _api_version not in SUPPORTED_PLUGIN_API_VERSIONS:
        return "plugin API version is unsupported"
    return "plugin admission metadata is incompatible"


def _descriptor_digest(descriptor: PluginDescriptor) -> str:
    payload = {
        "kind": descriptor.kind,
        "plugin_id": descriptor.plugin_id,
        "api_version": descriptor.api_version,
        "config_version": descriptor.config_version,
        "distribution": descriptor.distribution,
        "version": descriptor.version,
        "capabilities": sorted(descriptor.capabilities),
        "output_formats": sorted(descriptor.output_formats),
        "handle_versions": sorted(descriptor.handle_versions),
        "config_schema": descriptor.config_schema,
        "display_name": descriptor.display_name,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def build_plugin_lock(
    kind: PluginKind,
    entry: metadata.EntryPoint,
    descriptor: PluginDescriptor,
) -> PluginLock:
    """Build a descriptor/artifact-pinned lock for a qualified entry point.

    Release tooling should call this against the built distribution, then
    persist the returned five-part tuple in its immutable admission manifest.
    It refuses to generate a lock when provenance or descriptor identity does
    not match the entry point.
    """

    if descriptor.kind != kind or descriptor.plugin_id != str(entry.name):
        raise PluginAdmissionError("Plugin descriptor does not match entry point")
    if entry.dist is None:
        raise PluginAdmissionError("Plugin provenance is unavailable")
    distribution = entry.dist.name
    version = entry.dist.version
    if descriptor.distribution != distribution or descriptor.version != version:
        raise PluginAdmissionError("Plugin descriptor provenance does not match entry point")
    return (
        distribution,
        version,
        descriptor.api_version,
        _descriptor_digest(descriptor),
        _artifact_digest(entry),
    )


def _artifact_digest(entry: metadata.EntryPoint) -> str:
    distribution = entry.dist
    if distribution is None or distribution.files is None:
        raise PluginAdmissionError("Plugin artifact files are unavailable")
    digest = hashlib.sha256()
    try:
        for relative in sorted(distribution.files, key=str):
            path = Path(str(distribution.locate_file(str(relative))))
            if not path.is_file():
                raise PluginAdmissionError("Plugin artifact file is unavailable")
            digest.update(str(relative).encode("utf-8"))
            digest.update(b"\0")
            digest.update(path.read_bytes())
            digest.update(b"\0")
    except OSError as exc:
        raise PluginAdmissionError("Plugin artifact files are unreadable") from exc
    return digest.hexdigest()
