"""Admission-controlled entry-point discovery for trusted plugins."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import replace
from functools import wraps
from importlib import metadata
from threading import RLock
from typing import Literal, cast, overload

from dal_obscura_plugin_api import (
    PLUGIN_API_VERSION,
    PLUGIN_CONFIG_VERSION,
    CatalogFactory,
    PluginDescriptor,
    PluginKind,
    TableFormatFactory,
)
from dal_obscura_plugin_api.options import PluginOptions
from dal_obscura_plugin_api.secrets import SecretBackendFactory

from dal_obscura.plugins.artifacts import (
    ENTRY_POINT_GROUPS,
    PluginAdmissionError,
    PluginLock,
    artifact_digest,
    descriptor_digest,
    load_static_plugin_descriptor,
)
from dal_obscura.plugins.lifecycle import (
    PluginLifecycleState,
    transition_plugin_lifecycle,
)

_PLUGIN_ID = re.compile(r"[a-z][a-z0-9_.-]{0,63}\Z")


FactoryLoader = Callable[[metadata.EntryPoint], object]
DescriptorLoader = Callable[[metadata.EntryPoint], PluginDescriptor]
PluginStatus = Literal["enabled", "not_installed", "incompatible"]


class PluginRegistry:
    """Discovers only operator-allowlisted installed entry points."""

    def __init__(
        self,
        *,
        allowlist: Mapping[tuple[PluginKind, str], PluginLock] | None = None,
        entry_points_fn: Callable[[], metadata.EntryPoints] | None = None,
        factory_loader: FactoryLoader | None = None,
        options: Mapping[str, object] | None = None,
        descriptor_loader: DescriptorLoader | None = None,
    ) -> None:
        self._allowlist = dict(allowlist or {})
        self._entry_points_fn = entry_points_fn or metadata.entry_points
        self._factory_loader = factory_loader or (lambda entry: entry.load())
        self._descriptor_loader = descriptor_loader
        self._snapshot: dict[tuple[PluginKind, str], PluginDescriptor] = {}
        self._snapshot_entries: dict[tuple[PluginKind, str], metadata.EntryPoint] = {}
        self.options = PluginOptions.from_conf(options or {}, {key[1] for key in self._allowlist})
        for kind, plugin_id in self._allowlist:
            if kind == "secret_backend" and "scope_grants" in self.options.for_plugin(plugin_id):
                raise ValueError("Secret scope grants belong to host startup configuration")
        self._snapshot_lock = RLock()
        self._lifecycle: dict[tuple[PluginKind, str], PluginLifecycleState] = {}

    def discover(self) -> dict[tuple[PluginKind, str], PluginDescriptor]:
        """Reads entry-point metadata without importing factories."""
        descriptors, _ = self._discover_with_entries()
        return descriptors

    @overload
    def load(self, kind: Literal["catalog"], plugin_id: str) -> CatalogFactory: ...

    @overload
    def load(self, kind: Literal["table_format"], plugin_id: str) -> TableFormatFactory: ...

    @overload
    def load(self, kind: Literal["secret_backend"], plugin_id: str) -> SecretBackendFactory: ...

    def load(
        self, kind: PluginKind, plugin_id: str
    ) -> CatalogFactory | TableFormatFactory | SecretBackendFactory:
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
            admitted = key in self._snapshot
        if entry is not None and admitted:
            factory = self._factory_loader(entry)
        else:
            raise PluginAdmissionError(f"Plugin is not admitted: {kind}:{plugin_id}")
        if not callable(factory):
            raise PluginAdmissionError(
                f"Admitted plugin factory is not callable: {kind}:{plugin_id}"
            )
        # Entry-point imports have no static type; admission establishes the SDK boundary.
        if kind == "table_format" or not self.options.values.get(plugin_id):
            return cast(CatalogFactory | TableFormatFactory | SecretBackendFactory, factory)

        @wraps(factory)
        def configured(value, context):
            if kind == "catalog":
                value = replace(value, options=self.options.for_plugin(plugin_id, value.options))
            elif kind == "secret_backend":
                value = self.options.for_plugin(plugin_id, value)
            return cast(Callable, factory)(value, context)

        return cast(CatalogFactory | TableFormatFactory | SecretBackendFactory, configured)

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
            if key not in self._snapshot and key not in self._allowlist:
                raise PluginAdmissionError(f"Plugin is not configured: {kind}:{plugin_id}")
            current = self._lifecycle.get(key, PluginLifecycleState.ENABLED)
            state = transition_plugin_lifecycle(current, target)
            self._lifecycle[key] = state
            return state

    def reload(self) -> dict[tuple[PluginKind, str], PluginDescriptor]:
        """Builds a complete admission snapshot before atomically swapping it."""

        candidate, entries = self._discover_with_entries()
        with self._snapshot_lock:
            self._snapshot = dict(candidate)
            self._snapshot_entries = dict(entries)
            return dict(self._snapshot)

    def admitted(self) -> dict[tuple[PluginKind, str], PluginDescriptor]:
        """Returns the last valid snapshot without rebuilding or importing factories."""

        with self._snapshot_lock:
            return dict(self._snapshot)

    def format_options(
        self, plugin_id: str, *, captured: Mapping[str, object] | None = None
    ) -> Mapping[str, object]:
        """Admit only declared non-secret options into passive signed work."""
        options = self.options.for_plugin(plugin_id) if captured is None else captured
        descriptor = self.admitted().get(("table_format", plugin_id))
        if descriptor is None:
            raise PluginAdmissionError(f"Plugin is not admitted: table_format:{plugin_id}")
        fields = {field.name: field for field in descriptor.config_fields or ()}
        if set(options) - set(fields):
            raise ValueError("Unsupported format options")
        if any(fields[key].secret for key in options):
            raise ValueError("Format options cannot contain secrets")
        for key, value in options.items():
            fields[key].validate(value)
        return PluginOptions({plugin_id: options}).for_plugin(plugin_id)

    def identity(self, kind: PluginKind, plugin_id: str) -> PluginLock:
        """Exact admitted artifact identity, captured in every passive scan ticket."""
        with self._snapshot_lock:
            descriptor = self._snapshot.get((kind, plugin_id))
            if descriptor is None:
                raise PluginAdmissionError(f"Plugin is not admitted: {kind}:{plugin_id}")
            return self._allowlist[(kind, plugin_id)]

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
        expected = set(self._allowlist)
        for kind, plugin_id in sorted(expected):
            key = (kind, plugin_id)
            if key in admitted:
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
            if (key in admitted) and lifecycle is not PluginLifecycleState.ENABLED:
                row["lifecycle"] = lifecycle.value
            if reason is not None:
                row["reason"] = reason
            rows.append(row)
        return tuple(rows)

    def _select(self, group: str) -> list[metadata.EntryPoint]:
        return list(self._entry_points_fn().select(group=group))

    def _discover_with_entries(  # noqa: C901
        self,
    ) -> tuple[
        dict[tuple[PluginKind, str], PluginDescriptor],
        dict[tuple[PluginKind, str], metadata.EntryPoint],
    ]:
        descriptors: dict[tuple[PluginKind, str], PluginDescriptor] = {}
        entries: dict[tuple[PluginKind, str], metadata.EntryPoint] = {}
        points = self._entry_points_fn()
        artifact_digests: dict[int, str] = {}
        for kind, group in ENTRY_POINT_GROUPS.items():
            for entry in points.select(group=group):
                plugin_id = str(entry.name)
                # Ignore malformed, unallowlisted metadata. An installed
                # third-party distribution must not be able to make reload
                # fail or cause a denial of service before admission is even
                # considered.
                if not _PLUGIN_ID.fullmatch(plugin_id):
                    continue
                key = (kind, plugin_id)
                if key in descriptors:
                    raise PluginAdmissionError(f"Duplicate plugin ID: {kind}:{plugin_id}")
                admitted = self._allowlist.get(key)
                if admitted is None:
                    continue
                if len(admitted) != 5:
                    raise PluginAdmissionError(f"Invalid plugin lock for {kind}:{plugin_id}")
                (
                    distribution,
                    version,
                    api_version,
                    expected_descriptor_digest,
                    expected_artifact_digest,
                ) = admitted
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
                    # Static metadata is read without executing the factory.
                    descriptor = load_static_plugin_descriptor(entry)
                if (
                    descriptor.kind != kind
                    or descriptor.plugin_id != plugin_id
                    or descriptor.api_version != api_version
                    or descriptor.distribution != distribution
                    or descriptor.version != version
                ):
                    raise PluginAdmissionError(f"Plugin descriptor mismatch for {kind}:{plugin_id}")
                if descriptor.api_version != PLUGIN_API_VERSION:
                    raise PluginAdmissionError(
                        f"Plugin descriptor uses unsupported API version for {kind}:{plugin_id}"
                    )
                if descriptor.config_version != PLUGIN_CONFIG_VERSION:
                    raise PluginAdmissionError(
                        f"Plugin descriptor uses unsupported config version for {kind}:{plugin_id}"
                    )
                if descriptor_digest(descriptor) != expected_descriptor_digest:
                    raise PluginAdmissionError(
                        f"Plugin descriptor digest mismatch for {kind}:{plugin_id}"
                    )
                # Entry points from one enumeration share their Distribution.
                # Keep fingerprints only for this pass so reload rechecks bytes.
                distribution_key = id(entry.dist)
                if distribution_key not in artifact_digests:
                    artifact_digests[distribution_key] = artifact_digest(entry)
                if artifact_digests[distribution_key] != expected_artifact_digest:
                    raise PluginAdmissionError(
                        f"Plugin artifact digest mismatch for {kind}:{plugin_id}"
                    )
                descriptors[key] = descriptor
                entries[key] = entry
        return descriptors, entries

    @staticmethod
    def _validate_id(plugin_id: str) -> None:
        if not _PLUGIN_ID.fullmatch(plugin_id):
            raise PluginAdmissionError(f"Invalid plugin ID: {plugin_id!r}")


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
    if _api_version != PLUGIN_API_VERSION:
        return "plugin API version is unsupported"
    return "plugin admission metadata is incompatible"
