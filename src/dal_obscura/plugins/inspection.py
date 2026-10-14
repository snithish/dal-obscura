"""Static installed-package inspection shared by operator release tooling."""

from __future__ import annotations

from importlib import metadata

from packaging.utils import canonicalize_name

from dal_obscura.plugins.registry import (
    ENTRY_POINT_GROUPS,
    PluginAdmissionError,
    PluginRegistry,
    build_plugin_lock,
    load_static_plugin_descriptor,
)


def inspect_distributions(distributions: list[str]) -> dict[str, object]:
    """Prepare exact admission metadata without importing plugin factories."""
    requested = {canonicalize_name(name) for name in distributions}
    found: set[str] = set()
    rows: list[dict[str, object]] = []
    descriptors: list[dict[str, object]] = []
    locks = {}
    for kind, group in ENTRY_POINT_GROUPS.items():
        for entry in metadata.entry_points().select(group=group):
            if entry.dist is None or canonicalize_name(entry.dist.name) not in requested:
                continue
            descriptor = load_static_plugin_descriptor(entry)
            lock = build_plugin_lock(kind, entry, descriptor)
            key = (kind, descriptor.plugin_id)
            if key in locks:
                raise PluginAdmissionError(f"Duplicate installed plugin ID: {kind}:{entry.name}")
            locks[key] = lock
            found.add(canonicalize_name(entry.dist.name))
            rows.append({"kind": kind, "plugin_id": descriptor.plugin_id, "lock": list(lock)})
            descriptors.append(descriptor.to_json())
    if missing := requested - found:
        raise PluginAdmissionError("No static plugins found for: " + ", ".join(sorted(missing)))
    # Enforce current SDK/API/descriptor/artifact admission, not merely a self-consistent lock.
    PluginRegistry(allowlist=locks).reload()
    rows.sort(key=lambda row: (str(row["kind"]), str(row["plugin_id"])))
    descriptors.sort(key=lambda row: (str(row["kind"]), str(row["plugin_id"])))
    return {"lock": {"version": 1, "plugins": rows}, "descriptors": descriptors}
