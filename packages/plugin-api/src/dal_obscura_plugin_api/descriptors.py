"""One static descriptor contract for host admission and plugin runtime metadata."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import metadata, resources
from typing import cast

from dal_obscura_plugin_api.contracts import PluginDescriptor, PluginKind

STATIC_DESCRIPTOR_FILENAME = "dal_obscura-plugin.json"
MAX_STATIC_DESCRIPTOR_BYTES = 65_536


def parse_descriptor(
    raw: bytes,
    *,
    kind: PluginKind,
    plugin_id: str,
    distribution: str,
    version: str,
) -> PluginDescriptor:
    """Decode inert metadata and bind it to an expected installed identity."""
    if len(raw) > MAX_STATIC_DESCRIPTOR_BYTES:
        raise ValueError("Plugin static descriptor is too large")
    expected_kind, expected_id = kind, plugin_id
    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_json_object)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("Plugin static descriptor is invalid JSON") from exc
    descriptor_payload = _select_descriptor(payload, expected_kind, expected_id)
    api_version = descriptor_payload.get("api_version")
    config_version = descriptor_payload.get("config_version")
    capabilities = descriptor_payload.get("capabilities", [])
    output_formats = descriptor_payload.get("output_formats", [])
    handle_versions = descriptor_payload.get("handle_versions", [1])
    config_schema = descriptor_payload.get("config_schema", {})
    display_name = descriptor_payload.get("display_name", "")
    if descriptor_payload.get("kind") != kind or descriptor_payload.get("plugin_id") != plugin_id:
        raise ValueError("Plugin static descriptor identity mismatch")
    if not isinstance(api_version, str) or (
        not isinstance(config_version, int) or isinstance(config_version, bool)
    ):
        raise ValueError("Plugin static descriptor version fields are invalid")
    if not isinstance(capabilities, list) or any(
        not isinstance(item, str) for item in capabilities
    ):
        raise ValueError("Plugin static descriptor capabilities are invalid")
    if not isinstance(output_formats, list) or any(
        not isinstance(item, str) for item in output_formats
    ):
        raise ValueError("Plugin static descriptor output formats are invalid")
    if not isinstance(handle_versions, list) or any(
        not isinstance(item, int) or isinstance(item, bool) for item in handle_versions
    ):
        raise ValueError("Plugin static descriptor handle versions are invalid")
    if not isinstance(config_schema, Mapping) or not isinstance(display_name, str):
        raise ValueError("Plugin static descriptor fields are invalid")
    capability_values = cast(list[str], capabilities)
    output_format_values = cast(list[str], output_formats)
    handle_version_values = cast(list[int], handle_versions)
    return PluginDescriptor(
        kind=kind,
        plugin_id=plugin_id,
        api_version=api_version,
        config_version=config_version,
        distribution=distribution,
        version=version,
        capabilities=frozenset(capability_values),
        output_formats=frozenset(output_format_values),
        handle_versions=frozenset(handle_version_values),
        config_schema=cast(Mapping[str, object], config_schema),
        display_name=display_name,
    )


def _select_descriptor(
    payload: object, expected_kind: PluginKind, expected_id: str
) -> Mapping[str, object]:
    if not isinstance(payload, dict):
        raise ValueError("Plugin static descriptor must be an object")
    document = cast(Mapping[str, object], payload)
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
    if set(document) - allowed:
        raise ValueError("Plugin static descriptor contains unknown fields")
    descriptor_payload: Mapping[str, object]
    raw_descriptors = document.get("descriptors")
    if raw_descriptors is not None:
        if not isinstance(raw_descriptors, list) or not raw_descriptors:
            raise ValueError("Plugin static descriptors are invalid")
        candidates: list[Mapping[str, object]] = []
        for item in raw_descriptors:
            if isinstance(item, Mapping):
                candidate = cast(Mapping[str, object], item)
                if (
                    candidate.get("kind") == expected_kind
                    and candidate.get("plugin_id") == expected_id
                ):
                    candidates.append(candidate)
        if len(candidates) != 1:
            raise ValueError("Plugin static descriptor identity is ambiguous")
        descriptor_payload = candidates[0]
        descriptor_allowed = allowed - {"descriptors"}
        if set(descriptor_payload) - descriptor_allowed:
            raise ValueError("Plugin static descriptor contains unknown fields")
    else:
        descriptor_payload = document
    return descriptor_payload


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate descriptor keys instead of silently choosing one."""

    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def load_packaged_descriptor(
    distribution_name: str, package: str | None, kind: PluginKind, plugin_id: str
) -> PluginDescriptor:
    """Load runtime metadata after the host admits and imports a plugin.

    The host reads wheel files without calling this function, since resolving
    a package resource may import its package. Runtime and admission share the
    same decoder, and installed distribution metadata owns the version.
    """
    if not package:
        raise ValueError("Plugin descriptor requires an installed package")
    distribution = metadata.distribution(distribution_name)
    try:
        with resources.files(package).joinpath(STATIC_DESCRIPTOR_FILENAME).open("rb") as stream:
            raw = stream.read(MAX_STATIC_DESCRIPTOR_BYTES + 1)
    except OSError as exc:
        raise ValueError("Plugin static descriptor is unreadable") from exc
    return parse_descriptor(
        raw,
        kind=kind,
        plugin_id=plugin_id,
        distribution=distribution.name,
        version=distribution.version,
    )
