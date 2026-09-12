"""Authenticated plugin descriptor and capability routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from dal_obscura.common.plugin_api.contracts import PluginDescriptor
from dal_obscura.control_plane.interfaces.routes.deps import ControlPlaneDeps

_BUILTIN_CATALOG = PluginDescriptor(
    kind="catalog",
    plugin_id="iceberg.sql",
    api_version="1",
    config_version=1,
    distribution="dal-obscura",
    version="0.1.0",
    display_name="Iceberg SQL catalog",
    capabilities=frozenset({"nested_schema", "snapshot_reads", "splittable_scan"}),
    config_schema={
        "fields": [
            {"name": "uri", "type": "string", "required": True, "secret": False},
            {"name": "warehouse", "type": "string", "required": False, "secret": False},
            {"name": "username", "type": "string", "required": False, "secret": False},
            {
                "name": "password_secret",
                "type": "secret_reference",
                "required": False,
                "secret": True,
            },
        ]
    },
)
_BUILTIN_FORMAT = PluginDescriptor(
    kind="table_format",
    plugin_id="iceberg",
    api_version="1",
    config_version=1,
    distribution="dal-obscura",
    version="0.1.0",
    display_name="Apache Iceberg",
    capabilities=frozenset({"nested_schema", "snapshot_reads", "splittable_scan"}),
)


def router(deps: ControlPlaneDeps) -> APIRouter:
    """Builds the authenticated descriptor route."""

    api = APIRouter()

    @api.get("/v1/plugins", dependencies=[Depends(deps.require_admin)])
    def list_plugins() -> object:
        descriptors = (
            list(deps.plugin_registry.admitted().values())
            if deps.plugin_registry is not None
            else [_BUILTIN_CATALOG, _BUILTIN_FORMAT]
        )
        return {
            "plugins": [_descriptor_payload(descriptor) for descriptor in descriptors],
            "pairs": _pair_payload(descriptors),
        }

    return api


def _descriptor_payload(descriptor: PluginDescriptor) -> dict[str, object]:
    return {
        "kind": descriptor.kind,
        "plugin_id": descriptor.plugin_id,
        "api_version": descriptor.api_version,
        "config_version": descriptor.config_version,
        "distribution": descriptor.distribution,
        "version": descriptor.version,
        "display_name": descriptor.display_name or descriptor.plugin_id,
        "capabilities": sorted(descriptor.capabilities),
        "config_schema": dict(descriptor.config_schema),
        "status": "admitted",
    }


def _pair_payload(descriptors: list[PluginDescriptor]) -> list[dict[str, object]]:
    catalogs = [item for item in descriptors if item.kind == "catalog"]
    formats = [item for item in descriptors if item.kind == "table_format"]
    pairs: list[dict[str, object]] = []
    for catalog in catalogs:
        for table_format in formats:
            pairs.append(
                {
                    "catalog_plugin_id": catalog.plugin_id,
                    "format_plugin_id": table_format.plugin_id,
                    "capabilities": sorted(
                        catalog.capabilities & table_format.capabilities
                    ),
                    "status": "admitted",
                }
            )
    return pairs
