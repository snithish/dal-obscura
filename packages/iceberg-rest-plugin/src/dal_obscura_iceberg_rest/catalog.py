from __future__ import annotations

from datetime import datetime
from urllib.parse import urlsplit

from dal_obscura_plugin_api import (
    CatalogConfig,
    CatalogPlugin,
    DiscoveryPage,
    ExecutionContext,
    PluginDescriptor,
    TableHandle,
    TableIdentifier,
)

MAX_TABLES = 10_000
MAX_NAMESPACES = 10_000
MAX_PAGE = 500
_ALLOWED_OPTIONS = frozenset(
    {"uri", "warehouse", "token", "credential", "scope", "oauth2-server-uri"}
)

DESCRIPTOR = PluginDescriptor(
    kind="catalog",
    plugin_id="iceberg.rest",
    api_version="1",
    config_version=1,
    distribution="dal-obscura-iceberg-rest",
    version="0.1.0",
    display_name="Iceberg REST catalog",
    capabilities=frozenset({"nested_schema", "snapshot_reads", "splittable_scan"}),
    config_schema={
        "fields": [
            {"name": "uri", "type": "uri", "required": True, "secret": False},
            {"name": "warehouse", "type": "string", "required": False, "secret": False},
            {"name": "token", "type": "secret_reference", "required": False, "secret": True},
            {"name": "credential", "type": "secret_reference", "required": False, "secret": True},
        ]
    },
)


class RestCatalog(CatalogPlugin):
    descriptor = DESCRIPTOR

    def __init__(self, config: CatalogConfig, context: ExecutionContext) -> None:
        self._config = config
        self._validate_context(context)
        options = dict(config.options)
        unknown = set(options) - _ALLOWED_OPTIONS
        if unknown:
            raise ValueError("REST catalog options contain unsupported keys")
        uri = options.get("uri")
        if not isinstance(uri, str) or not uri:
            raise ValueError("REST catalog requires a URI")
        parsed = urlsplit(uri)
        if parsed.scheme not in {"https", "http"} or not parsed.netloc:
            raise ValueError("REST catalog URI must be an absolute HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("REST catalog URI cannot contain credentials or query data")
        self._options = options
        self._catalog = None

    def list_tables(
        self,
        context: ExecutionContext,
        *,
        continuation: str | None = None,
        limit: int,
    ) -> DiscoveryPage:
        self._validate_context(context)
        if not 1 <= limit <= MAX_PAGE:
            raise ValueError("REST catalog page size is out of bounds")
        offset = _continuation_offset(continuation)
        catalog = self._load_catalog(context)
        identifiers = []
        for index, namespace in enumerate(catalog.list_namespaces()):
            if index >= MAX_NAMESPACES:
                raise ValueError("REST catalog contains too many namespaces")
            self._validate_context(context)
            for identifier in catalog.list_tables(namespace):
                self._validate_context(context)
                identifiers.append(_identifier(identifier))
                if len(identifiers) > MAX_TABLES:
                    raise ValueError("REST catalog contains too many tables")
        identifiers.sort(key=lambda item: (*item.namespace, item.name))
        if offset > len(identifiers):
            raise ValueError("REST catalog continuation token is out of range")
        end = min(offset + limit, len(identifiers))
        return DiscoveryPage(
            entries=tuple(identifiers[offset:end]),
            continuation=str(end) if end < len(identifiers) else None,
        )

    def resolve_table(self, identifier: TableIdentifier, context: ExecutionContext) -> TableHandle:
        self._validate_context(context)
        table = self._load_catalog(context).load_table((*identifier.namespace, identifier.name))
        metadata_location = getattr(table, "metadata_location", None)
        if not isinstance(metadata_location, str) or not metadata_location:
            raise ValueError("REST catalog table has no metadata location")
        return TableHandle(
            catalog_plugin_id=DESCRIPTOR.plugin_id,
            catalog_instance_id=self._config.instance_id,
            catalog_revision=self._config.revision,
            identifier=identifier,
            format_plugin_id="iceberg",
            handle_version=1,
            snapshot_id=_snapshot_id(table),
            metadata={
                "metadata_location": metadata_location,
            },
        )

    def _load_catalog(self, context: ExecutionContext):
        self._validate_context(context)
        if self._catalog is None:
            from pyiceberg.catalog import load_catalog

            properties = {key: str(value) for key, value in self._options.items() if key != "uri"}
            self._catalog = load_catalog(
                self._config.instance_id,
                type="rest",
                uri=str(self._options["uri"]),
                **properties,
            )
        return self._catalog

    @staticmethod
    def _validate_context(context: ExecutionContext) -> None:
        if context.deadline <= datetime.now(context.deadline.tzinfo):
            raise ValueError("REST catalog execution deadline has expired")
        if context.cancel_check is not None and context.cancel_check():
            raise ValueError("REST catalog operation was cancelled")


def _identifier(value: object) -> TableIdentifier:
    if isinstance(value, (tuple, list)):
        parts = tuple(str(part) for part in value)
    else:
        raise ValueError("REST catalog returned an invalid table identifier")
    if len(parts) < 2:
        raise ValueError("REST catalog returned an invalid table identifier")
    return TableIdentifier(namespace=parts[:-1], name=parts[-1])


def _continuation_offset(value: str | None) -> int:
    if value is None:
        return 0
    if not value.isdigit() or len(value) > 6:
        raise ValueError("REST catalog continuation token is invalid")
    return int(value)


def _snapshot_id(table: object) -> str | None:
    snapshot = getattr(table, "current_snapshot", None)
    value = getattr(snapshot, "snapshot_id", snapshot)
    return str(value) if value is not None else None


def rest_catalog_factory(config: CatalogConfig, context: ExecutionContext) -> RestCatalog:
    return RestCatalog(config, context)
