from __future__ import annotations

from datetime import datetime
from threading import Lock
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
            {"name": "scope", "type": "string", "required": False, "secret": False},
            {"name": "oauth2-server-uri", "type": "uri", "required": False, "secret": False},
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
        if any(not isinstance(value, str) for value in options.values()):
            raise ValueError("REST catalog options must be strings or resolved secrets")
        uri = options.get("uri")
        if not isinstance(uri, str) or not uri:
            raise ValueError("REST catalog requires a URI")
        parsed = urlsplit(uri)
        if parsed.scheme not in {"https", "http"} or not parsed.netloc:
            raise ValueError("REST catalog URI must be an absolute HTTP(S) URL")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("REST catalog URI cannot contain credentials or query data")
        if parsed.scheme == "http" and any(
            key in options for key in ("token", "credential")
        ):
            raise ValueError("REST catalog credentials require an HTTPS URI")
        warehouse = options.get("warehouse")
        if warehouse is not None:
            _validate_optional_uri(warehouse, "warehouse")
        oauth_uri = options.get("oauth2-server-uri")
        if oauth_uri is not None:
            _validate_optional_uri(oauth_uri, "oauth2-server-uri", http_only=True)
        self._options = options
        self._catalog = None
        self._catalog_lock = Lock()
        self._closed = False

    def validate_config(self, context: ExecutionContext) -> None:
        """Validate the already-admitted configuration without provider I/O."""

        self._validate_context(context)
        if self._closed:
            raise ValueError("REST catalog is closed")

    def list_namespaces(
        self,
        context: ExecutionContext,
        *,
        namespace: tuple[str, ...] = (),
    ) -> tuple[tuple[str, ...], ...]:
        self._validate_context(context)
        if namespace:
            raise ValueError("REST namespace traversal accepts only the root namespace")
        catalog = self._load_catalog(context)
        result: set[tuple[str, ...]] = set()
        for raw in catalog.list_namespaces():
            self._validate_context(context)
            if not isinstance(raw, (tuple, list)) or not raw or any(
                not isinstance(part, str) or not part for part in raw
            ):
                raise ValueError("REST catalog returned an invalid namespace")
            result.add(tuple(raw))
            if len(result) > MAX_NAMESPACES:
                raise ValueError("REST catalog contains too many namespaces")
        return tuple(sorted(result))

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

    def close(self) -> None:
        """Release the provider session and make this catalog unusable."""

        with self._catalog_lock:
            if self._closed:
                return
            self._closed = True
            catalog = self._catalog
            self._catalog = None
            if catalog is None:
                return
            close = getattr(catalog, "close", None)
            if callable(close):
                close()
                return
            session = getattr(catalog, "_session", None)
            close = getattr(session, "close", None)
            if callable(close):
                close()

    def _load_catalog(self, context: ExecutionContext):
        self._validate_context(context)
        if self._closed:
            raise ValueError("REST catalog is closed")
        if self._catalog is None:
            with self._catalog_lock:
                if self._closed:
                    raise ValueError("REST catalog is closed")
                if self._catalog is None:
                    from pyiceberg.catalog import load_catalog

                    properties = {
                        key: str(value) for key, value in self._options.items() if key != "uri"
                    }
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
    if not isinstance(value, (tuple, list)):
        raise ValueError("REST catalog returned an invalid table identifier")
    parts_list: list[str] = []
    for part in value:
        if not isinstance(part, str):
            raise ValueError("REST catalog returned an invalid table identifier")
        parts_list.append(part)
    parts = tuple(parts_list)
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


def _validate_optional_uri(value: object, label: str, *, http_only: bool = False) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"REST catalog {label} must be a URI")
    parsed = urlsplit(value)
    allowed = {"http", "https", "s3", "gs", "abfs", "file"}
    if not parsed.netloc or parsed.scheme not in allowed:
        raise ValueError(f"REST catalog {label} must be an absolute HTTP(S) URI")
    if http_only and parsed.scheme != "https":
        raise ValueError(f"REST catalog {label} must use HTTPS")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError(f"REST catalog {label} cannot contain credentials or query data")


def rest_catalog_factory(config: CatalogConfig, context: ExecutionContext) -> RestCatalog:
    return RestCatalog(config, context)
