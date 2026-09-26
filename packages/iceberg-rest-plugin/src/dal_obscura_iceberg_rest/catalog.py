from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime
from threading import Lock
from typing import Any, cast
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
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_READ_TIMEOUT_SECONDS = 30.0
MIN_TIMEOUT_SECONDS = 0.1
MAX_CONNECT_TIMEOUT_SECONDS = 30.0
MAX_READ_TIMEOUT_SECONDS = 120.0
_ALLOWED_OPTIONS = frozenset(
    {
        "uri",
        "warehouse",
        "token",
        "credential",
        "scope",
        "oauth2-server-uri",
        "connect-timeout-ms",
        "read-timeout-ms",
    }
)
_ACTIVE_REQUEST_BUDGET: ContextVar[tuple[datetime, Any, float, float] | None] = ContextVar(
    "dal_obscura_rest_request_budget", default=None
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
    output_formats=frozenset({"iceberg"}),
    handle_versions=frozenset({1}),
    config_schema={
        "fields": [
            {"name": "uri", "type": "uri", "required": True, "secret": False},
            {"name": "warehouse", "type": "string", "required": False, "secret": False},
            {"name": "token", "type": "secret_reference", "required": False, "secret": True},
            {"name": "credential", "type": "secret_reference", "required": False, "secret": True},
            {"name": "scope", "type": "string", "required": False, "secret": False},
            {"name": "oauth2-server-uri", "type": "uri", "required": False, "secret": False},
            {"name": "connect-timeout-ms", "type": "integer", "required": False, "secret": False},
            {"name": "read-timeout-ms", "type": "integer", "required": False, "secret": False},
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
        try:
            port = parsed.port
        except ValueError as exc:
            raise ValueError("REST catalog URI has an invalid port") from exc
        _validate_port(port, "REST catalog URI")
        if parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("REST catalog URI cannot contain credentials or query data")
        if parsed.scheme == "http" and any(key in options for key in ("token", "credential")):
            raise ValueError("REST catalog credentials require an HTTPS URI")
        warehouse = options.get("warehouse")
        if warehouse is not None:
            _validate_optional_uri(warehouse, "warehouse")
        oauth_uri = options.get("oauth2-server-uri")
        if oauth_uri is not None:
            _validate_optional_uri(oauth_uri, "oauth2-server-uri", http_only=True)
        self._connect_timeout = _timeout_seconds(
            cast(str | None, options.get("connect-timeout-ms")),
            default=DEFAULT_CONNECT_TIMEOUT_SECONDS,
            maximum=MAX_CONNECT_TIMEOUT_SECONDS,
            label="connect-timeout-ms",
        )
        self._read_timeout = _timeout_seconds(
            cast(str | None, options.get("read-timeout-ms")),
            default=DEFAULT_READ_TIMEOUT_SECONDS,
            maximum=MAX_READ_TIMEOUT_SECONDS,
            label="read-timeout-ms",
        )
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
        with self._request_budget(context):
            catalog = self._load_catalog(context)
            result: set[tuple[str, ...]] = set()
            raw_namespaces = catalog.list_namespaces(namespace)
            for raw in raw_namespaces:
                self._validate_context(context)
                if (
                    not isinstance(raw, (tuple, list))
                    or not raw
                    or any(not isinstance(part, str) or not part for part in raw)
                ):
                    raise ValueError("REST catalog returned an invalid namespace")
                value = tuple(raw)
                if namespace and value[: len(namespace)] != namespace:
                    continue
                result.add(value)
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
        with self._request_budget(context):
            if not 1 <= limit <= MAX_PAGE:
                raise ValueError("REST catalog page size is out of bounds")
            offset = _continuation_offset(continuation)
            catalog = self._load_catalog(context)
            identifiers = []
            for index, namespace in enumerate(catalog.list_namespaces(())):
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
        with self._request_budget(context):
            table = self._load_catalog(context).load_table((*identifier.namespace, identifier.name))
            self._validate_context(context)
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
            first_error: Exception | None = None
            close = getattr(catalog, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as exc:
                    first_error = exc
            session = getattr(catalog, "_session", None)
            close = getattr(session, "close", None)
            if callable(close):
                try:
                    close()
                except Exception as exc:
                    if first_error is None:
                        first_error = exc
            if first_error is not None:
                raise first_error

    def _load_catalog(self, context: ExecutionContext):
        self._validate_context(context)
        if self._closed:
            raise ValueError("REST catalog is closed")
        if self._catalog is None:
            with self._catalog_lock:
                if self._closed:
                    raise ValueError("REST catalog is closed")
                if self._catalog is None:
                    properties = {
                        key: str(value)
                        for key, value in self._options.items()
                        if key not in {"uri", "connect-timeout-ms", "read-timeout-ms"}
                    }
                    self._catalog = _create_catalog(
                        self._config.instance_id,
                        uri=str(self._options["uri"]),
                        connect_timeout=self._connect_timeout,
                        read_timeout=self._read_timeout,
                        **properties,
                    )
        return self._catalog

    @contextmanager
    def _request_budget(self, context: ExecutionContext) -> Iterator[None]:
        self._validate_context(context)
        token = _ACTIVE_REQUEST_BUDGET.set(
            (context.deadline, context.cancel_check, self._connect_timeout, self._read_timeout)
        )
        try:
            yield
        finally:
            _ACTIVE_REQUEST_BUDGET.reset(token)

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
    if callable(snapshot):
        snapshot = snapshot()
    value = getattr(snapshot, "snapshot_id", snapshot)
    return str(value) if value is not None else None


def _validate_optional_uri(value: object, label: str, *, http_only: bool = False) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"REST catalog {label} must be a URI")
    parsed = urlsplit(value)
    allowed = {"http", "https", "s3", "gs", "abfs", "file"}
    if parsed.scheme not in allowed or (parsed.scheme != "file" and not parsed.netloc):
        raise ValueError(f"REST catalog {label} must be an absolute URI with a supported scheme")
    if parsed.scheme == "file" and parsed.netloc:
        raise ValueError(f"REST catalog {label} file URI must be local")
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(f"REST catalog {label} has an invalid port") from exc
    _validate_port(port, f"REST catalog {label}")
    if http_only and parsed.scheme != "https":
        raise ValueError(f"REST catalog {label} must use HTTPS")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError(f"REST catalog {label} cannot contain credentials or query data")


def _timeout_seconds(
    value: str | None,
    *,
    default: float,
    maximum: float,
    label: str,
) -> float:
    if value is None:
        return default
    if not value.isdigit():
        raise ValueError(f"REST catalog {label} must be a positive integer in milliseconds")
    milliseconds = int(value)
    seconds = milliseconds / 1000
    if not MIN_TIMEOUT_SECONDS <= seconds <= maximum:
        raise ValueError(f"REST catalog {label} is out of bounds")
    return seconds


def _validate_port(port: int | None, label: str) -> None:
    """Validate an authority port while keeping malformed values out of providers."""

    if port is not None and not 1 <= port <= 65_535:
        raise ValueError(f"{label} has an invalid port")


def _create_catalog(
    name: str,
    *,
    uri: str,
    connect_timeout: float,
    read_timeout: float,
    **properties: str,
) -> Any:
    """Construct PyIceberg REST with a bounded requests session.

    PyIceberg's REST adapter does not pass a timeout to ``requests``.  The
    subclass keeps provider behavior intact while applying a connect/read
    timeout to every request, including the initial config fetch in its
    constructor.  The active execution context narrows the timeout further.
    """

    from pyiceberg.catalog.rest import RestCatalog as PyIcebergRestCatalog

    class BoundedRestCatalog(PyIcebergRestCatalog):
        def _create_session(self):
            session = super()._create_session()
            _install_request_timeout(session, connect_timeout, read_timeout)
            return session

    return BoundedRestCatalog(name, uri=uri, **properties)


def _install_request_timeout(session: Any, connect_timeout: float, read_timeout: float) -> None:
    original_request = session.request

    def request(method: str, url: str, **kwargs: Any) -> Any:
        budget = _ACTIVE_REQUEST_BUDGET.get()
        if budget is None:
            timeout = (connect_timeout, read_timeout)
        else:
            deadline, cancel_check, configured_connect, configured_read = budget
            if cancel_check is not None and cancel_check():
                raise ValueError("REST catalog operation was cancelled")
            remaining = (deadline - datetime.now(deadline.tzinfo)).total_seconds()
            if remaining <= 0:
                raise TimeoutError("REST catalog execution deadline has expired")
            timeout = (
                min(configured_connect, remaining),
                min(configured_read, remaining),
            )
        # Provider methods must not be able to opt out of the gateway budget
        # by supplying an unbounded or larger requests timeout.
        kwargs["timeout"] = timeout
        # Redirects are a new destination and must be revalidated by the
        # governed connection boundary before any follow-up request.
        kwargs["allow_redirects"] = False
        response = original_request(method, url, **kwargs)
        if budget is not None and cancel_check is not None and cancel_check():
            close = getattr(response, "close", None)
            if callable(close):
                close()
            raise ValueError("REST catalog operation was cancelled")
        return response

    session.request = request


def rest_catalog_factory(config: CatalogConfig, context: ExecutionContext) -> RestCatalog:
    return RestCatalog(config, context)
