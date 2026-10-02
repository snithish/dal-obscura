"""Deliberately invalid provider output for trust-boundary tests."""

from dal_obscura_plugin_api import DiscoveryPage


def discover_iceberg_tables(
    catalog_name,
    options,
    *,
    load_catalog_fn,
    max_namespaces=1000,
    max_tables=10000,
    deadline_at=None,
    cancel_check=None,
):
    """Drive the actual SQL SDK plugin with a replaceable provider I/O boundary."""
    from datetime import datetime, timedelta, timezone
    from time import monotonic
    from unittest.mock import patch

    from dal_obscura_plugin_api import CatalogConfig, ExecutionContext

    from dal_obscura.sources import sql_catalog
    from dal_obscura.sources.plugin_runtime import _close_plugin_preserving_error, _identifier_name

    remaining = 30 if deadline_at is None else deadline_at - monotonic()
    context = ExecutionContext(
        deadline=datetime.now(timezone.utc) + timedelta(seconds=remaining),
        correlation_id="sql-provider-contract",
        cancel_check=cancel_check,
    )
    with (
        patch.object(
            sql_catalog,
            "_load_iceberg_catalog",
            lambda name, values: load_catalog_fn(name, **values),
        ),
        patch.object(sql_catalog, "DEFAULT_MAX_NAMESPACES", max_namespaces),
        patch.object(sql_catalog, "DEFAULT_MAX_TABLES", max_tables),
    ):
        plugin = sql_catalog.SqlCatalog(
            CatalogConfig(
                plugin_id="iceberg.sql", instance_id=catalog_name, revision=0, options=options
            ),
            context,
        )
        try:
            identifiers = []
            continuation = None
            while True:
                page = plugin.list_tables(context, continuation=continuation)
                identifiers.extend(page.entries)
                if page.continuation is None:
                    return [
                        {
                            "backend": "iceberg",
                            "name": _identifier_name(item),
                            "table_identifier": _identifier_name(item),
                        }
                        for item in identifiers
                    ]
                continuation = page.continuation
        finally:
            _close_plugin_preserving_error(plugin)


def _unchecked_discovery_page(entries: object, continuation: object) -> DiscoveryPage:
    page = object.__new__(DiscoveryPage)
    object.__setattr__(page, "entries", entries)
    object.__setattr__(page, "continuation", continuation)
    return page
