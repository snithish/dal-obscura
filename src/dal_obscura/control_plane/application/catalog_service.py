"""Workspace catalog service functions.

Example:
    ```python
    catalogs = list_workspace_catalogs(store)
    ```
"""

from __future__ import annotations

from typing import Any, cast
from urllib.parse import parse_qsl, urlsplit

from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.infrastructure.catalog_discovery import discover_catalog_tables
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore

CatalogDiscoverer = Any


def list_workspace_catalogs(store: PublicationStore) -> list[dict[str, object]]:
    """Lists catalogs configured in the default workspace.

    Example:
        ```python
        catalogs = list_workspace_catalogs(store)
        ```
    """

    context = store.get_default_workspace_context()
    if context is None:
        return []
    return store.list_workspace_catalogs(context)


def discover_workspace_catalog_tables(
    store: PublicationStore,
    name: str,
    *,
    discover: CatalogDiscoverer = discover_catalog_tables,
    egress_allowlist: tuple[str, ...] = (),
) -> dict[str, object]:
    """Discovers tables for a configured workspace catalog.

    Example:
        ```python
        result = discover_workspace_catalog_tables(store, "analytics")
        ```
    """

    context = _required_workspace_context(store)
    catalog = store.get_workspace_catalog(context, name)
    catalog_options = cast(dict[str, Any], catalog["options"])
    validate_catalog_options(catalog_options, egress_allowlist=egress_allowlist)
    tables = discover(
        str(catalog["name"]),
        str(catalog["module"]),
        catalog_options,
    )
    governed_targets = {
        value
        for asset in store.list_workspace_assets(context)
        if asset["catalog"] == catalog["name"]
        for value in (asset["name"], asset["table_identifier"])
        if isinstance(value, str) and value
    }
    return {
        "catalog": catalog["name"],
        "tables": [
            {
                **table,
                "target": table["name"],
                "governed": table["name"] in governed_targets
                or table["table_identifier"] in governed_targets,
            }
            for table in tables
        ],
    }


def upsert_workspace_catalog(
    store: PublicationStore,
    name: str,
    module: str,
    options: dict[str, Any],
    egress_allowlist: tuple[str, ...] = (),
) -> dict[str, str]:
    """Creates or updates a workspace catalog definition.

    Example:
        ```python
        result = upsert_workspace_catalog(store, "analytics", "iceberg", {})
        ```
    """

    validate_catalog_options(options, egress_allowlist=egress_allowlist)
    context = store.ensure_default_workspace_context()
    catalog_id = store.upsert_catalog(
        cell_id=context.cell_id,
        tenant_id=context.tenant_id,
        name=name,
        module=module,
        options=options,
    )
    return {"id": str(catalog_id), "name": name}


def _required_workspace_context(store: PublicationStore):
    context = store.get_default_workspace_context()
    if context is None:
        raise LookupError("No workspace has been configured")
    return context


def validate_catalog_options(
    options: dict[str, Any],
    *,
    egress_allowlist: tuple[str, ...] = (),
) -> None:
    """Rejects credential-bearing endpoints and enforces explicit host egress.

    Secret references remain configuration data; raw userinfo and query tokens
    are never accepted in a catalog URI. An empty allowlist is intentionally
    useful for local development, while production startup requires one.
    """

    normalized_allowlist = {item.strip().lower().rstrip(".") for item in egress_allowlist}
    for key, value in _walk_strings(options):
        if "://" not in value:
            continue
        parsed = urlsplit(value)
        if parsed.username or parsed.password:
            raise ValidationFailure(
                f"Catalog option {key!r} must use a secret reference, not URI credentials"
            )
        if any(
            name.lower() in {"access_token", "api_key", "password", "secret", "token"}
            for name, _item in parse_qsl(parsed.query, keep_blank_values=True)
        ):
            raise ValidationFailure(
                f"Catalog option {key!r} must not put secrets in a URI query"
            )
        hostname = parsed.hostname
        if (
            hostname
            and normalized_allowlist
            and hostname.lower().rstrip(".") not in normalized_allowlist
        ):
            raise ValidationFailure(
                f"Catalog endpoint host {hostname!r} is outside the configured egress allowlist"
            )


def _walk_strings(value: object, prefix: str = "options"):
    if isinstance(value, dict):
        for key, item in value.items():
            yield from _walk_strings(item, f"{prefix}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk_strings(item, f"{prefix}[{index}]")
    elif isinstance(value, str):
        yield prefix, value
