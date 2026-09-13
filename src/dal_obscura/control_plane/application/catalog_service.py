"""Workspace catalog service functions.

Example:
    ```python
    catalogs = list_workspace_catalogs(store)
    ```
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from math import isfinite
from threading import BoundedSemaphore, Lock
from typing import Any, cast
from urllib.parse import parse_qsl, urlsplit

from dal_obscura.common.plugin_api import PluginRegistry
from dal_obscura.control_plane.application.errors import ValidationFailure
from dal_obscura.control_plane.infrastructure.catalog_discovery import (
    ICEBERG_CATALOG_MODULE,
    discover_catalog_tables,
    discover_public_catalog_tables,
)
from dal_obscura.control_plane.infrastructure.repositories import PublicationStore
from dal_obscura.data_plane.infrastructure.adapters.secret_providers import (
    EnvSecretProvider,
    resolve_secret_refs,
)

CatalogDiscoverer = Any

_MAX_OPTION_DEPTH = 16
_MAX_OPTION_NODES = 1_024
_MAX_OPTION_KEYS = 128
_MAX_OPTION_ITEMS = 256
_MAX_OPTION_STRING = 4_096
DEFAULT_MAX_ACTIVE_DISCOVERIES_PER_SESSION = 2


class _SessionDiscoverySlot:
    def __init__(self) -> None:
        self.semaphore = BoundedSemaphore(DEFAULT_MAX_ACTIVE_DISCOVERIES_PER_SESSION)
        self.active = 0


_SESSION_DISCOVERY_SLOTS: dict[str, _SessionDiscoverySlot] = {}
_SESSION_DISCOVERY_LOCK = Lock()


@contextmanager
def _admit_session_discovery(session_key: str | None):
    """Admit at most two concurrent discoveries for one authenticated session."""

    if not session_key:
        yield
        return
    with _SESSION_DISCOVERY_LOCK:
        slot = _SESSION_DISCOVERY_SLOTS.setdefault(session_key, _SessionDiscoverySlot())
        if not slot.semaphore.acquire(blocking=False):
            raise ValidationFailure("Catalog discovery session capacity is exhausted; retry later")
        slot.active += 1
    try:
        yield
    finally:
        with _SESSION_DISCOVERY_LOCK:
            slot.semaphore.release()
            slot.active -= 1
            if slot.active == 0:
                _SESSION_DISCOVERY_SLOTS.pop(session_key, None)


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
    session_key: str | None = None,
    plugin_registry: PluginRegistry | None = None,
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
    catalog_options = _resolve_catalog_secrets(catalog_options)
    try:
        with _admit_session_discovery(session_key):
            if (
                plugin_registry is not None
                and str(catalog["module"]) != ICEBERG_CATALOG_MODULE
            ):
                tables = discover_public_catalog_tables(
                    str(catalog["name"]),
                    str(catalog["module"]),
                    catalog_options,
                    revision=_catalog_revision(catalog),
                    plugin_registry=plugin_registry,
                )
            else:
                tables = discover(
                    str(catalog["name"]),
                    str(catalog["module"]),
                    catalog_options,
                )
    except ValidationFailure:
        raise
    except Exception as exc:
        # Provider exceptions can include catalog URIs, credentials, or
        # implementation details. Keep those outside the browser/API boundary.
        raise ValidationFailure("Catalog discovery failed") from exc
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


def diagnose_workspace_catalog(
    store: PublicationStore,
    name: str,
    *,
    discover: CatalogDiscoverer = discover_catalog_tables,
    egress_allowlist: tuple[str, ...] = (),
    session_key: str | None = None,
    plugin_registry: PluginRegistry | None = None,
) -> dict[str, object]:
    """Runs bounded catalog discovery and returns a redacted readiness result."""

    context = _required_workspace_context(store)
    catalog = store.get_workspace_catalog(context, name)
    catalog_options = cast(dict[str, Any], catalog["options"])
    validate_catalog_options(catalog_options, egress_allowlist=egress_allowlist)
    catalog_options = _resolve_catalog_secrets(catalog_options)
    checked_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    try:
        with _admit_session_discovery(session_key):
            tables = list(
                discover_public_catalog_tables(
                    str(catalog["name"]),
                    str(catalog["module"]),
                    catalog_options,
                    revision=_catalog_revision(catalog),
                    plugin_registry=plugin_registry,
                )
                if plugin_registry is not None
                and str(catalog["module"]) != ICEBERG_CATALOG_MODULE
                else discover(
                    str(catalog["name"]),
                    str(catalog["module"]),
                    catalog_options,
                )
            )
    except ValidationFailure:
        raise
    except Exception:
        # Discovery exceptions can contain URIs, credentials, or provider
        # internals. Return a stable operator-safe message instead.
        return {
            "catalog": catalog["name"],
            "status": "unavailable",
            "message": "Catalog discovery failed",
            "checked_at": checked_at,
        }
    sample_tables = [
        str(table["name"])
        for table in tables[:10]
        if isinstance(table, dict) and table.get("name")
    ]
    return {
        "catalog": catalog["name"],
        "status": "ready",
        "message": "Catalog discovery succeeded",
        "checked_at": checked_at,
        "table_count": len(tables),
        "sample_tables": sample_tables,
    }


def upsert_workspace_catalog(
    store: PublicationStore,
    name: str,
    module: str,
    options: dict[str, Any],
    egress_allowlist: tuple[str, ...] = (),
    *,
    actor_principal: str = "system",
    plugin_registry: PluginRegistry | None = None,
) -> dict[str, str]:
    """Creates or updates a workspace catalog definition.

    Example:
        ```python
        result = upsert_workspace_catalog(store, "analytics", "iceberg", {})
        ```
    """

    if module != "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog":
        if plugin_registry is None:
            raise ValidationFailure("Catalog plugin is not admitted")
        admitted = plugin_registry.admitted() or plugin_registry.reload()
        if ("catalog", module) not in admitted:
            raise ValidationFailure("Catalog plugin is not admitted")
    validate_catalog_options(options, egress_allowlist=egress_allowlist)
    context = store.ensure_default_workspace_context()
    catalog_id = store.upsert_catalog(
        cell_id=context.cell_id,
        tenant_id=context.tenant_id,
        name=name,
        module=module,
        options=options,
    )
    store.record_workspace_audit_event(
        cell_id=context.cell_id,
        tenant_id=context.tenant_id,
        actor_principal=actor_principal,
        action="workspace.catalog.update",
        resource_type="catalog",
        resource_id=str(catalog_id),
        details={"name": name, "module": module, "option_keys": sorted(options)},
    )
    return {"id": str(catalog_id), "name": name}


def _required_workspace_context(store: PublicationStore):
    context = store.get_default_workspace_context()
    if context is None:
        raise LookupError("No workspace has been configured")
    return context


def _catalog_revision(catalog: dict[str, object]) -> int:
    raw_revision = catalog.get("revision", 0)
    if isinstance(raw_revision, bool):
        return 0
    if isinstance(raw_revision, (int, str)):
        try:
            return max(0, int(raw_revision))
        except ValueError:
            return 0
    return 0


def _resolve_catalog_secrets(options: dict[str, Any]) -> dict[str, Any]:
    try:
        resolved = resolve_secret_refs(options, provider=EnvSecretProvider())
    except ValueError as exc:
        raise ValidationFailure("Catalog secret could not be resolved") from exc
    return cast(dict[str, Any], resolved)


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

    _validate_option_shape(options)
    normalized_allowlist = {item.strip().lower().rstrip(".") for item in egress_allowlist}
    _reject_dynamic_loader_options(options)
    _reject_inline_secrets(options)
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


def _validate_option_shape(  # noqa: C901
    value: object,
    *,
    depth: int = 0,
    nodes: list[int] | None = None,
) -> None:
    """Bounds provider option JSON before any recursive security checks."""

    counter = nodes if nodes is not None else [0]
    counter[0] += 1
    if counter[0] > _MAX_OPTION_NODES:
        raise ValidationFailure("Catalog options contain too many values")
    if depth > _MAX_OPTION_DEPTH:
        raise ValidationFailure("Catalog options are too deeply nested")
    if isinstance(value, dict):
        if len(value) > _MAX_OPTION_KEYS:
            raise ValidationFailure("Catalog option object is too large")
        for key, nested in value.items():
            if not isinstance(key, str) or not key.strip() or len(key) > _MAX_OPTION_STRING:
                raise ValidationFailure("Catalog option key is invalid or too long")
            _validate_option_shape(nested, depth=depth + 1, nodes=counter)
        return
    if isinstance(value, list):
        if len(value) > _MAX_OPTION_ITEMS:
            raise ValidationFailure("Catalog option list is too large")
        for nested in value:
            _validate_option_shape(nested, depth=depth + 1, nodes=counter)
        return
    if isinstance(value, str):
        if len(value) > _MAX_OPTION_STRING:
            raise ValidationFailure("Catalog option string is too long")
        return
    if value is None or isinstance(value, (bool, int)):
        return
    if isinstance(value, float):
        if not isfinite(value):
            raise ValidationFailure("Catalog option numbers must be finite")
        return
    raise ValidationFailure("Catalog options must contain JSON-compatible values")


_SECRET_OPTION_KEYS = {
    "access_token",
    "api_key",
    "client_secret",
    "credential",
    "credentials",
    "password",
    "passwd",
    "private_key",
    "secret",
    "token",
}


def _reject_inline_secrets(value: object, prefix: str = "options") -> None:
    """Requires sensitive catalog options to use an explicit secret reference."""

    if isinstance(value, dict):
        mapping = cast(dict[str, object], value)
        if set(mapping) == {"secret"} and isinstance(mapping.get("secret"), str):
            return
        for key, nested in mapping.items():
            name = str(key).strip().lower()
            path = f"{prefix}.{key}"
            if name in _SECRET_OPTION_KEYS and (
                not isinstance(nested, dict)
                or set(nested) != {"secret"}
                or not isinstance(cast(dict[str, object], nested).get("secret"), str)
            ):
                raise ValidationFailure(f"Catalog option {path!r} must use a secret reference")
            _reject_inline_secrets(nested, path)
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _reject_inline_secrets(nested, f"{prefix}[{index}]")


_DYNAMIC_LOADER_OPTION_KEYS = {
    "py-catalog-impl",
    "py-io-impl",
    "catalog-impl",
    "io-impl",
    "class-path",
    "implementation-class",
}


def _reject_dynamic_loader_options(value: object, prefix: str = "options") -> None:
    """Rejects provider settings that select arbitrary installed Python classes."""

    if isinstance(value, dict):
        for key, nested in cast(dict[str, object], value).items():
            path = f"{prefix}.{key}"
            if str(key).strip().lower() in _DYNAMIC_LOADER_OPTION_KEYS:
                raise ValidationFailure(
                    f"Catalog option {path!r} cannot select an implementation class"
                )
            _reject_dynamic_loader_options(nested, path)
    elif isinstance(value, list):
        for index, nested in enumerate(value):
            _reject_dynamic_loader_options(nested, f"{prefix}[{index}]")
