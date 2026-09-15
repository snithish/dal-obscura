"""Workspace catalog service functions.

Example:
    ```python
    catalogs = list_workspace_catalogs(store)
    ```
"""

from __future__ import annotations

import ipaddress
import math
from contextlib import contextmanager
from datetime import datetime, timezone
from math import isfinite
from threading import BoundedSemaphore, Lock
from typing import Any, cast
from urllib.parse import parse_qsl, urlsplit

from dal_obscura_plugin_api import PluginDescriptor

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
    SecretProvider,
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


def list_workspace_catalogs(
    store: PublicationStore,
    *,
    plugin_registry: PluginRegistry | None = None,
) -> list[dict[str, object]]:
    """Lists catalogs configured in the default workspace.

    Example:
        ```python
        catalogs = list_workspace_catalogs(store)
        ```
    """

    context = store.get_default_workspace_context()
    if context is None:
        return []
    catalogs = store.list_workspace_catalogs(context)
    if plugin_registry is None:
        return catalogs
    # Admission is a startup concern. Request paths read the immutable
    # snapshot and fail closed when startup did not admit the plugin.
    admitted = plugin_registry.admitted()
    for catalog in catalogs:
        module = str(catalog.get("module", ""))
        if module == ICEBERG_CATALOG_MODULE:
            catalog["plugin_id"] = "iceberg.sql"
        elif ("catalog", module) in admitted:
            catalog["plugin_id"] = module
        else:
            catalog["plugin_id"] = None
    return catalogs


def discover_workspace_catalog_tables(
    store: PublicationStore,
    name: str,
    *,
    discover: CatalogDiscoverer = discover_catalog_tables,
    egress_allowlist: tuple[str, ...] = (),
    session_key: str | None = None,
    plugin_registry: PluginRegistry | None = None,
    secret_provider: SecretProvider | None = None,
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
    validate_admitted_catalog_options(str(catalog["module"]), catalog_options, plugin_registry)
    validate_catalog_options(catalog_options, egress_allowlist=egress_allowlist)
    catalog_options = _resolve_catalog_secrets(
        catalog_options,
        scope=f"catalog:{name}",
        provider=secret_provider,
    )
    try:
        with _admit_session_discovery(session_key):
            if plugin_registry is not None and str(catalog["module"]) != ICEBERG_CATALOG_MODULE:
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
    secret_provider: SecretProvider | None = None,
) -> dict[str, object]:
    """Runs bounded catalog discovery and returns a redacted readiness result."""

    context = _required_workspace_context(store)
    catalog = store.get_workspace_catalog(context, name)
    catalog_options = cast(dict[str, Any], catalog["options"])
    validate_admitted_catalog_options(str(catalog["module"]), catalog_options, plugin_registry)
    validate_catalog_options(catalog_options, egress_allowlist=egress_allowlist)
    catalog_options = _resolve_catalog_secrets(
        catalog_options,
        scope=f"catalog:{name}",
        provider=secret_provider,
    )
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
                if plugin_registry is not None and str(catalog["module"]) != ICEBERG_CATALOG_MODULE
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
        str(table["name"]) for table in tables[:10] if isinstance(table, dict) and table.get("name")
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
    expected_revision: int | None = None,
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
        admitted = plugin_registry.admitted()
        if ("catalog", module) not in admitted:
            raise ValidationFailure("Catalog plugin is not admitted")
        validate_descriptor_options(admitted[("catalog", module)], options)
    validate_catalog_options(options, egress_allowlist=egress_allowlist)
    context = store.ensure_default_workspace_context()
    catalog_id = store.upsert_catalog(
        cell_id=context.cell_id,
        tenant_id=context.tenant_id,
        name=name,
        module=module,
        options=options,
        expected_revision=expected_revision,
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


def validate_admitted_catalog_options(
    module: str,
    options: dict[str, Any],
    plugin_registry: PluginRegistry | Any | None,
) -> None:
    """Validate persisted external catalog options before provider use.

    Catalog rows can predate descriptor validation or be restored from an
    older deployment. Every discovery and schema path therefore repeats the
    admitted descriptor check before resolving secrets or loading a factory.
    The built-in Iceberg compatibility module retains its legacy option
    contract and is validated by ``validate_catalog_options``.
    """

    if module == ICEBERG_CATALOG_MODULE:
        return
    if plugin_registry is None:
        raise ValidationFailure("Catalog plugin is not admitted")
    admitted = plugin_registry.admitted()
    descriptor = admitted.get(("catalog", module))
    if descriptor is None:
        raise ValidationFailure("Catalog plugin is not admitted")
    validate_descriptor_options(descriptor, options)


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


def _resolve_catalog_secrets(
    options: dict[str, Any],
    *,
    scope: str | None = None,
    provider: SecretProvider | None = None,
) -> dict[str, Any]:
    try:
        resolved = resolve_secret_refs(
            options,
            provider=provider or EnvSecretProvider(),
            expected_scope=scope,
        )
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
            raise ValidationFailure(f"Catalog option {key!r} must not put secrets in a URI query")
        hostname = parsed.hostname
        if hostname:
            _validate_ip_literal(hostname, key=key, allowlist=normalized_allowlist)
        if (
            hostname
            and normalized_allowlist
            and hostname.lower().rstrip(".") not in normalized_allowlist
        ):
            raise ValidationFailure(
                f"Catalog endpoint host {hostname!r} is outside the configured egress allowlist"
            )


def _validate_ip_literal(hostname: str, *, key: str, allowlist: set[str]) -> None:
    """Reject unsafe literal destinations before a provider can open a socket.

    Explicitly allowlisted private and loopback addresses remain available for
    internal catalogs and local development. Link-local, unspecified, and
    multicast addresses are always denied because they include metadata-service
    and wildcard destinations that are not valid catalog endpoints.
    """

    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return
    normalized = hostname.lower().rstrip(".")
    if address.is_link_local or address.is_unspecified or address.is_multicast:
        raise ValidationFailure(f"Catalog option {key!r} targets a disallowed special address")
    if (
        address.is_private or address.is_loopback or address.is_reserved
    ) and normalized not in allowlist:
        raise ValidationFailure(f"Catalog endpoint host {hostname!r} is a private address")


def validate_descriptor_options(
    descriptor: PluginDescriptor,
    options: dict[str, Any],
    *,
    kind: str = "Catalog",
) -> None:
    """Enforce the bounded declarative fields exposed by an admitted plugin."""

    raw_fields = descriptor.config_schema.get("fields")
    if not isinstance(raw_fields, list):
        return
    field_specs = [cast(dict[str, object], item) for item in raw_fields if isinstance(item, dict)]
    fields = {item.get("name") for item in field_specs if isinstance(item.get("name"), str)}
    raw_defaults = descriptor.config_schema.get("defaults")
    if isinstance(raw_defaults, dict):
        fields.update(key for key in raw_defaults if isinstance(key, str))
    if unknown := sorted(set(options) - fields):
        raise ValidationFailure(f"{kind} options contain unsupported fields: " + ", ".join(unknown))
    required: set[str] = set()
    for item in field_specs:
        name = item.get("name")
        if isinstance(name, str) and name in options:
            _validate_descriptor_value(
                name,
                item.get("type"),
                options[name],
                kind=kind,
                choices=item.get("options"),
            )
        if item.get("required") is True and isinstance(name, str):
            required.add(name)
    missing = sorted(
        name for name in required if name not in options or options[name] in (None, "")
    )
    if missing:
        raise ValidationFailure(
            f"{kind} options are missing required fields: " + ", ".join(missing)
        )


def _validate_descriptor_value(  # noqa: C901
    name: str,
    field_type: object,
    value: object,
    *,
    kind: str,
    choices: object = None,
) -> None:
    if field_type in (None, "string", "uri") and not isinstance(value, str):
        raise ValidationFailure(f"{kind} option {name!r} must be a string")
    if field_type == "boolean" and not isinstance(value, bool):
        raise ValidationFailure(f"{kind} option {name!r} must be a boolean")
    if field_type == "integer" and (isinstance(value, bool) or not isinstance(value, int)):
        raise ValidationFailure(f"{kind} option {name!r} must be an integer")
    if field_type == "number" and (
        isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
    ):
        raise ValidationFailure(f"{kind} option {name!r} must be a finite number")
    if field_type == "enum":
        if not isinstance(choices, list) or not choices:
            raise ValidationFailure(f"{kind} option {name!r} has no valid enum choices")
        if value not in choices:
            raise ValidationFailure(f"{kind} option {name!r} must be one of the declared choices")
    if field_type == "secret_reference":
        if not isinstance(value, dict) or set(value) != {"secret", "scope"}:
            raise ValidationFailure(f"{kind} option {name!r} must be an explicit secret reference")
        reference = cast(dict[str, object], value)
        secret_name = reference.get("secret")
        if not isinstance(secret_name, str) or not secret_name.strip():
            raise ValidationFailure(f"{kind} option {name!r} has an invalid secret reference")
        if "scope" in reference and (
            not isinstance(reference.get("scope"), str)
            or not str(reference["scope"]).strip()
            or len(str(reference["scope"])) > _MAX_OPTION_STRING
        ):
            raise ValidationFailure(f"{kind} option {name!r} has an invalid secret scope")
        return
    if field_type not in (None, "string", "uri", "boolean", "integer", "enum", "number"):
        raise ValidationFailure(f"{kind} option {name!r} has an unsupported field type")


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
        if (
            set(mapping) == {"secret", "scope"}
            and isinstance(mapping.get("secret"), str)
            and isinstance(mapping.get("scope"), str)
            and bool(str(mapping.get("scope")).strip())
        ):
            return
        for key, nested in mapping.items():
            name = str(key).strip().lower()
            path = f"{prefix}.{key}"
            if name in _SECRET_OPTION_KEYS and (
                not isinstance(nested, dict)
                or set(nested) != {"secret", "scope"}
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
