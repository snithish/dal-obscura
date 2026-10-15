"""Bounded catalog options, descriptor admission and credential validation."""

from __future__ import annotations

import ipaddress
import math
from collections.abc import Iterator, Mapping
from math import isfinite
from typing import Any, cast
from urllib.parse import parse_qsl, urlsplit

from dal_obscura_plugin_api import PluginDescriptor

from dal_obscura.application.governance.errors import ValidationFailure
from dal_obscura.application.governance.ports import GovernancePluginRegistry

ICEBERG_CATALOG_ID = "iceberg.catalog"
_MAX_OPTION_DEPTH = 16
_MAX_OPTION_NODES = 1_024
_MAX_OPTION_KEYS = 128
_MAX_OPTION_ITEMS = 256
_MAX_OPTION_STRING = 4_096

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
_DYNAMIC_LOADER_OPTION_KEYS = {
    "py-catalog-impl",
    "py-io-impl",
    "catalog-impl",
    "io-impl",
    "class-path",
    "implementation-class",
}


def validate_admitted_catalog_options(
    plugin_id: str,
    options: dict[str, Any],
    plugin_registry: GovernancePluginRegistry | None,
) -> None:
    """Validate persisted external catalog options before provider use.

    Stored configuration crosses a trust boundary. Discovery and schema
    operations validate the admitted descriptor before resolving secrets or
    loading a factory.
    Built-in Iceberg options use the PyIceberg configuration contract and are
    validated by ``validate_catalog_options``.
    """

    if plugin_id == ICEBERG_CATALOG_ID:
        return
    if plugin_registry is None:
        raise ValidationFailure("Catalog plugin is not admitted")
    admitted = plugin_registry.admitted()
    descriptor = admitted.get(("catalog", plugin_id))
    if descriptor is None:
        raise ValidationFailure("Catalog plugin is not admitted")
    validate_descriptor_options(descriptor, options)


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

    if "catalog_name" in options:
        raise ValidationFailure("Unsupported catalog option: catalog_name")
    _validate_option_shape(options)
    normalized_allowlist = {item.strip().lower().rstrip(".") for item in egress_allowlist}
    _reject_dynamic_loader_options(options)
    _reject_inline_secrets(options)
    for key, value in _walk_strings(options):
        if "://" not in value:
            continue
        try:
            parsed = urlsplit(value)
            # Accessing hostname/credentials can itself validate bracketed
            # hosts and ports, so keep those parser failures inside the
            # user-facing validation boundary as well.
            username = parsed.username
            password = parsed.password
            hostname = parsed.hostname
        except ValueError as exc:
            raise ValidationFailure(f"Catalog option {key!r} contains an invalid URI") from exc
        if parsed.scheme.lower() == "file" and parsed.netloc:
            raise ValidationFailure(
                f"Catalog option {key!r} must use a local file URI without an authority"
            )
        if password or (username and parsed.scheme.lower() not in {"abfs", "abfss"}):
            raise ValidationFailure(
                f"Catalog option {key!r} must use a secret reference, not URI credentials"
            )
        if any(
            name.lower() in {"access_token", "api_key", "password", "secret", "token"}
            for name, _item in parse_qsl(parsed.query, keep_blank_values=True)
        ):
            raise ValidationFailure(f"Catalog option {key!r} must not put secrets in a URI query")
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
    if not isinstance(raw_fields, (list, tuple)):
        return
    field_specs = [
        cast(Mapping[str, object], item) for item in raw_fields if isinstance(item, Mapping)
    ]
    fields = {item.get("name") for item in field_specs if isinstance(item.get("name"), str)}
    raw_defaults = descriptor.config_schema.get("defaults")
    if isinstance(raw_defaults, Mapping):
        fields.update(key for key in raw_defaults if isinstance(key, str))
    if unknown := sorted(set(options) - fields):
        raise ValidationFailure(f"{kind} options contain unsupported fields: " + ", ".join(unknown))
    required: set[str] = set()
    for item in field_specs:
        name = item.get("name")
        if isinstance(name, str) and name in options:
            _validate_descriptor_value(
                name, item.get("type"), options[name], kind=kind, choices=item.get("options")
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
        if not isinstance(choices, (list, tuple)) or not choices:
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


def _walk_strings(value: object, prefix: str = "options") -> Iterator[tuple[str, str]]:
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
