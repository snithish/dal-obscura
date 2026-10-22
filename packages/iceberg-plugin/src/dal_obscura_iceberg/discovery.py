"""Bounded traversal of PyIceberg namespaces and table identifiers."""

from collections import deque
from collections.abc import Callable, Iterable, Iterator
from time import monotonic
from typing import cast

from dal_obscura_plugin_api import TableIdentifier
from dal_obscura_plugin_api.identifiers import identifier_name, table_identifier
from pyiceberg.catalog import Catalog

Namespace = tuple[str, ...]
DEFAULT_MAX_NAMESPACES = 1_000
DEFAULT_MAX_TABLES = 10_000


def _walk_namespaces(
    catalog: Catalog,
    *,
    max_namespaces: int = DEFAULT_MAX_NAMESPACES,
    deadline_at: float | None = None,
    cancel_check: Callable[[], bool] | None = None,
) -> list[Namespace]:
    namespaces: list[Namespace] = []
    pending: deque[Namespace] = deque([()])
    seen: set[Namespace] = set()
    while pending:
        namespace = pending.popleft()
        if namespace in seen:
            continue
        _check_budget(deadline_at, cancel_check)
        if len(seen) >= max_namespaces:
            raise ValueError(f"Catalog discovery exceeded the {max_namespaces}-namespace limit")
        seen.add(namespace)
        namespaces.append(namespace)
        for child in _bounded_provider_items(
            catalog.list_namespaces(namespace),
            limit=max_namespaces - len(seen),
            kind="namespace",
            deadline_at=deadline_at,
            cancel_check=cancel_check,
        ):
            pending.append(_namespace_tuple(child))
    return namespaces


def _list_tables(catalog: Catalog, namespace: Namespace) -> Iterable[object]:
    try:
        return catalog.list_tables(namespace)
    except ValueError as exc:
        # PyIceberg SQL catalogs reject the root namespace instead of returning
        # an empty result. Tables still appear under the namespaces discovered
        # by _walk_namespaces; retain other provider errors.
        if not namespace and str(exc) == "Empty namespace identifier":
            return ()
        raise


def _namespace_tuple(namespace: object) -> Namespace:
    if isinstance(namespace, str):
        parts = tuple(namespace.split("."))
        if any(not _valid_identifier_segment(part) for part in parts):
            raise ValueError("Catalog provider returned an invalid namespace")
        return parts
    if isinstance(namespace, tuple):
        if any(not _valid_identifier_segment(part) for part in namespace):
            raise ValueError("Catalog provider returned an invalid namespace")
        return cast(tuple[str, ...], namespace)
    if isinstance(namespace, list):
        if any(not _valid_identifier_segment(part) for part in namespace):
            raise ValueError("Catalog provider returned an invalid namespace")
        return cast(tuple[str, ...], tuple(namespace))
    raise ValueError("Catalog provider returned an invalid namespace")


def _identifier_to_name(identifier: object) -> str:
    if isinstance(identifier, str):
        try:
            return identifier_name(table_identifier(identifier))
        except ValueError as exc:
            raise ValueError("Catalog provider returned an invalid table identifier") from exc
    if isinstance(identifier, (tuple, list)):
        if any(not _valid_identifier_segment(part) for part in identifier):
            raise ValueError("Catalog provider returned an invalid table identifier")
        parts = cast(tuple[str, ...], tuple(identifier))
        if not parts:
            raise ValueError("Catalog provider returned an invalid table identifier")
        return identifier_name(TableIdentifier(namespace=parts[:-1], name=parts[-1]))
    raise ValueError("Catalog provider returned an invalid table identifier")


def _valid_identifier_segment(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and len(value) <= 256
        and all(ord(char) >= 0x20 and ord(char) != 0x7F for char in value)
    )


def _bounded_provider_items(
    values: Iterable[object],
    *,
    limit: int,
    kind: str,
    deadline_at: float | None,
    cancel_check: Callable[[], bool] | None,
) -> Iterator[object]:
    if limit < 0:
        raise ValueError(f"Catalog discovery exceeded the {kind} limit")
    for index, value in enumerate(values):
        _check_budget(deadline_at, cancel_check)
        if index >= limit:
            raise ValueError(f"Catalog discovery exceeded the {kind} limit")
        yield value


def _check_budget(deadline_at: float | None, cancel_check: Callable[[], bool] | None) -> None:
    if cancel_check is not None and cancel_check():
        raise InterruptedError("Catalog discovery cancelled")
    if deadline_at is not None and monotonic() >= deadline_at:
        raise TimeoutError("Catalog discovery deadline exceeded")
