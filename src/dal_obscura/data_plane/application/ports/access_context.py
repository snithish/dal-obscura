"""Atomic binding of executable source and authorization for one request."""

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import Protocol

from dal_obscura.common.catalog.ports import TableFormat
from dal_obscura.data_plane.application.ports.authorization import AuthorizationPort
from dal_obscura.data_plane.application.ports.catalog import CatalogRegistryPort


class AccessContextUnavailable(RuntimeError):
    """Planning cannot obtain a provider within its configured resource budget."""


@dataclass(frozen=True)
class AccessContext:
    table_format: TableFormat
    authorizer: AuthorizationPort


class AccessContextPort(Protocol):
    def open(self, catalog: str | None, target: str) -> AbstractContextManager[AccessContext]:
        """Bind source, policy and admission consistently; lease provider until exit."""
        ...


@dataclass(frozen=True)
class StaticAccessContext:
    """Composition for immutable catalogs/policies, including in-memory fixtures.

    Mutable live configuration must implement AccessContextPort with a shared
    snapshot instead of independently querying these two ports.
    """

    authorizer: AuthorizationPort
    catalog_registry: CatalogRegistryPort

    @contextmanager
    def open(self, catalog: str | None, target: str) -> Iterator[AccessContext]:
        yield AccessContext(self.catalog_registry.describe(catalog, target), self.authorizer)
