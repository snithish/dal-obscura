"""Build complete read services while keeping each scenario's inputs explicit."""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

from dal_obscura.policy.authorization import AuthorizationPort
from dal_obscura.read.service import ReadService, ReadSettings
from dal_obscura.read.stream import ManagedStream, StreamGuard
from dal_obscura.read.transform import (
    DuckDBRowTransformAdapter,
)
from dal_obscura.sources.access import AccessContext
from tests.support.use_cases import FakeTicketCodec, FakeTicketStore, FixtureTaskCodec


class _UnusedSource:
    @contextmanager
    def open(self, catalog, target):
        raise AssertionError("This fetch scenario must not discover a source")
        yield


def make_read_service(**options: Any) -> ReadService:
    settings = {
        name: options.pop(name) for name in ReadSettings.__dataclass_fields__ if name in options
    }
    return ReadService(
        row_transform=options.pop("row_transform", DuckDBRowTransformAdapter()),
        access_context=options.pop("access_context", _UnusedSource()),
        ticket_codec=options.pop("ticket_codec", FakeTicketCodec()),
        ticket_store=options.pop("ticket_store", FakeTicketStore()),
        settings=ReadSettings(**settings),
        task_codec=options.pop("task_codec", FixtureTaskCodec()),
        **options,
    )


@dataclass(frozen=True)
class StaticAccessContext:
    """Composition for immutable catalogs/policies, including in-memory fixtures.

    Mutable live configuration must implement AccessContextPort with a shared
    snapshot instead of independently querying these two ports.
    """

    authorizer: AuthorizationPort
    catalog_registry: Any

    @contextmanager
    def open(self, catalog: str | None, target: str) -> Iterator[AccessContext]:
        table = self.catalog_registry.describe(catalog, target)
        yield AccessContext(table, self.authorizer, table.get_schema())


def expiry_stream(batches, *, ticket_expires_at, identity_expires_at, stream_deadline_at, now):
    return ManagedStream(
        batches,
        guard=StreamGuard(
            ticket_expires_at,
            identity_expires_at,
            stream_deadline_at,
            now,
            lambda: None,
        ),
    )
