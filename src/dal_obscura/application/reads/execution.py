"""Explicit ownership for lazy streams, including streams never advanced."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from threading import BoundedSemaphore

import pyarrow as pa


class ManagedStream(Iterator[pa.RecordBatch]):
    def __init__(
        self,
        batches: Iterable[pa.RecordBatch],
        *,
        guard: Callable[[], None] | None = None,
        resources: tuple[object, ...] = (),
        progress: Callable[[], None] | None = None,
        schema: pa.Schema | None = None,
    ) -> None:
        self._closed = False
        self._guard = guard
        self._progress = progress
        self._schema = schema
        try:
            self._iterator = iter(batches)
        except BaseException:
            _close_resources((batches, *resources), suppress_errors=True)
            raise
        self._resources = (self._iterator, batches, *resources)

    def __next__(self) -> pa.RecordBatch:
        if self._closed:
            raise StopIteration
        try:
            if self._progress is not None:
                self._progress()
            batch = next(self._iterator)
            if self._progress is not None:
                self._progress()
            if self._schema is not None and not batch.schema.equals(
                self._schema, check_metadata=True
            ):
                raise ValueError("Arrow input schema does not match declared execution schema")
            if self._guard is not None:
                self._guard()
            return batch
        except StopIteration:
            self.close()
            raise
        except BaseException:
            self.close(suppress_errors=True)
            raise

    def close(self, *, suppress_errors: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        resources, self._resources = self._resources, ()
        self._iterator = iter(())
        _close_resources(resources, suppress_errors=suppress_errors)


def _close_resources(resources: tuple[object, ...], *, suppress_errors: bool) -> None:
    seen: set[int] = set()
    failure: BaseException | None = None
    for resource in resources:
        if id(resource) in seen:
            continue
        seen.add(id(resource))
        close = getattr(resource, "close", None)
        if callable(close):
            try:
                close()
            except BaseException as exc:
                failure = failure or exc
    if failure is not None and not suppress_errors:
        raise failure


@dataclass(frozen=True)
class StreamGuard:
    """Separate cooperative input progress from strict output disclosure checks."""

    ticket_expires_at: int
    identity_expires_at: int | None
    stream_deadline_at: int
    now: Callable[[], int]
    check_revocation: Callable[[], None]

    cancel_check: Callable[[], bool] | None = None

    def check_progress(self) -> None:
        if self.cancel_check is not None and self.cancel_check():
            raise StreamCancelledError("Stream cancelled")
        current_time = self.now()
        if current_time >= self.stream_deadline_at:
            raise TimeoutError("Stream deadline exceeded")
        if current_time >= self.ticket_expires_at:
            raise PermissionError("Ticket expired")
        if self.identity_expires_at is not None and current_time >= self.identity_expires_at:
            raise PermissionError("Identity expired")

    def check_disclosure(self) -> None:
        self.check_progress()
        self.check_revocation()


class StreamResourceError(RuntimeError):
    """A governed stream exceeded an operational resource budget."""


class StreamAdmissionError(StreamResourceError):
    """Raised when the configured governed-stream capacity is exhausted."""


class StreamCancelledError(TimeoutError):
    """Cooperative cancellation observed at an execution checkpoint."""


class SourceInitializationValueError(ValueError):
    """Invalid provider initialization, distinguished from later input failures."""


class SourceInitializationPermissionError(PermissionError):
    """Denied provider initialization, distinguished from progress expiry."""


class ExecutionSupervisor:
    """One process owner admits lazy executions before their source is opened."""

    def __init__(self, *, max_active_streams: int = 16) -> None:
        if max_active_streams < 1:
            raise ValueError("max_active_streams must be positive")
        self._slots = BoundedSemaphore(max_active_streams)

    def open(
        self,
        source_factory: Callable[[], tuple[pa.Schema, Iterable[pa.RecordBatch]]],
        transform_factory: Callable[[Iterable[pa.RecordBatch]], Iterable[pa.RecordBatch]],
        guard: StreamGuard | None,
    ) -> ExecutionLease:
        return ExecutionLease(self, source_factory, transform_factory, guard)

    def _acquire(self) -> None:
        if not self._slots.acquire(blocking=False):
            raise StreamAdmissionError("Governed stream capacity is exhausted; retry later")

    def _release(self) -> None:
        self._slots.release()


class ExecutionLease(Iterator[pa.RecordBatch]):
    """A lazy, idempotently closed execution holding at most one admission slot."""

    def __init__(
        self,
        supervisor: ExecutionSupervisor,
        source_factory: Callable[[], tuple[pa.Schema, Iterable[pa.RecordBatch]]],
        transform_factory: Callable[[Iterable[pa.RecordBatch]], Iterable[pa.RecordBatch]],
        guard: StreamGuard | None,
    ) -> None:
        self._supervisor = supervisor
        self._source_factory: Callable[[], tuple[pa.Schema, Iterable[pa.RecordBatch]]] | None = (
            source_factory
        )
        self._transform_factory: (
            Callable[[Iterable[pa.RecordBatch]], Iterable[pa.RecordBatch]] | None
        ) = transform_factory
        self._guard = guard
        self._iterator: Iterator[pa.RecordBatch] | None = None
        self._resources: tuple[object, ...] = ()
        self._admitted = False
        self._closed = False

    def _initialize(self) -> None:
        source_factory, transform_factory = self._source_factory, self._transform_factory
        assert source_factory is not None and transform_factory is not None
        self._supervisor._acquire()
        self._admitted = True
        if self._guard is not None:
            self._guard.check_progress()
        schema, batches = source_factory()
        self._resources = (batches,)
        if self._guard is not None:
            self._guard.check_progress()
        try:
            scanner = ManagedStream(
                batches,
                schema=schema,
                progress=self._guard.check_progress if self._guard is not None else None,
            )
        except BaseException:
            # ManagedStream closes its inputs when iterator construction fails.
            self._resources = ()
            raise
        self._resources = (scanner,)
        transformed = transform_factory(scanner)
        self._resources = (transformed, scanner)
        self._iterator = iter(transformed)
        self._resources = (self._iterator, transformed, scanner)

    def __next__(self) -> pa.RecordBatch:
        if self._closed:
            raise StopIteration
        try:
            if self._iterator is None:
                self._initialize()
            assert self._iterator is not None
            batch = next(self._iterator)
            if self._guard is not None:
                self._guard.check_disclosure()
            return batch
        except StopIteration:
            self.close()
            raise
        except BaseException:
            self.close(suppress_errors=True)
            raise

    def close(self, *, suppress_errors: bool = False) -> None:
        if self._closed:
            return
        self._closed = True
        resources, self._resources = self._resources, ()
        self._iterator = None
        self._source_factory = None
        self._transform_factory = None
        self._guard = None
        try:
            _close_resources(resources, suppress_errors=suppress_errors)
        finally:
            if self._admitted:
                self._admitted = False
                self._supervisor._release()
