"""Explicit ownership for lazy streams, including streams never advanced."""

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

import pyarrow as pa


class ManagedStream(Iterator[pa.RecordBatch]):
    def __init__(
        self,
        batches: Iterable[pa.RecordBatch],
        *,
        guard: Callable[[], None] | None = None,
        resources: tuple[object, ...] = (),
    ) -> None:
        self._closed = False
        self._guard = guard
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
            batch = next(self._iterator)
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
    """Check every output after transformation, immediately before disclosure."""

    ticket_expires_at: int
    identity_expires_at: int | None
    stream_deadline_at: int
    now: Callable[[], int]
    check_revocation: Callable[[], None]

    def __call__(self) -> None:
        current_time = self.now()
        if current_time >= self.stream_deadline_at:
            raise TimeoutError("Stream deadline exceeded")
        if current_time >= self.ticket_expires_at:
            raise PermissionError("Ticket expired")
        if self.identity_expires_at is not None and current_time >= self.identity_expires_at:
            raise PermissionError("Identity expired")
        self.check_revocation()
