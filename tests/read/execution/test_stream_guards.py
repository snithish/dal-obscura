from typing import Any, cast

import pyarrow as pa
import pytest


def test_stream_expiry_guard_rejects_ticket_expiry_before_identity_expiry():
    from tests.support.read.fakes import expiry_stream

    batch = pa.record_batch([pa.array([1])], names=["id"])
    guarded = expiry_stream(
        [batch],
        ticket_expires_at=1000,
        identity_expires_at=2000,
        stream_deadline_at=2000,
        now=lambda: 1000,
    )

    with pytest.raises(PermissionError, match="Ticket expired"):
        next(guarded)


def test_stream_deadline_guard_stops_before_emitting_a_late_batch():
    from tests.support.read.fakes import expiry_stream

    batch = pa.record_batch([pa.array([1])], names=["id"])
    guarded = expiry_stream(
        [batch],
        ticket_expires_at=1000,
        identity_expires_at=None,
        stream_deadline_at=100,
        now=lambda: 100,
    )

    with pytest.raises(TimeoutError, match="Stream deadline exceeded"):
        next(guarded)


def test_stream_guard_closes_upstream_when_consumer_stops_early() -> None:
    from tests.support.read.fakes import expiry_stream

    class ClosableBatches:
        def __init__(self) -> None:
            self.closed = False

        def __iter__(self):
            yield pa.record_batch([pa.array([1])], names=["id"])
            yield pa.record_batch([pa.array([2])], names=["id"])

        def close(self) -> None:
            self.closed = True

    upstream = ClosableBatches()
    guarded = expiry_stream(
        upstream,
        ticket_expires_at=9999,
        identity_expires_at=None,
        stream_deadline_at=9999,
        now=lambda: 1,
    )
    next(guarded)
    cast(Any, guarded).close()

    assert upstream.closed is True
