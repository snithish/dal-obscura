"""Cancellation belongs to the public execution boundary, including lazy work."""

from datetime import UTC, datetime, timedelta

import pytest
from dal_obscura_plugin_api import ExecutionContext
from dal_obscura_plugin_api.execution import active, checked_iterator, close_preserving_error


def context(cancelled):
    return ExecutionContext(datetime.now(UTC) + timedelta(minutes=1), "test", cancelled)


def test_guard_rejects_result_after_cancellation():
    cancelled = False

    @active
    def read(context):
        nonlocal cancelled
        cancelled = True
        return "must not escape"

    with pytest.raises(InterruptedError):
        read(context(lambda: cancelled))


def test_iterator_closes_when_cancelled_between_batches():
    cancelled = False
    closed = []

    def batches():
        try:
            yield 1
            yield 2
        finally:
            closed.append(True)

    stream = checked_iterator(batches(), context(lambda: cancelled))
    assert next(stream) == 1
    cancelled = True
    with pytest.raises(InterruptedError):
        next(stream)
    assert closed == [True]


def test_iterator_closes_on_early_stop():
    closed = []

    def batches():
        try:
            yield 1
            yield 2
        finally:
            closed.append(True)

    stream = checked_iterator(batches(), context(lambda: False))
    assert next(stream) == 1
    stream.close()
    assert closed == [True]


def test_generator_guard_checks_each_pull_and_closes_provider():
    cancelled = False
    closed = []

    @active
    def read(context):
        try:
            yield 1
            yield 2
        finally:
            closed.append(True)

    stream = read(context(lambda: cancelled))
    assert next(stream) == 1
    cancelled = True
    with pytest.raises(InterruptedError):
        next(stream)
    assert closed == [True]


def test_cleanup_does_not_mask_primary_error() -> None:
    class FailingClose:
        def close(self) -> None:
            raise RuntimeError("cleanup failed")

    with pytest.raises(ValueError, match="primary failure"):
        try:
            raise ValueError("primary failure")
        except ValueError:
            close_preserving_error(FailingClose())
            raise
