"""Cancellation belongs to the public execution boundary, including lazy work."""

from datetime import UTC, datetime, timedelta

import pytest
from dal_obscura_plugin_api import ExecutionContext
from dal_obscura_plugin_api.execution import active, checked_iterator, close_all_preserving_error


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


@pytest.mark.parametrize("cleanup_error", [RuntimeError, KeyboardInterrupt])
@pytest.mark.parametrize("primary_error", [None, ValueError, GeneratorExit])
@pytest.mark.parametrize("lookup_failure", [False, True])
def test_cleanup_preserves_primary_error_and_exposes_cleanup_only_failure(
    cleanup_error, primary_error, lookup_failure
) -> None:
    closed = []

    class FailingClose:
        def __init__(self, name):
            self.name = name

        @property
        def close(self):
            if lookup_failure:
                self.fail()
            return self.fail

        def fail(self) -> None:
            closed.append(self.name)
            raise cleanup_error(f"{self.name} cleanup failed")

    resources = [FailingClose("first"), FailingClose("second")]
    with pytest.raises(
        primary_error or cleanup_error,
        match="primary failure" if primary_error else "first cleanup failed",
    ):
        if primary_error is None:
            close_all_preserving_error(resources)
        else:
            try:
                raise primary_error("primary failure")
            except BaseException:
                close_all_preserving_error(resources)
                raise
    assert closed == ["first", "second"]
