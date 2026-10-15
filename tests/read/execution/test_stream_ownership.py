import pyarrow as pa
import pytest

from dal_obscura.application.reads.execution import ManagedStream


class Source:
    def __init__(self, *, close_failure=False):
        self.close_calls = 0
        self.close_failure = close_failure

    def __iter__(self):
        yield pa.record_batch([pa.array([1])], names=["id"])

    def close(self):
        self.close_calls += 1
        if self.close_failure:
            raise RuntimeError("close failed")


def test_unstarted_stream_closes_all_owned_resources_once():
    source = Source()
    scanner = ManagedStream(source)
    transformed = (batch for batch in scanner)
    output = ManagedStream(transformed, resources=(scanner,))

    output.close()
    output.close()

    assert source.close_calls == 1
    assert list(output) == []


def test_guard_failure_is_preserved_when_cleanup_also_fails():
    source = Source(close_failure=True)

    def revoked():
        raise PermissionError("revoked")

    output = ManagedStream(source, guard=revoked)
    with pytest.raises(PermissionError, match="revoked"):
        next(output)

    output.close()
    assert source.close_calls == 1


def test_stream_construction_failure_closes_the_original_source():
    class BrokenSource(Source):
        def __iter__(self):
            raise ValueError("cannot iterate")

    source = BrokenSource(close_failure=True)
    with pytest.raises(ValueError, match="cannot iterate"):
        ManagedStream(source)
    assert source.close_calls == 1


def test_supervisor_is_lazy_and_admits_before_opening_another_source():
    from dal_obscura.application.reads.execution import ExecutionSupervisor, StreamAdmissionError

    supervisor = ExecutionSupervisor(max_active_streams=1)
    opened = []
    schema = pa.schema([("id", pa.int64())])

    def source():
        value = Source()
        opened.append(value)
        return schema, value

    never_started = supervisor.open(source, lambda batches: batches, None)
    never_started.close()
    never_started.close()
    assert opened == []
    first = supervisor.open(source, lambda batches: batches, None)
    assert opened == []
    assert next(first).to_pylist() == [{"id": 1}]
    second = supervisor.open(source, lambda batches: batches, None)
    with pytest.raises(StreamAdmissionError, match="capacity"):
        next(second)
    assert len(opened) == 1
    first.close()
    first.close()
    assert opened[0].close_calls == 1
    third = supervisor.open(source, lambda batches: batches, None)
    assert next(third).to_pylist() == [{"id": 1}]
    third.close()
    assert [value.close_calls for value in opened] == [1, 1]


@pytest.mark.parametrize(
    "failure", ["source", "source-iterator", "transform", "transform-iterator"]
)
def test_initialization_failure_preserves_error_and_releases_capacity(failure):
    from dal_obscura.application.reads.execution import ExecutionSupervisor

    supervisor = ExecutionSupervisor(max_active_streams=1)
    schema = pa.schema([("id", pa.int64())])
    source = Source(close_failure=True)
    closed_transform = []

    class BrokenSource(Source):
        def __iter__(self):
            raise ValueError("primary initialization failure")

    if failure == "source-iterator":
        source = BrokenSource(close_failure=True)

    def open_source():
        if failure == "source":
            raise ValueError("primary initialization failure")
        return schema, source

    class BrokenTransform:
        def __iter__(self):
            raise ValueError("primary initialization failure")

        def close(self):
            closed_transform.append(True)
            raise RuntimeError("transform cleanup failed")

    def transform(batches):
        if failure == "transform":
            raise ValueError("primary initialization failure")
        if failure == "transform-iterator":
            return BrokenTransform()
        return batches

    stream = supervisor.open(open_source, transform, None)
    with pytest.raises(ValueError, match="primary initialization failure"):
        next(stream)
    stream.close()
    assert source.close_calls == (0 if failure == "source" else 1)
    assert closed_transform == ([True] if failure == "transform-iterator" else [])
    replacement = supervisor.open(lambda: (schema, Source()), lambda batches: batches, None)
    assert [batch.to_pylist() for batch in replacement] == [[{"id": 1}]]


def test_cancellation_of_zero_output_scan_stops_at_next_input_checkpoint():
    from dal_obscura.application.reads.execution import (
        ExecutionSupervisor,
        StreamCancelledError,
        StreamGuard,
    )

    schema = pa.schema([("id", pa.int64())])
    cancelled = [False]
    counts = {"pulls": 0, "closed": 0, "sql": 0}

    def source():
        try:
            for value in range(4):
                counts["pulls"] += 1
                if counts["pulls"] == 2:
                    cancelled[0] = True
                yield pa.record_batch([[value]], schema=schema)
        finally:
            counts["closed"] += 1

    def check_revocation():
        counts["sql"] += 1

    guard = StreamGuard(100, None, 100, lambda: 0, check_revocation, lambda: cancelled[0])
    stream = ExecutionSupervisor().open(
        lambda: (schema, source()),
        lambda batches: (batch for batch in batches if batch.num_rows == 0),
        guard,
    )
    with pytest.raises(StreamCancelledError, match="cancelled"):
        list(stream)
    assert counts == {"pulls": 2, "closed": 1, "sql": 0}


def test_supervisor_checks_revocation_once_per_output_and_preserves_primary_error():
    from dal_obscura.application.reads.execution import ExecutionSupervisor, StreamGuard

    schema = pa.schema([("id", pa.int64())])
    checks = []

    def check_revocation():
        checks.append("sql")

    guard = StreamGuard(100, None, 100, lambda: 0, check_revocation)
    supervisor = ExecutionSupervisor(max_active_streams=1)
    stream = supervisor.open(lambda: (schema, Source()), lambda batches: batches, guard)
    assert [batch.to_pylist() for batch in stream] == [[{"id": 1}]]
    assert checks == ["sql"]
    source = Source(close_failure=True)

    def revoked():
        raise PermissionError("revoked")

    denied = supervisor.open(
        lambda: (schema, source),
        lambda batches: batches,
        StreamGuard(100, None, 100, lambda: 0, revoked),
    )
    with pytest.raises(PermissionError, match="revoked"):
        next(denied)
    assert source.close_calls == 1
    assert [
        batch.to_pylist()
        for batch in supervisor.open(lambda: (schema, Source()), lambda batches: batches, guard)
    ] == [[{"id": 1}]]


@pytest.mark.parametrize("mismatch", ["type", "metadata"])
def test_supervisor_checks_declared_execution_schema_before_disclosure(mismatch):
    from dal_obscura.application.reads.execution import ExecutionSupervisor, StreamGuard

    declared = pa.schema([pa.field("id", pa.int64(), metadata={b"field_id": b"1"})])
    actual = pa.schema(
        [
            pa.field(
                "id",
                pa.int32() if mismatch == "type" else pa.int64(),
                metadata={b"field_id": b"2" if mismatch == "metadata" else b"1"},
            )
        ]
    )
    checks = []
    guard = StreamGuard(100, None, 100, lambda: 0, lambda: checks.append("sql"))
    stream = ExecutionSupervisor().open(
        lambda: (declared, [pa.record_batch([[1]], schema=actual)]), lambda batches: batches, guard
    )
    with pytest.raises(ValueError, match="declared execution schema"):
        list(stream)
    assert checks == []
