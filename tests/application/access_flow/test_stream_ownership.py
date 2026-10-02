import pyarrow as pa
import pytest

from dal_obscura.read.stream import ManagedStream


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
