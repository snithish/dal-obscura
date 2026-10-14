"""Same-host schema identity latency and Python allocation probes."""

import gc
import tracemalloc

import pyarrow as pa
import pytest

from dal_obscura.control.assets.schema import schema_fingerprint

pytestmark = pytest.mark.heavy


def _schema_case(case: str) -> tuple[pa.Schema, str]:
    if case == "wide-4000":
        return (
            pa.schema([pa.field(f"field_{i}", pa.string()) for i in range(4000)]),
            "3382af5e8f19d1daf7d554a66bc7513212df41dd30db412336b399526fd0cab6",
        )
    if case == "nested-64":
        nested: pa.DataType = pa.string()
        for _ in range(63):
            nested = pa.struct([pa.field("child", nested)])
        return (
            pa.schema([pa.field("root", nested)]),
            "dbae5309ee4a597e97ff5c2118bd48d403413c0af635a43e53fdfe8b52b1a17d",
        )
    return (
        pa.schema([pa.field("value", pa.string())], metadata={b"key": b"x" * 1_200_000}),
        "7c28478d9815caad100378493d91d72473f0e077c06a51c82b1188127da77f82",
    )


@pytest.mark.parametrize("case", ["wide-4000", "nested-64", "metadata-1.2MB"])
@pytest.mark.benchmark(group="schema-identity", min_rounds=15, max_time=0.2)
def test_schema_identity_latency_and_python_allocation(case, benchmark):
    schema, expected = _schema_case(case)
    assert benchmark(schema_fingerprint, schema) == expected
    # Measure allocations separately: tracing would distort the timing lane.
    # This excludes fixture construction and native Arrow allocations; it is
    # not a worker RSS measurement or a hard memory guarantee.
    gc.collect()
    tracemalloc.start()
    try:
        assert schema_fingerprint(schema) == expected
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    benchmark.extra_info["python_peak_bytes"] = peak
    benchmark.extra_info["canonical_digest"] = expected
