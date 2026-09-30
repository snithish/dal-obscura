# Capacity benchmarks

Store benchmark JSON and redacted environment metadata here. Do not store tokens,
customer rows, or provider credentials. Process-local results do not establish
cross-worker capacity or a production throughput guarantee.

## Run repeatable measurements

Use the pinned `uv.lock` and synthetic datasets in `tests/benchmarks/`:

```sh
DAL_OBSCURA_CAPACITY_RUNS=5 ./scripts/run_capacity_benchmarks.sh \
  evaluation/capacity/<commit>-<runner>-<timestamp>
```

The script refuses to overwrite an evidence directory and records the commit,
working-tree state, platform, Python version, lockfile digest, and run count.
Each suite writes a separate `pytest-benchmark` JSON file. Compare on the same
runner with the same workload and warmup; separate warm and cold runs.

## Interpret results

Record hardware, process count, consumer versions, dataset size and shape, resource
limits, and exact commands. Inspect throughput, first-batch latency, total duration,
and peak RSS alongside output correctness. Explain skipped or unavailable scenarios.
Use deployment-specific capacity objectives rather than treating local benchmarks
as an SLA.

For concurrency and soak measurements, run the actual authenticated deployment
with representative file skew, delete density, nested rows, and clients. Monitor
RSS, open handles, pending operations, rejection rates, and recovery after overload.
Exercise cancellation and verify upstream iterators and reservations are released.
Preserve commands, results, and artifact identities together in the output directory.

See [development](../../docs/development.md) for individual benchmark commands and
[read execution invariants](../../docs/read-execution-invariants.md) for resource limits.
