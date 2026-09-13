# X21 capacity evidence runbook

This directory is reserved for measured capacity artifacts. The acceptance
thresholds in `docs/plugin-platform/ACCEPTANCE.md` are gates, not claims about
the current workspace. Store only benchmark JSON and redacted metadata here;
never store tokens, customer rows, or provider credentials.

## Reference workload

Use a warmed 4-vCPU/8-GiB runner with the pinned `uv.lock` and the synthetic
datasets already owned by `tests/benchmarks/`. The qualified workload includes
10,000 schema nodes, a 10,000-entry discovery traversal, and a 10-million-row
multi-file read. The existing streaming benchmark also exercises a larger
25-million-row run to catch accidental result materialization; it does not
replace the required 10-million-row measurement.

The process-local benchmark suites are repeatable with:

```sh
DAL_OBSCURA_CAPACITY_RUNS=5 ./scripts/run_capacity_benchmarks.sh \
  evaluation/capacity/<commit>-<runner>-<timestamp>
```

The script refuses to overwrite an evidence directory and records the commit,
working-tree state, platform, Python version, lockfile digest, and run count.
Each suite writes a separate `pytest-benchmark` JSON file. Run the suites on the
same runner and commit for comparison; do not mix warm and cold runs.

## Required measurements

Record absolute values and the comparison to the preserved qualified Iceberg
baseline:

- schema/discovery latency and whether the configured 10-second deadline was
  met;
- synthetic evaluation latency and whether the 5-second deadline was met;
- throughput and p95 first-batch latency over five comparable runs, with no more
  than a 15% regression;
- Arrow/Flight chunk count, first-chunk latency, total duration, and peak RSS;
- rejected versus admitted operations under bounded concurrency;
- for the separate 60-minute mixed workload at 16 consumers: total RSS/native
  memory, open handles, pending operations, cancellation cleanup time, and
  injected alert results.

The 60-minute workload must run against the secure local or production topology
with the same authentication, authorization, plugin lock, and resource limits.
It cannot be substituted with a unit test or a short smoke run. Record the
hardware, process count, client/consumer versions, dataset identity, and exact
command. Report skipped or unavailable cells with the required environment;
never infer cross-worker capacity from process-local metrics.

## Review checklist

1. Verify the candidate commit is clean and the lockfile digest matches the
   baseline being compared.
2. Warm the service and clients before collecting the five benchmark runs.
3. Inspect p50/p95 and absolute throughput, not only the fastest run.
4. Compare first and last ten-minute median RSS, open handles, and pending
   operations for the mixed workload; RSS growth must be at most 10%.
5. Inject a cancellation and an alert failure, then verify reservations and
   upstream iterators are released by the deadline plus two seconds.
6. Link the artifact directory and exact test output in `docs/plugin-platform/STATUS.md`.
   Keep X21 `implemented-unverified` until every A21 cell has executed on the
   required runner.

