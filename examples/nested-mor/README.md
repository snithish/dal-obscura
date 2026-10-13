# Nested S3 merge-on-read scenarios

Run two full-scale, independent examples through the production governance HTTP
API and Arrow Flight servers. The [offline HTML report](../../docs/scenarios/nested-mor.html)
contains the complete code flow, source excerpts, actual timings, batch telemetry,
task assignment, correctness checks and partition/projection IO comparisons.

## Run

Requirements: Docker Engine + Compose v2, or a compatible Podman/Docker CLI with
Compose; roughly 6–8 GiB VM memory and 20 GiB free disk. No host ports are published.
The first build compiles the pinned Apache Iceberg Rust binding and downloads
locked Python dependencies. Build time is separate from read time. The emulator explicitly configures both
catalog S3 endpoint and region; official region auto-discovery otherwise contacts
AWS rather than Moto.

```bash
cd examples/nested-mor
./run delta
SCENARIO=delta docker compose -p obscura-nested-delta down --volumes
./run iceberg
SCENARIO=iceberg docker compose -p obscura-nested-iceberg down --volumes
```

Each runner recreates only its named disposable project, builds the source image,
waits for seeding/provisioning and runs verification without rerunning completed
dependency jobs. Results remain in ignored `results/delta.json` and
`results/iceberg.json`. The stack uses ephemeral Moto S3 storage; removing its
container deletes its objects. PostgreSQL/state volumes are removed by cleanup.

## VS Code debugging

Install the Microsoft Python extension and Docker Compose. In Run and Debug, select
**Nested MoR: Full distributed flow**. VS Code asks for `delta` or `iceberg`, starts
a fresh scenario, and waits until the verifier is listening. The compound attaches
to the planner, all three workers and the verifier; set breakpoints in `src/` and
`examples/nested-mor/scripts/verify.py` before starting. `justMyCode` is disabled,
so stepping can enter plugin and dependency code. The verifier waits for VS Code
before planning begins. Worker debug servers start with the services and accept
the compound's debugger connections.

The debugger publishes ports 5678–5682 on loopback only. Use **Nested MoR: Stop
debug stack** from Tasks when finished; it removes only the selected scenario's
Compose project and volumes. Equivalent terminal entry point is
`./debug delta` or `./debug iceberg`; attach the same compound when its ready
message appears. The ordinary `./run` flow remains unchanged.

From the repository root, embed completed evidence into the report:

```bash
uv run --no-project examples/nested-mor/render.py
```

The renderer requires both full-size results, all checks passing and completed
optimization probes. It captures source/result SHA-256 hashes and source excerpts.
The HTML embeds full evidence and supports JSON downloads without a server. Its
relative source links work when served from the repository root. Serve it locally
with `uv run --no-project python -m http.server 8879 --bind 127.0.0.1` if desired.

## Data and correctness

Both catalogs contain **101 physical tables**: `retail.customers` plus 100 small
unrelated tables. The target has **10 partitions × 1,000,002 physical base rows**,
and at least **1,000,000 live rows in every partition** after MoR mutations.
The source has nested structs, lists of structs, nested participant lists, maps
of structs and a separate map with PII keys. Seven masks protect emails/phones
and hide the PII-key map. Null containers, empty containers and null elements
repeat throughout the full fixture.

- Delta: a UUID-backed deletion vector removes two physical positions, then a
  delta-rs append inserts a replacement. The original data-file hash stays unchanged.
- Iceberg v2: a position delete removes ID 2; an equality delete removes old ID 1;
  a newer sequence inserts replacement ID 1. Original data files remain active.
- A post-plan append is excluded by old tickets and included by new plans.
- Pinned and US-filtered reads compare every nested value against an independent
  scalar oracle. Every phase checks exact IDs, schema metadata and batch sizes.
- Three independent worker containers fetch signed tickets concurrently. Passive
  task units must be disjoint; an instrumented S3 guard denies unrelated data GETs
  and GETs of files outside each worker's ticket. Metadata/deletes can be shared.
- A caller filter on masked PII must fail authorization.

Fixture writers use the existing owning delete helpers; they are not production
protocol readers. Apache Iceberg Rust and Delta Kernel interpret native deletes.
PyIceberg's public `add_files` commits Arrow-written Parquet to preserve null
collections affected by the PyIceberg 0.11.1 writer bug. The production read bridge
also preserves validity during upstream field-ID evolution.

## Performance and optimization evidence

Five phases record plan, earliest batch and parallel-fetch wall time:

1. Cold pinned read with every nested scalar checked.
2. Warm full read with IDs/schema checked.
3. `region = 'US'` read with every returned scalar checked.
4. `partition = '3'` read, checking exact IDs/partition coverage.
5. `id`-only projection, checking exact IDs/schema.

Every output batch records rows, logical bytes and retained bytes. S3 middleware
records worker, method, path, query, range, status and response Content-Length.
Successful data GET bytes include repeated ranges/footers, not unique stored bytes.
The example checks output pruning separately from physical IO pruning.

**Partition predicate pushdown is currently absent in both format plugins.**
Neither declares `filter_pushdown`; core retains the full predicate and DuckDB
evaluates it after native reading. The partition probe must return only partition
3, but still plans/scans all partitions. Delta projects Arrow columns; Iceberg
reads all native fields before selecting output, retaining hidden delete keys.
No statistic-based row-group predicate pruning is claimed. The report records
these gaps rather than treating smaller output as pushdown evidence.

Defaults: 16,384-row Parquet groups; 8,192-row native/output batches; one DuckDB
thread, 256 MB DuckDB memory per stream, two active streams per worker; a fixed
16 MiB plugin logical/retained batch cap; 64 MiB transform input/output batch caps;
one-hour ticket and stream deadlines, one exchange per ticket. The read deadline
also bounds plugin execution; the separate ten-second planning-operation deadline
does not prematurely terminate long streams. Arrow/native allocations are not all
charged to DuckDB. These settings do not establish a global RSS or capacity SLA.

Client verification/backpressure is included in fetch time. Appending after
planning, evidence introspection and controller setup are excluded. Single warm
samples are observations, not statistical benchmarks or distributed-host speedups.
The Compose workers share one VM; Moto and the synthetic JWT issuer do not qualify
AWS throughput, IAM/TLS or live SSO.

Configuration:

```bash
SCENARIO_ROUNDS=3 ./run delta
# Development probe only; never qualifies the requested scale:
SCENARIO_SMOKE=1 SCENARIO_ROWS_PER_PARTITION=128 ./run iceberg
```

Delta's maintained API materializes snapshot-wide deletion masks. Its current
16-million physical-row cap includes replacement/later appends; larger examples
are rejected explicitly. The shared example defaults fit that bound. Partition
count must be at least ten. Increasing rows/partition must respect that cap.
Only the target has millions of rows; the 100 unrelated tables are real small
catalog distractors, not a separate large workload.
