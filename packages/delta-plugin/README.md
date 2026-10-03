# Delta Lake plugin

`dal-obscura-delta` 0.2.0 implements SDK API 2 with catalog `delta.directory` and
format `delta`. The package imports the public SDK, delta-rs and Arrow; it does not
import service internals. It is an optional, independently admitted wheel.

## Configuration

Install the qualified wheel on every control/data-plane worker. Admit both entry
points through a lock generated from those exact installed artifacts:

```bash
uv build --wheel --out-dir dist/plugins-0.2.0 packages/delta-plugin
uv pip install dist/plugins-0.2.0/dal_obscura_delta-0.2.0-py3-none-any.whl
uv run --no-sync python scripts/build_plugin_lock.py \
  --output /tmp/delta-plugin-lock.json \
  --plugin catalog:delta.directory --plugin table_format:delta
```

Merge these selections into the deployment's generated lock, using the same
builder for all admitted entry points. Configure a catalog with plugin ID
`delta.directory` and options:

```json
{"root": "/srv/data", "tables_path": "/srv/data/delta-tables.json"}
```

The registry file contains a JSON array. Namespace segments are explicit, so a
literal dotted segment differs from multiple segments:

```json
[
  {"namespace": ["retail"], "name": "sales", "path": "sales"},
  {"namespace": ["retail.eu"], "name": "orders", "path": "eu/orders"}
]
```

Paths must remain inside the configured root; each Delta table's log, data,
checkpoint and deletion-vector files must remain inside that table directory.
Local symlinks and shallow clones referencing files outside the table root are
rejected. Roots and registries may use local paths, file URIs, or AWS S3 URIs.
For S3, use `{"root":"s3://bucket/data","tables_path":"s3://bucket/data/tables.json"}`.
Storage credentials come from the worker AWS provider chain, never configuration
handles or tickets. Membership is captured when the immutable catalog configuration opens;
publish a new catalog revision after changing the registry. Table resolution
captures the current Delta table UUID and version, without refreshing old handles.

## Parallel reads and deletes

The public Arrow dataset API cannot read deletion-vector tables or independently
split their scans by row group. The adapter therefore combines Arrow row-group
readers with masks already decoded by Delta Kernel; this small bridge is needed
for independently executable parallel tickets.

Delta-rs 1.5.0 owns transaction-log replay, checkpoint handling, snapshot
reconciliation, protocol validation and deletion-vector decoding through Delta
Kernel. Production code contains no Roaring or Z85 implementation and does not
reconstruct snapshots itself. The plugin has a separate, bounded path-admission
guard that inspects log/checkpoint locations before provider IO. This is required
because the public reader can follow absolute deletion-vector paths outside its
table root; it is not a replacement transaction-log reader.

Planning opens the captured version, obtains its active files and exact Arrow
schema, and splits Parquet row groups. Largest estimated row-group byte costs go
to the least-loaded task, with deterministic ties. Excess groups share tasks;
empty scans and fully deleted groups produce no work. Readers consume endpoints
concurrently to realize parallelism; task order does not guarantee row order.

Copy-on-write deletes/updates use only surviving files from the pinned snapshot.
For merge-on-read, the kernel's selection vectors are sliced by physical file-row
offset, including across row groups, and captured as base64 Arrow IPC boolean
arrays in passive JSON tasks. Inline, UUID-relative and contained absolute DV
files are supported. Fetch applies the captured mask before governance SQL
filters and column masking, so deleted old values cannot reappear after an update.
Replacement rows are scanned as ordinary active files. Tasks restore in fresh
plugin instances without consulting the latest table version or reopening DVs.

Partition values come from snapshot metadata, including NULL partitions; missing
new columns are filled by Arrow schema evolution. Projection preserves requested
field order, nested types and metadata. Streams use 8,192-row batches, disabled
read-ahead and cancellation/deadline checks around lazy work. Vacuuming files or
logs needed by an issued ticket causes an explicit failure; fetch never falls
forward to newer data. Configure Delta retention longer than ticket lifetime.

## Supported scope and budgets

- Local filesystem and AWS S3 tables. Other object stores and catalog-managed
  Delta tables are not supported. Arrow and delta-rs own storage IO.
- Reader protocol 1, or protocol 3 with `deletionVectors`, `timestampNtz` and
  `v2Checkpoint`. Column mapping and unknown reader features fail closed.
- Registry: at most 128 tables and 1 MiB. Snapshot: at most 10,000 active files
  and 10,000 nonempty scan row groups. Log admission: at most 10,000 entries,
  64 MiB on disk and 1 MiB per JSON action.
- The public Python DV API materializes all selection vectors for a snapshot.
  DV-enabled snapshots therefore require complete nonnegative physical row-count
  statistics and at most 16 million physical rows, checked before materialization.
  This conservative limit also applies when the DV feature is enabled but no
  current files have DVs. The adapter does not claim per-file DV memory use.
- SDK JSON string/task limits also apply to encoded row-group masks. There is no
  SQL filter pushdown; core DuckDB applies the complete filter to original values.
- Metadata admission and pinned Delta state are reopened in each worker instance.
  No distributed metadata cache, persistent coordinator or new database is added.
  Library calls cannot be interrupted mid-call; late output is prevented by the
  operation checks. Resource caps are not a deployment-wide RSS guarantee.

## Qualification

```bash
uv run --no-sync python -m pytest packages/delta-plugin/tests
DAL_OBSCURA_RUN_CONSUMER_TESTS=1 uv run --no-sync pytest tests/consumers
```

Installed-wheel qualification must disable repository source paths:
`python -m pytest -o pythonpath= packages/delta-plugin/tests`. Tests cover actual
Delta writes/deletes/updates, protocol-valid DV fixtures in all three storage
forms, merge-on-read replacement rows, parallel JSON-restored scans, checkpoints,
schema evolution, cancellation, vacuum failures, path admission and public
conformance. The real Flight consumer lane checks nested masking over DV-backed
files. Handwritten DV serialization is confined to test fixture generation.

References: [delta-rs API](https://delta-io.github.io/delta-rs/1.5.0/api/delta_table/)
and [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).
