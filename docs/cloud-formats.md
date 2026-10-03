# Cloud format execution

All bundled formats can read local storage and AWS S3. Catalog discovery may use
SQL, REST Iceberg, an operator manifest, or an operator Delta registry. Every
format binds an immutable snapshot handle and emits only passive JSON tasks.
Other clouds are outside this release; no generic object-store abstraction or
second protocol implementation was added.

## Storage and credentials

Arrow S3FileSystem, delta-rs and DuckDB own storage IO and their native AWS
credential chains. Workers supply workload identity or standard AWS environment
credentials. No storage credential, endpoint override, callback or live provider
is captured in a handle/ticket. Operator startup AWS_REGION/AWS_DEFAULT_REGION
and AWS_ENDPOINT_URL_S3 (AWS_ENDPOINT_URL fallback) configure regional/private
test access. Workers must have equivalent read permissions, including metadata,
checkpoint and delete objects. S3 IAM/STS renewal on an actual AWS account requires
live deployment qualification; the S3 emulator lane does not establish it.

The SDK StorageRoot helper contains only root/path admission and bounded metadata
reads; maintained Arrow filesystems own transport. Local roots reject symlinks.
S3 members reject root escape, traversal, credential-bearing URIs and query/fragment
components. Delta additionally admits log/checkpoint/DV paths before kernel IO.
Iceberg applies configured allowlists to metadata, manifests, data and deletes.

## Native semantics and parallelism

- **Delta:** delta-rs 1.5.0/Delta Kernel owns snapshot replay, checkpoints, protocol
  checks and DV decoding. Arrow reads grouped Parquet row groups; kernel masks are
  sliced by physical row offset and applied before governance SQL. This bridge is
  necessary because the public Arrow dataset reader rejects DV-enabled tables
  and the public kernel scan has no independent row-group task API. Both COW and
  MoR replacements retain exactly one copy of surviving rows. Tasks pin masks and
  versions; later append/delete does not change issued work.
- **Iceberg v2:** PyIceberg owns metadata/manifest discovery. DuckDB 1.5.4's native
  Iceberg extension owns COW, position deletes and equality deletes. PyIceberg's
  public reader rejects equality deletes, so it cannot satisfy this contract.
  Native extensions are pinned wheel artifacts; runtime downloading is disabled.
  COW/position scans use deterministic disjoint file groups (native filename
  predicate), one native thread per task. Equality-delete scans use one task and
  up to four native threads: the native filename column currently fails with
  equality deletes. Using it would corrupt/fail reads; rerunning the entire table
  in multiple tasks would duplicate work. This backend limit prevents distributed
  equality-delete fan-out in this release.
- **Manifest/Parquet:** Arrow owns immutable file reads; row groups are balanced
  across disjoint tasks. Replacement files require a new manifest revision.
  It is not a transactional table format and has no independent MoR semantics.

Iceberg native reads retain all table fields before Arrow projection so equality
keys remain available even when omitted from caller output. This costs extra IO;
it avoids the upstream equality-delete projection defect. Native filter hints
are validated SQL and advisory; core always enforces the complete governance
filter on original values before masking. No custom SQL-to-Iceberg expression
translator, delete matcher or serialized DataFile codec remains.

## Bounds and evidence

Each native Iceberg connection uses at most four threads, 256 MiB engine memory,
no disk spill and 8,192-row streaming batches. Planning admits at most 10,000
manifest entries; Delta and Parquet have additional documented task/metadata
limits. Provider metadata APIs may materialize listings/manifests; these caps do
not establish a hard planning RSS limit. Native blocking IO is not immediately
interruptible; active checks reject late batches and cleanup owns readers and
connections. Per-worker limits do not establish a deployment-wide capacity SLA.

The socket lane `tests/plugin_platform/test_cloud_formats.py` runs real Arrow,
Delta Kernel, PyIceberg and native DuckDB against a disposable HTTP S3 emulator.
It covers COW/MoR deletes and replacements, restored parallel tasks, snapshot
pinning and object names with escaped characters. Local owning lanes cover schema
metadata, projection, cancellation, cleanup and path admission. No production
AWS benchmark or speedup claim is made.

References: [delta-rs S3](https://delta-io.github.io/delta-rs/latest/integrations/object-storage/s3/),
[PyIceberg configuration](https://py.iceberg.apache.org/configuration/),
[DuckDB Iceberg](https://duckdb.org/docs/current/core_extensions/iceberg/overview),
[upstream equality projection issue](https://github.com/duckdb/duckdb-iceberg/issues/940).
