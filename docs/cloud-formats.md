# Catalog-independent COW and MoR execution

Delta and Iceberg readers bind immutable snapshot handles and produce independent,
passive JSON tasks. Catalog lookup, storage transport, and table semantics remain
separate. Every worker executes only its assigned data work; shared metadata and
delete reads are legitimate. COW versus MoR is inferred from the captured snapshot,
not selected by a consumer flag.

## Official catalogs and storage

`iceberg.catalog` delegates catalog selection to PyIceberg `load_catalog`: SQL,
REST, Hive, Glue, DynamoDB, BigQuery, and in-memory catalogs. The old `iceberg.sql`
plugin ID and module have been removed. Install backend dependencies with
`uv sync --dev --extra server --extra postgres --extra cloud`. Provider configuration
uses PyIceberg properties; admitted external catalog plugins can emit the same
Iceberg metadata-location handle. No catalog-specific branch exists in reading.
Catalog implementations loaded by arbitrary Python import strings are not admitted.

Delta supports `delta.directory` for a bounded storage registry and `delta.catalog`
for registered table locations, including native `uc://catalog.schema.table` Unity
lookup. The official delta-rs client owns Unity lookup and credentials. Its public
Python API does not expose catalog-wide listing: the registered identifiers define
the governed scope. Other admitted catalog plugins can resolve Delta locations to
the same location/version/table-ID handle. No custom Unity HTTP implementation is
introduced.

Delta delegates log, checkpoint, DV, and Parquet transport to the official
`DeltaStorageHandler` and `DeltaTable` object-store clients. Their supported local,
S3, Azure, GCS and other backends are no longer restricted by an Arrow S3-only
helper. Root containment, URI admission and local symlink rejection remain in the
adapter. Worker startup `DAL_OBSCURA_DELTA_STORAGE_OPTIONS` is a JSON string mapping
of native delta-rs options; standard native credential/environment chains still apply.

Iceberg uses one official PyIceberg FileIO for metadata, manifests, data, and
deletes. The native Apache Iceberg reader receives read-only worker-local FileIO
callbacks instead of a separate OpenDAL filesystem. Thus every location supported
by the configured official PyArrow/Fsspec FileIO can be consumed by the native
reader, including S3, GCS, Azure, HDFS and additional installed Fsspec backends.
Set `DAL_OBSCURA_ICEBERG_IO_PROPERTIES` to a JSON string mapping of official
PyIceberg FileIO properties. Only the official PyArrow/Fsspec implementation names
are accepted. Metadata properties cannot override worker transport or load Python
implementations. AWS region/endpoint startup variables remain supported.

Credentials, endpoints and live providers stay on each worker. Handles/tickets
contain locations and snapshot/task identity only. Catalog-vended credentials are
not serialized: workers must have their own access to every referenced storage
location. This includes cross-location manifests/delete files admitted by path
rules. Catalog/file-system support requires the official client's dependencies,
credentials and provider configuration; it does not mean arbitrary URIs or table
protocol features are supported.

## Native semantics and distributed work

- **Delta COW:** the public delta-rs Arrow dataset owns partition expressions,
  statistics and schema handling. Its Parquet fragments are narrowed to assigned
  row groups. The active-file set comes from the captured native snapshot.
- **Delta MoR:** the public Arrow dataset rejects deletion vectors. Delta Kernel's
  public `deletion_vectors()` decodes exact keep masks; Arrow reads assigned row
  groups, then applies masks by physical row offset before governance SQL. Inline,
  UUID-relative and contained absolute DV files are covered. Newer replacement
  rows are ordinary active files. No protocol/delete decoder is implemented here.
- **Iceberg v1/v2:** Apache Iceberg Rust plans and reads assigned file/range work.
  It owns COW, position/equality deletes, partition applicability and sequence
  ordering. Manifest offsets allow disjoint row-group splitting when a few files
  must serve multiple tickets. Snapshot history restores required historical
  equality keys; upstream field-ID evolution runs after deletes. Renaming or
  reusing a column name does not reuse its old identity.

Tasks are deterministically balanced under their budget. Excess work is grouped,
empty scans produce no tickets, and every assigned unit is covered exactly once.
A later append/update/delete cannot change an issued snapshot. Neither whole-table
rescans on every worker nor DuckDB threads substitute for distributed tasks.
Complete SQL filters and masks remain in DuckDB after native delete application.

## Necessary bridges and costs

The official Python Iceberg binding has no independently executable file-task
reader. The small PyO3 binding exposes Apache's native planner/Arrow reader.
Its read-only FileIO bridge adapts official Python streams to Rust byte ranges;
it does not parse manifests, interpret snapshots, or match deletes. Each range
opens/closes a seekable provider stream and copies its bytes across the Python/Rust
boundary. Blocking provider work runs outside the async IO thread. This broad
transport compatibility has additional stream-open/IO/copy cost; it is not a
zero-copy claim. All callbacks check the operation context and path admission.
Provider calls already in progress may finish after cancellation; late output is
rejected and their streams still close.

The native reader retains the upstream merged NULL/equality-delete fix, pinned
in Cargo. PyIceberg's collection-validity bridge and DuckDB's sliced-nested-Arrow
bridge remain documented in [execution invariants](read-execution-invariants.md).
No live callback or native task object enters production tickets.

Delta still rejects column mapping/unknown reader features explicitly. Its public
DV API materializes masks snapshot-wide, so the existing 16-million physical-row
bound remains. Iceberg v3 is outside the qualified v1/v2 reader contract. File/task,
stream and retained-batch budgets apply; native metadata/delete buffers are
upstream-owned and have no hard process/cluster RSS guarantee. Neither format
currently advertises SQL filter pushdown. Unsupported protocol/provider behavior
fails explicitly rather than returning partially interpreted data.

## Evidence and reproduction

Full Python qualification passed 1,076 tests with disposable PostgreSQL, every
real consumer backend enabled and zero skips. The format/S3 lane checks COW and
MoR in fresh worker processes with assigned-file access guards and exact expected
rows. It covers row-group splitting, replacement sequences, hidden delete keys,
nested null/empty values and pinned snapshots. SQL/in-memory catalog tests and the
real Glue API/S3 emulator lane exercise official catalog selection independently
of the format. Fresh installed-wheel qualification disables repository production
import paths.

The [Compose example](../examples/nested-mor/README.md) uses a catalog of 101 tables,
10 partitions and 10,000,020 physical base rows. Three independent Flight worker
containers share object storage and consume separate tickets. The
[HTML report](scenarios/nested-mor.html) contains reproducible code flow, exhaustive
nested correctness checks, batch telemetry and actual partition/projection IO
comparisons. Emulator evidence does not establish live AWS/GCP/Azure IAM renewal,
TLS/service quotas or multi-host throughput; those remain deployment qualification.

References: [delta-rs API](https://delta-io.github.io/delta-rs/latest/api/delta_table/),
[PyIceberg configuration](https://py.iceberg.apache.org/configuration/),
[Apache Iceberg Rust](https://github.com/apache/iceberg-rust).
