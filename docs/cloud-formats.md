# Cloud format execution

All bundled formats can read local storage and AWS S3. Catalog discovery may use
SQL, REST Iceberg, an operator manifest, or an operator Delta registry. Every
format binds an immutable snapshot handle and emits only passive JSON tasks.
Other clouds are outside this release; no generic object-store abstraction or
second protocol implementation was added.

## Storage and credentials

Arrow S3FileSystem, delta-rs and Apache OpenDAL own storage IO and their native AWS
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
- **Iceberg v2:** PyIceberg owns admission/schema discovery. Apache Iceberg Rust
  0.10.0 with Apache's merged [NULL fix](https://github.com/apache/iceberg-rust/pull/2781)
  plans native delete-aware file tasks and owns COW, position/equality
  deletes, partition matching and sequence applicability. Its official Python
  binding has no file-task reader API; a small [native binding](../packages/iceberg-reader/README.md)
  exposes the maintained Rust planner/Arrow reader. Every mode uses deterministic,
  disjoint file groups. When file count is below task budget, manifest row-group
  offsets split the largest eligible range; absent offsets fall back to file granularity.
  Workers reconstruct native plans from pinned metadata,
  then execute only their assigned files/row groups; no whole-table data rescan or
  thread-only fallback remains. Metadata and delete files may be shared.
- **Manifest/Parquet:** Arrow owns immutable file reads; row groups are balanced
  across disjoint tasks. Replacement files require a new manifest revision.
  It is not a transactional table format and has no independent MoR semantics.

Iceberg reads retain snapshot fields and required historical equality keys before
field-ID projection to the captured metadata schema. This preserves dropped or
renamed delete keys, including a different column reusing the old name. Native
schema history/pruning and PyIceberg's projection visitor own type/default
behavior. Reading hidden fields costs IO. The format no longer advertises SQL
pushdown; core enforces complete SQL filters on original values before masking.
No custom SQL translator, delete matcher or serialized DataFile codec exists.

## Bounds and evidence

Each native Iceberg reader uses one IO runtime thread, one data-file stream and
8,192-row batches. Planning admits at most 10,000 manifest entries/native file
plans; Delta and Parquet have additional documented limits. Async planning and
batch pulls use operation deadlines. Metadata/delete buffers are upstream-owned;
these limits do not establish a hard RSS or cluster capacity cap. Active checks
reject late batches; close owns streams and opened providers.

The socket lane `tests/plugin_platform/test_cloud_formats.py` uses real Arrow,
Delta Kernel, PyIceberg and Apache Iceberg against disposable HTTP S3. Iceberg
COW/position/equality cases run separate fresh worker processes: the server denies
reads of any other task's data files, and workers must return exact rows after
deletes/replacement updates. This proves independent worker execution and disjoint
data IO on one host. An additional qualification ran three independent Linux VM
worker containers per delete mode against the host's S3 emulator, using the Linux
native wheel and passive JSON assignments. Exact rows, schemas and assigned-file
IO passed across that OS/network boundary. Actual multi-host AWS IAM/STS remains
deployment qualification.
Local lanes cover nested schemas, evolution, hidden keys, cancellation, cleanup
and path admission. No production AWS benchmark or speedup claim is made.

References: [delta-rs S3](https://delta-io.github.io/delta-rs/latest/integrations/object-storage/s3/),
[PyIceberg configuration](https://py.iceberg.apache.org/configuration/),
[Apache Iceberg Rust](https://github.com/apache/iceberg-rust),
[upstream file reader](https://docs.rs/iceberg/0.10.0/iceberg/arrow/struct.ArrowReader.html).
