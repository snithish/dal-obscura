# Native Iceberg file reader

This library binds Apache Iceberg Rust 0.10.0's public file planner and Arrow
reader, pinned to upstream commit `22c256e985a68b3a71dd3890d3ebd6bedc3e7b0d`.
Released 0.10.x drops nonmatching NULL rows in equality deletes; this exact
maintained commit includes [Apache PR #2781](https://github.com/apache/iceberg-rust/pull/2781).
Remove the Cargo source patch once a release includes that fix. It has no catalog factory, plugin ABI, protocol parser, or delete matcher.
The SDK Iceberg format remains owned by `dal_obscura.sources.iceberg_plugin`.

`Reader` opens captured metadata and snapshot, retains at most 10,000 native file
plans, and exposes file paths/costs. `start(fragments)` reads only assigned file byte ranges,
with upstream position/equality deletes and sequence/partition applicability.
Workers reconstruct plans independently from metadata: native task objects,
partitions, and name mappings never cross the wire. Shared metadata/delete reads
are expected; whole-table data rescans are forbidden. Manifest split offsets
allow one large file to fan out across workers. Absent offsets, that file remains
one unit; no speculative footer discovery is added during planning. Upstream
range selection preserves absolute positions for position deletes. Footer/range
read-ahead may overlap storage bytes; assigned row groups remain disjoint.

The official Python binding exposes a single-partition DataFusion table, but no
independently executable file reader. This small PyO3 bridge exposes those Rust
APIs directly. Upstream planning can omit historical equality keys after a column
is dropped: the bridge restores only required fields using upstream schema
history/pruning. All delete decoding, matching, and execution remain upstream.
The Python format uses PyIceberg's field-ID projection visitor after deletes to
preserve renames, additions, dropped keys, nested types, and exact Arrow metadata.
SQL filter/mask enforcement remains in the governed core; no SQL pushdown is
advertised by this reader.

OpenDAL owns local and S3 IO and the native AWS credential chain. Credentials are
worker-local; only passive file assignments cross processes. Reads release the
GIL, use one IO runtime thread and one data-file stream, and emit 8,192-row batches.
Operation deadlines bound async planning/pulls; closing drops the stream. Native
metadata/delete buffers are upstream-owned, without a hard process RSS cap.

Build with the repository's pinned Rust 1.94 toolchain and `uv build --wheel
packages/iceberg-reader`. Ship its exact platform wheel with every worker. The
owning tests are `tests/plugin_platform/test_iceberg_format_plugin.py` and the
S3 socket lane `tests/plugin_platform/test_cloud_formats.py`: separate fresh
processes are denied access to other tasks' data files and must return exact rows.
This proves passive worker portability and disjoint IO on one test host. Additional
Linux VM worker-container qualification against the host S3 emulator passed all
three delete modes with the Linux native wheel, exact rows and schemas, and the
same assigned-file IO guard. Actual multi-host AWS IAM/STS and deployment
throughput remain deployment qualification.

Upstream: [Apache Iceberg Rust](https://github.com/apache/iceberg-rust),
[public Arrow reader](https://docs.rs/iceberg/0.10.0/iceberg/arrow/struct.ArrowReader.html),
[official Python binding](https://pypi.org/project/pyiceberg-core/).
