# Rebuilt plugin contract

All bundled plugins now implement one breaking API 2 contract in release 0.2.0.
The server contains the native SQL catalog and Iceberg format; the independent
manifest/Parquet and Iceberg REST wheels use the same public SDK boundary.
The new independent [Delta wheel](../packages/delta-plugin/README.md) adds
`delta.directory` and `delta` entry points with pinned parallel scans and native
deletion-vector handling. Build and admit its exact artifact alongside the other
external plugins; source-only registration is not release qualification.

Factories bind configuration or a table handle once. Catalogs expose discovery,
resolution and cleanup; configuration validation happens during construction.
Formats expose `schema(context)`, `plan(ScanRequest, context)` and
`execute(ScanTask, context)`. The request contains one exact projected Arrow schema
and task budget. Task objects validate and recursively freeze bounded passive JSON;
serialization produces a detached JSON object. No generic object/null task,
repeated handle argument, separate projection, plugin-supplied fingerprint,
schema-version counter, configuration-validation hook, or capability negotiation
remains. Core uses SDK factory protocols directly.

Descriptors and configuration options are recursively immutable. Descriptor JSON
serialization is shared by exact admission digests and HTTP metadata. One operation
guard handles deadlines and cancellation. Both runtime and conformance close lazy
planning iterators on success, error and budget rejection. Arrow schemas include
metadata in equality checks. Empty Iceberg scans return zero tasks. Parquet plans
keep projection once per scan, rather than repeating it on every row group.

Conformance now checks partial scans against their requested schema, and discovery
coverage uses structured identifiers to preserve literal dots. Cancellation tests
trigger cancellation from actual provider work rather than counting internal
validation calls. Obsolete validation-hook and fingerprint fixtures were removed.

## Build and qualification

Rebuild all seven distributions whenever an owning artifact changes; the native
reader needs a wheel for each worker platform:

```bash
uv build --wheel --out-dir dist/plugins-0.2.0 .
uv build --wheel --out-dir dist/plugins-0.2.0 packages/plugin-api
uv build --wheel --out-dir dist/plugins-0.2.0 packages/plugin-conformance
uv build --wheel --out-dir dist/plugins-0.2.0 packages/manifest-parquet-plugin
uv build --wheel --out-dir dist/plugins-0.2.0 packages/iceberg-rest-plugin
uv build --wheel --out-dir dist/plugins-0.2.0 packages/delta-plugin
uv build --wheel --out-dir dist/plugins-0.2.0 packages/iceberg-reader
```

Install exact wheels in a fresh environment. Run package and core routing tests
with `pytest -o pythonpath=` so repository production sources cannot satisfy
imports. Include `packages/plugin-api/tests` and the socket cloud-format lane.
Also qualify SDK/conformance/Delta with the service absent. Run CLI smoke tests
and migration upgrade/check against a fresh disposable database.

Generate the external lock from those installed artifacts:

```bash
python scripts/build_plugin_lock.py --output /tmp/plugin-lock.json \
  --plugin catalog:iceberg.rest --plugin catalog:manifest \
  --plugin table_format:parquet.dataset --plugin catalog:delta.directory \
  --plugin table_format:delta
```

Admit all five entries through the registry and verify exact artifact identities.
Native SQL/Iceberg ship in the service wheel. The independently packaged
[native Iceberg reader](../packages/iceberg-reader/README.md) is a pinned server
dependency. Build it with Rust 1.94 and ship its exact platform wheel to every worker;
it implements no additional plugin ABI. Workers never compile/download during reads.

Full mandatory Python qualification uses disposable PostgreSQL, all five real
Flight backend fixtures and the no-skips wrapper. S3 tests use a disposable HTTP
emulator with the actual Arrow/Delta/Apache Iceberg clients. They establish storage and
scan contracts, not live AWS IAM/STS renewal or production throughput. See
[cloud execution](cloud-formats.md) for COW/MoR, upstream API gaps and budgets.

Release output remains outside Git in `dist/plugins-0.2.0/`: seven distributions' wheels,
`plugin-lock.json`, `qualification.json` and `SHA256SUMS`. The local archive is
`dist/dal-obscura-plugins-0.2.0.tar.gz`. Artifact changes require a regenerated
lock and checksums; source-only registration is insufficient. The release is
qualified locally and has not been published to a package registry.

Read the [SDK](../packages/plugin-api/README.md),
[conformance guide](../packages/plugin-conformance/README.md) and
[fresh deployment guide](core-cutover.md).

## Distributed reader qualification on 2026-10-03

- Full Python: **1,064 passed, zero skips**, disposable PostgreSQL 17.10 and all
  five real Flight backend fixtures.
- Installed wheels with production source paths disabled: **120 package checks**,
  **79 routing/cloud checks**, **33 SDK/Delta checks without the service**, and
  **46 final native checks** after the last reader/server rebuild.
- COW, position and equality delete tasks passed in separate fresh processes over
  S3 for both file and row-group layouts. Guards deny another task's data files;
  exact schemas, rows, updates and pinned snapshots are checked.
- The final native wheel compiled on macOS arm64 and Linux aarch64. Three Linux
  VM worker containers per delete mode also read the host S3 emulator using passive
  JSON tasks, with exact rows, schemas and assigned-file reads.
- Five exact external locks, CLI smoke checks, fresh packaged schema baseline,
  Ruff, Ty, Rust fmt/Clippy, all-file pre-commit and documentation links passed.
- Full service Docker image build reached dependency installation but exceeded
  available VM disk space. Linux native compilation and worker execution passed
  using host-backed build caches; the complete image build remains unqualified.
- No live AWS IAM/STS renewal, cluster capacity or throughput claim. Existing demo
  services were preserved.

Old Iceberg task payloads and `parallelism` configuration are intentionally
unsupported. Drain old tickets and deploy matching service/native reader wheels
to every worker before issuing new plans. The public plugin API remains version 2.

## Previous qualification on 2026-10-03 (before distributed reader cutover)

- Full Python: **1,049 passed, zero skips**, using disposable PostgreSQL and all
  five real Flight backend fixtures, including the S3 emulator lane.
- Final installed wheels: **120 package checks**, **64 routing/cloud checks**,
  and **33 SDK/Delta checks without the service**; production source paths disabled.
- Five exact external lock entries admitted; server/control CLI and packaged
  baseline migration upgrade/check passed.
- Ruff lint/format, Ty, all-file pre-commit and changed documentation links passed.
- Native multi-file observation: 16 files, 16,000 rows, four tasks, 170.58 ms mean
  across three rounds. This is a smoke measurement without a paired baseline;
  it establishes no speedup or production capacity claim.

Detailed artifact provenance, current hashes and limits are in the local release
`qualification.json`. Existing demo services were not modified.
