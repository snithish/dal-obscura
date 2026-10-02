# Rebuilt plugin contract

All bundled plugins now implement one breaking API 2 contract in release 0.2.0.
The server contains the native SQL catalog and Iceberg format; the independent
manifest/Parquet and Iceberg REST wheels use the same public SDK boundary.

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

## Qualification

- Full Python suite: **1,006 passed, zero skipped**, including disposable PostgreSQL
  and all four real Flight consumer backends.
- Fresh environment containing only built wheels: **88 package checks** and
  **38 runtime routing checks** passed, with repository source paths disabled.
- All three external entry points admitted and loaded from the exact generated lock.
- SDK-only environment: JSON task/request smoke passed with the service absent.
- Server/control CLI and packaged migration upgrade/check passed.
- JVM `mvn verify`: **BUILD SUCCESS**, including six Spark integration tests.
- Ruff lint/format, Ty and all-file pre-commit passed.
- Focused ticket-to-response benchmark: 19.4343 ms mean across 28 rounds. This is
  a qualification observation, not a paired performance comparison.

Release artifacts are built into `dist/plugins-0.2.0/`: all five wheels,
`plugin-lock.json`, `qualification.json` and `SHA256SUMS`. The compressed bundle is
`dist/dal-obscura-plugins-0.2.0.tar.gz`. Build output remains outside Git.
The lock qualifies these exact wheels; changing an artifact requires a new lock.
The release was validated locally and has not been published to a package registry.

Read the [SDK contract](../packages/plugin-api/README.md),
[conformance guide](../packages/plugin-conformance/README.md) and
[cutover runbook](core-cutover.md) for details.
