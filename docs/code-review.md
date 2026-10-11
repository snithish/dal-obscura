# Repository review — 2026-10-11

The review covered policy/read contracts, tickets, storage, identity, HTTP/Flight/CLI
composition, catalog and format plugins, native execution, the governance UI,
JVM connectors, tests and release configuration. Changes preserve governed-read
features while removing obsolete APIs and redundant representations.

## Changes

- Removed catalog forwarding aliases, single-ticket persistence, filter/property
  wrappers and unused secret-provider context. Callers now use the owning contract.
- Shared startup secret configuration parsing between service planes; removed the
  redacted-value compatibility exception from provider admission and persistence.
- Removed duplicate projection dependency state, dead UI lifecycle/mask helpers,
  unused inventory pagination keys and repetitive internal docstring examples.
- Moved shared read scenarios into `tests/support`; removed tests for unused APIs.
- Aligned Python tooling with the required 3.12 runtime, removed the empty SQLite
  extra and SQLAlchemy's obsolete `future` flag. Pinned JVM build plugins.
- Fixed metrics cardinality at the operation limit and rejection of non-finite
  durations, Parquet metadata drift, and Spark paths containing literal dots,
  collection-token names or quotes. Regressions first reproduced the failures.
- Included the completed generic catalog/FileIO and distributed COW/MoR work:
  `iceberg.catalog` replaces `iceberg.sql`; Delta handles pin location, version and
  table identity. Official clients own snapshots/deletes. Credentials remain local
  to workers and tickets contain passive JSON.

Validation remains at independent HTTP, configuration, plugin and signed-ticket
trust boundaries. The native Iceberg FileIO bridge, Arrow null/offset handling and
Delta deletion-vector bridge remain necessary because upstream APIs do not expose
all required distributed execution behavior; see [cloud formats](cloud-formats.md).

## Qualification

- Full Python: **1,086 passed, zero skips**, disposable PostgreSQL and real consumers.
- Fresh installed wheels, production source paths disabled: **255 passed**.
- SDK/conformance/Delta wheels with the service absent: **66 passed**.
- JVM: complete Maven verification passed, including six real Spark integration
  cases; an additional focused run covered literal-name zero-column reads.
- UI: **57 helper tests and 58 component/story tests passed**; TypeScript checking,
  generated API types, production build and bundle budgets passed.
- Ruff, formatting and Ty passed. Installed CLIs and fresh packaged database
  initialization/check passed. Six rebuilt Python wheels and the unchanged qualified
  native platform wheel were installed; six external plugin locks were regenerated
  from those exact installed artifacts. Release artifacts remain outside Git.

The [large nested-table report](scenarios/nested-mor.html) contains an earlier
captured source/artifact manifest and measured ten-partition, ten-million-row runs.
Those timings were not rerun for this review and are not performance claims for the
new revision. Current correctness qualification includes separate S3 worker
processes. Live cloud IAM renewal, multi-host capacity and new throughput baselines
remain outside this evidence.
