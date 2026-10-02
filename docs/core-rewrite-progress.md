# Core rewrite execution ledger

Approved 2026-10-02. Preserve supported behavior while replacing duplicated
representations and ownership. No new persistent subsystem or compatibility stack.

## Sequence

1. Feature ledger, functional and measured baseline.
2. Immutable canonical policy/identity/schema contracts and structured errors.
3. Shared authorization/projection compiler for schema, planning, and preview.
4. Unified admitted source runtime and passive versioned native/plugin task codecs.
5. One read service; atomic issuance/claiming; explicit stream ownership.
6. Feature-owned control/storage/identity operations and transaction boundaries.
7. Generated client contracts, focused UI ownership, all consumer parity.
8. Measured optimizations, dead-code/module removal, full qualification and docs.

## Feature owners and required evidence

| Contract | Existing behavioral owner |
| --- | --- |
| Policy grants, wildcards, masks, exemptions, NULL defaults | tests/domain/access_control |
| Nested path identity and container/key dependencies | tests/common/query_planning; tests/application/access_flow |
| SQL validation, hidden predicates, filters before masks | domain row filters; planning filters; DuckDB filtering |
| Exact output schema and nested values | DuckDB masking; consumer contracts; native scans |
| Atomic configuration, provider leases, schema admission | live configuration and admission suites |
| Ticket integrity, captured identity, expiry, limits, revocation | ticket payload/store and application fetch/ticket suites |
| Streaming cancellation, errors, budgets, backpressure | DuckDB streaming; Flight streaming; RSS benchmarks |
| Pinned native scans and data/delete file semantics | native Iceberg conformance and consumer suites |
| Plugin admission, independent SDK, exact wheel isolation | plugin platform; package conformance; wheel CI |
| Catalogs, assets, owners, policy publication, preview, audit | owning control-plane HTTP and application suites |
| SSO, session isolation, attributes/allowed values, logout | session/identity suites and live Keycloak browser lane |
| CAS, audit atomicity, migration, backup/restore | SQLite and real PostgreSQL integration/recovery suites |
| UI authoring, navigation, stale requests, accessibility | UI helper, Storybook, synthetic/live browser lanes |
| Python, DuckDB, Polars, Java, Spark 3 | consumer tests, SDK, JVM verify |

## Baseline

- Commit: e86f1a5c2891edaef5ef9018ea69ab64d0f6dc8c.
- Python core: 120 files / 20,331 lines, generated protobuf excluded.
- Fresh fast lane: 894 passed / 66 deselected, 24.69 seconds.
- Prior full qualification: 960 non-benchmark Python cases, no skips, including
  real PostgreSQL and four consumer backends. This will be rerun after the rewrite.
- Bounded benchmark baseline: all six passed sequentially with local socket permission.
  Initial sandboxed run could not bind Flight; its partial timings are discarded.

## Completed slices

- Canonical immutable principal, policy, decision, and request collections; one
  mask/rule model and isolated validated filter ASTs.
- One recursive SQL/output-schema projection compiler used by reads and previews.
- One read service for schema, planning, and fetching; captured authorization;
  pre-issuance validation; explicit, idempotent stream ownership and cleanup.
- Passive JSON scan envelopes replace executable pickles. Native Iceberg file,
  delete-file, partition, metric and residual contracts round-trip explicitly.
- Built-in SQL Iceberg and installed plugins share the admitted SDK source path.
  One planning provider and schema descriptor are retained per request.
- Feature-owned policy/read/sources/identity/storage/control modules replace the
  old layered trees. ConfigStore and ProvisioningService forwarding façades are
  removed; routes own explicit transactions and invoke feature commands.
- Atomic published snapshots live in storage. Workspace summary uses a single
  aggregate query instead of hydrating assets. Conflicts carry typed revisions.
- One typed schema traversal serves leaf expansion, schema bounds, synthetic
  preview, schema discovery, and admission identities. Literal dotted names and
  literal collection-token field names retain their distinct identities.
- Iceberg tickets balance estimated data plus delete-file bytes, deterministically,
  instead of round-robin file counts. Every task remains assigned exactly once.
- Migration 0003 rewrites the moved built-in OIDC provider selector and invalidates
  legacy scan tickets while retaining configuration and audit data.

## Final qualification

- Full Python: 992 passed, zero skips (31.78 seconds), with disposable real
  PostgreSQL and all four Python/PyArrow/DuckDB/Polars consumer paths enabled.
- UI: 59 helper cases, 58 component/story cases, 89 browser workflows; TypeScript,
  generated API freshness and bundle budgets pass.
- Real Keycloak: PKCE sign-in, claims, inventory, session reload and local/federated
  logout pass. Remote Docker's clock was about two seconds ahead; test harness
  waits three seconds before ID-token validation. Production validation unchanged.
- JVM `verify`: BUILD SUCCESS, including six Spark 3 integration cases, zero skips.
- Five independent 0.2 wheels built with uv_build. Installed conformance/plugins:
  76 cases; actual admitted source routing: 35 cases. Exact lock generation,
  packaged migrations, CLIs, client-only install and SDK-containing sdist pass.
- ruff lint/format, ty, OpenAPI snapshot/client generation and semantic index pass.
- All six bounded benchmarks pass after paired fixture normalization; fetch
  16.86 → 16.74 ms. Explicit passive metadata dispatch adds roughly 1.1 ms.
  Transform medians vary; no universal speedup claimed.
- Large scan: 35 million rows across both probes; larger 25M run streams 3,055
  chunks with 8,192-row maximum and 512 MiB RSS growth, under the 1 GiB limit.
- Final non-generated core: 122 files / 19,018 lines, 6.4% fewer lines than baseline.

Shared control source discovery, SDK API 2 cutover, generated UI contracts and
navigation/draft/query owners are complete. Migration preservation covers catalog
and asset/policy revisions, rules, owners and audit, plus old-ticket invalidation.
The [visual review](architecture/core-rewrite-review.html) records architecture,
tradeoffs and remaining limits; its JSON record contains exact measurements.

## Completion

Implementation and qualification complete. Commit/push records follow Git history.
No compatibility forwarding tree, executable production pickle path or new persistent
subsystem remains. Deployment-wide capacity and unsupported consumer cells remain
outside this rewrite's verified claims; see the cutover and compatibility guides.

## Cutover

Internal imports and plugin task contracts can break. Preserve configuration,
policies, ownership and audit with migrations where necessary. Outstanding tickets
using the old executable task encoding will be invalidated; clients must re-plan.
