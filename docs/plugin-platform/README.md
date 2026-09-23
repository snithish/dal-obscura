# Plugin platform: historical implementation handoff

**Owner decision 2026-09-23:** This handoff's policy draft/review/publication
workflows and upgrade/backfill assumptions are superseded. Follow the [live
configuration decision](../decisions/2026-09-live-configuration.md) for current
product and migration semantics. Plugin admission, the authenticated UI,
authorization, nested schemas, supported consumers, and pickle boundaries remain.

**Superseded 2026-09-20.** Start with the [current review and R01–R12 plan](../experience/README.md).
Completed N work is reconciled there. This page and the N queue below are retained
for historical context, not current execution. Existing B/G guarantees still apply.

Reviewed 2026-09-13 at c464152. **Planning documents only; paid-production HOLD.**
The historical queue was N01–N16; current execution starts at R01 in the linked plan.
Second pass at c6230b1 confirmed the same implementation baseline; the current
documents additionally specify separate editor/publisher handoff, bounded audit
backend queries and removal of obsolete policy routes and test constraints.

## Read in order

1. [Review and 24-packet reconciliation](IMPLEMENTATION_REVIEW.md): what exists,
   concrete remaining defects and evidence limits.
2. [Remaining implementation packets](IMPLEMENTATION_PLAN.md): dependencies,
   functional/non-functional requirements and atomic completion criteria.
3. [Acceptance specification](ACCEPTANCE.md): G01–G05 regression groups and
   B01–B22 remaining scenarios, fixtures and measurable release gates.
4. [UI and UX contract](UX_REQUIREMENTS.md): visual foundation, full workflows,
   accessibility and backend obligations.
5. [Technology decisions](TECHNOLOGY.md) and [cleanup plan](CLEANUP_PLAN.md):
   supported stable tooling, one implementation per responsibility, deletion proof.
6. [Progress ledger](STATUS.md): ready work, status definitions and evidence template.
7. [Architecture contract](ARCHITECTURE.md): extension boundaries retained by this plan.

## Authority and scope

The owner's latest request authorizes breaking changes and deletion of replaced
API/config/plugin/UI paths. No compatibility shims or dual-version runtime.
The earlier specific instruction preserving pickle logic/classes/import paths/
semantics remains in force. This is the only historical compatibility boundary
that broad cleanup may not silently rewrite.

Preserve policy authoring/management UI, backend enforcement, authentication,
authorization, nested schemas and equally secure local operation. Flight workers
remain stateless, masks/filters are core-validated DuckDB SQL, and scans stream
Arrow with bounded parallelism. Use the existing control-plane database; do not
add another state service. Target one isolated deployment/database/key set per
customer. Shared customer hosting is outside this release scope.

The plugin architecture already exists. Finish and qualify SQL-Iceberg/Iceberg,
REST-Iceberg/Iceberg and manifest/Parquet with Python/Arrow, DuckDB and Spark/JVM.
Further frameworks use the Arrow contract; advertise only executed support cells.
Plugins are explicitly installed/pinned trusted operator code. Browser uploads,
unrestricted dynamic imports and bypassing core authorization are not extensibility.

## One active source of work

These documents supersede conflicting execution instructions in the earlier UI,
gateway and X plans. Keep old functional guarantees except the explicitly replaced
compatibility rule; [acceptance](ACCEPTANCE.md) maps them into current ownership.

Historical snapshots: [plan](IMPLEMENTATION_PLAN_ARCHIVE_20260913.md),
[review](IMPLEMENTATION_REVIEW_ARCHIVE_20260913.md),
[acceptance](ACCEPTANCE_ARCHIVE_20260913.md),
[ledger](STATUS_ARCHIVE_20260913.md). They are evidence, not another queue.

An implementation agent must read packet dependencies, reuse completed code,
write the smallest behavioral regression case, deliver one atomic slice, remove
replaced paths and record executed evidence. A source check or mocked provider
does not qualify real operation. Do not claim production readiness until all gates
pass. This planning task changes no runtime, dependencies or executable tests.
