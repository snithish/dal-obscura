# Core 0.2 cutover

Version 0.2 replaces the layered internal trees with policy/read/sources/identity/
storage/control owners, uses plugin API 2, and removes executable scan tickets.
Flight protobuf v1 and HTTP `/v1` remain unchanged. Supported Python, DuckDB,
Polars, Java and Spark 3 client behavior is preserved; internal imports are breaking.

## Upgrade sequence

1. Back up the configuration database using the established maintenance workflow.
   Preserve the deployment's runtime secrets and the previous plugin lock/artifacts.
2. Stop both planes and all workers before migration. This is a coordinated cutover;
   mixed 0.1/0.2 workers and rolling compatibility are unsupported.
3. Build/install the server and independent SDK 0.2.0 wheels. Rebuild external
   plugins against SDK 0.2.0, declare API 2 and entry-point groups
   `dal_obscura.catalogs.v2` / `dal_obscura.table_formats.v2`, and run conformance.
   Tasks must be bounded passive JSON and survive JSON round trips.
4. Regenerate the admitted plugin lock from the exact installed wheels. Mount the
   same immutable lock and artifacts in both planes; do not reuse API 1 entries.
5. Run `dal-obscura-migrate upgrade`, then `dal-obscura-migrate check` with the
   deployment database URL. Migration `20261002_0003` changes the moved built-in
   OIDC selector and deletes outstanding executable tickets. It retains catalogs,
   asset/policy revisions, rules, owners and audit. Clients must plan fresh tickets.
6. Start both planes, verify readiness, then exercise sign-in, allowed/denied reads,
   schema discovery, policy save/preview, explicit revocation and SSO sign-out.

Do not downgrade a migrated database into old workers. Restore the backup with
matching prior artifacts if rollback is necessary. Database schemas are checked
at startup; services do not migrate automatically.

## Resource and correctness boundaries

One database snapshot binds each new read's source, policy and schema admission.
Planning retains one opened format/schema descriptor and validates every task
before atomic ticket issuance. Existing tickets keep captured policy until expiry
or explicit revocation. Each output batch checks ticket expiry, identity expiry,
stream deadline and revocation. Streams close on exhaustion, failure and early stop.

DuckDB receives Arrow batches and applies the full SQL filter before masking.
The same projection compiler declares and emits nested output types. Provider
pushdown is advisory. Iceberg preserves pinned metadata, file/delete tasks and
projection order; deterministic task grouping balances estimated byte costs.

Limits are per worker. Backend blocking IO cannot always be interrupted by deadline
checks. Planning memory grows with file count; associated delete buffers are not
charged to a hard global byte cap. Bounded batch validation does not impose a hard
process RSS limit. See [execution invariants](read-execution-invariants.md).

## Qualification

See the [rewrite report](architecture/core-rewrite-review.html) for measured results,
source changes and remaining limits. The [architecture atlas](architecture/architecture-atlas.html)
and its [editable source](architecture/architecture-atlas.md) describe the shipped owners.
