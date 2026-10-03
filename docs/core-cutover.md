# Core 0.2 cutover

Version 0.2 replaces the layered internal trees with policy/read/sources/identity/
storage/control owners, uses plugin API 2, and removes executable scan tickets.
Flight protobuf v1 and HTTP `/v1` remain unchanged. Supported Python, DuckDB,
Polars, Java and Spark 3 client behavior is preserved; internal imports are breaking.

## Fresh deployment

There are no deployed consumers requiring compatibility. Earlier databases,
handles, tickets, catalog type discriminators and plugin artifacts are unsupported.
Do not run this release against an existing demo database. Existing user demos
are deliberately left untouched during qualification.

1. Build/install all six 0.2.0 wheels: service, SDK, conformance, manifest/Parquet,
   REST Iceberg and Delta. Native SQL catalog/Iceberg ship in the service wheel.
2. Generate the artifact lock from those exact installed wheels and deploy the
   same immutable artifacts/lock to both planes. The SDK is the sole factory/task
   contract; handles and tasks contain bounded passive JSON.
3. Configure a fresh database and run `dal-obscura-migrate upgrade`, then `check`.
   The sole revision `20261003_0001` creates the current schema directly. There
   are no ALTER, data translation or old-ticket cleanup migrations.
4. Publish catalogs, policies, identities and runtime configuration using current
   contracts. Storage credentials belong to worker identity; never put them into
   published table handles or tickets.
5. Start both planes and verify readiness, authentication, allowed/denied reads,
   schema discovery, save/preview, revocation and provider SSO sign-out.

Services check the schema and never migrate at startup. See
[plugin qualification](plugin-rebuild.md) for installed-wheel checks and
[cloud format execution](cloud-formats.md) for native engine boundaries.

## Resource and correctness boundaries

One database snapshot binds each new read's source, policy and schema admission.
Planning retains one opened format/schema descriptor and validates every task
before atomic ticket issuance. Existing tickets keep captured policy until expiry
or explicit revocation. Each output batch checks ticket expiry, identity expiry,
stream deadline and revocation. Streams close on exhaustion, failure and early stop.

DuckDB receives Arrow batches and applies the full SQL filter before masking.
The same projection compiler declares and emits nested output types. Provider
pushdown is advisory. Iceberg preserves pinned metadata and projection order; DuckDB owns delete
interpretation. Copy-on-write and position-delete tasks balance file bytes.
Equality deletes use one native scan with bounded intra-query parallelism.

Limits are per worker. Backend blocking IO cannot always be interrupted by deadline
checks. Planning memory grows with file count; associated delete buffers are not
charged to a hard global byte cap. Bounded batch validation does not impose a hard
process RSS limit. See [execution invariants](read-execution-invariants.md).

## Qualification

See the [rewrite report](architecture/core-rewrite-review.html) for measured results,
source changes and remaining limits. The [architecture atlas](architecture/architecture-atlas.html)
and its [editable source](architecture/architecture-atlas.md) describe the shipped owners.
