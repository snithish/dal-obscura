# Architecture

## Boundaries

`src/dal_obscura/` contains the service. Its folders represent ownership rather
than separate deployment planes. `policy/` contains immutable principals and
policies, authorization, filter validation and the SQL projection compiler.
`read/` coordinates plans and fetches. Its `tickets/` folder owns payloads,
signing and persistence contracts; `execution/` owns DuckDB, stream lifetimes
and resource limits. `sources/` resolves published catalogs and adapts SDK plugins.
`plugins/` admits installed artifacts. `runtime/` binds startup configuration and
secret backends. `identity/`, `storage/`, `control/` and `interfaces/` own identity,
persistence, administration and transport translation respectively. Administrative
features live under `control/assets/`, `catalogs/`, `policies/` and `workspace/`.
Asset authorization lives with assets; policy editing uses that authority.

`packages/` contains independent SDK, conformance and provider distributions.
Shared Arrow paths, traversal, identities and bounds live in the SDK so providers
can implement exact schema contracts without importing the service. Schema helpers
live under the SDK's `schema/`; storage reads and path admission share one owner.

The UI's `app/` owns startup and navigation, `data/` owns server communication,
and `features/` groups screens, state, stories and unit tests by purpose. `shared/`
contains widgets used across features. Browser contracts stay in separate feature
suites under `e2e/`. Spark connector packages group scan construction, readers,
Arrow conversion, options and filters around the registered data-source entry point.

Core tests mirror service features and transport boundaries. Shared test support
is grouped by owner; a helper used by one suite belongs beside that suite. Package
qualification remains independent of core test support. Empty package markers
declare Python packages; they do not forward old imports or introduce another API.

## Governed reads

Authentication captures a principal and one published configuration. Source,
policy and schema admission use the same snapshot. Database transactions finish
before provider IO. Planning retains one opened format and schema descriptor.
Every task and output schema is validated before ticket issuance becomes atomic.

A ticket carries bounded passive work, captured identity and schema information.
Fetch verifies signature, principal context, expiry, exchange limits and explicit
revocation. Filters run against original values before masking. Hidden predicate
columns remain internal. The projection compiler produces both SQL and declared
Arrow types, preserving metadata, SQL NULLs and ancestor-mask precedence.

Streams check deadlines, identity expiry and revocation before each emitted batch.
The stream owner closes readers, providers, iterators and connections on every
exit path. Limits bound work and buffers; they do not establish a cluster-wide
RSS or capacity guarantee. Provider calls that block internally may delay
cooperative cancellation until the next checkpoint.

## Distributed sources

Plugins return independent file, fragment or row-group tasks with disjoint
coverage. Group excess work within the task budget and return no tasks for an
empty scan. Worker credentials stay outside handles and tickets. Iceberg uses
Apache's Rust file reader for snapshots and position/equality deletes, including
historical equality field IDs through schema changes. The pinned reader source
and upstream fix are recorded in its Cargo configuration.

Bundled plugins use the same SDK and artifact admission as other installed plugins.
Plugin metadata and global options are captured at startup. Request handling does
not change shared configuration or rebuild the admission registry.
