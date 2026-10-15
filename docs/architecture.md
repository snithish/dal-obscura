# Architecture

## Boundaries

`src/dal_obscura/` contains the service. Its folders represent ownership rather
than separate deployment planes. `kernel/` holds immutable identity, policy,
projection and published-snapshot values. Identity has no policy dependency;
policy has no planning dependency. Kernel code cannot open databases, construct
providers or depend on transport frameworks.

`application/reads/` owns planning, ticket contracts and execution lifetimes.
`application/governance/` owns administrative commands and repository contracts.
Governance can use the read compiler and row-transform ports; it cannot depend on
read orchestration. Commands require explicit actors and correlation IDs. They
own transaction boundaries, authorization, revision fences and audit writes.
Repositories return detached values. A closed unit of work cannot reopen a session.

`adapters/` owns SQL persistence, DuckDB compilation and transformation, provider
execution and ticket signing. Existing `identity/`, `plugins/` and `runtime/`
implementations are concrete adapters too. Stored identity implementation paths
remain stable. Adapters use application contracts and kernel values and cannot
import HTTP or CLI handlers. `bootstrap/` selects implementations and owns startup
resources. `interfaces/` translates transport input and output around constructed
application dependencies; CLI entrypoints delegate resource setup to bootstrap.
Request handlers cannot choose providers or borrow SQL sessions.

HTTP request and response models belong with their route feature. Only strict
request handling and error envelopes are shared. Operator commands separate
argument dispatch from image preparation, control transport, deployment and Flight
verification. Startup configuration validation belongs to bootstrap, while catalog
option validation belongs to governance and keeps its security bounds together.

The Flight command codec serializes and parses protobuf bytes independently of
server request admission. Flight handlers apply validation and limits around that
codec. Read and control health endpoints report their own readiness conditions;
bootstrap supplies their dependencies and owns any background server resources.

These rules include function-local and type-only imports. Architecture tests
resolve package imports and namespace packages rather than checking import text.
Startup admits locked plugin entry points and fixed identity provider identifiers;
dynamic identifiers do not bypass dependency rules.

`packages/` contains independent SDK, conformance and provider distributions.
Shared Arrow paths, traversal, identities and bounds live in the SDK so providers
can implement exact schema contracts without importing the service. Schema helpers
live under the SDK's `schema/`; storage reads and path admission share one owner.

The UI's `app/` owns startup, shell presentation and navigation. The identity
session boundary owns generation, request cancellation and the private query cache.
It invalidates the generation and unmounts private children before awaiting logout.
Every private continuation must reject an obsolete generation even when abort does
not stop a response. Feature routes own their queries; the policy controller owns
drafts, submissions and preview persona fences. Dirty state reaches navigation
through explicit callbacks. `data/` owns transport and generated API adaptation
and cannot import views. `shared/` contains presentation used across features.
Browser contracts stay in separate feature suites under `e2e/`. Spark connector
packages group scan construction, readers, Arrow conversion, options and filters
around the registered data-source entry point.

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
columns remain internal. The projection compiler produces SQL and the declared
Arrow schema together. Its existing output normalization, SQL NULL behavior and
ancestor-mask precedence are shared by planning and transformation. Each input
batch must match the provider's declared execution schema, including Arrow metadata.

One process supervisor admits lazy execution before opening its source. Input
checkpoints test cancellation, deadlines and identity/ticket expiry without SQL,
including when the filter emits no rows. Every output batch receives one strict
SQL revocation check immediately before disclosure. The execution lease closes
readers, providers, iterators and connections and releases admission on every exit
path. Cleanup cannot replace a primary error. Limits bound work and retained
buffers; they do not establish a cluster-wide RSS or capacity guarantee. Provider
calls that block internally may delay cooperative cancellation until the next
checkpoint.

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
