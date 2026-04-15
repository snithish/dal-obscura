# Spark Connector Design

## Goal

Add a production-grade, read-only Apache Spark 3.x connector that reads data
through `dal-obscura` while preserving the service's current security and
planning model.

The connector must:

- integrate with Spark through DataSource V2
- authenticate to `dal-obscura` as the actual end user
- keep `dal-obscura` as the authoritative planner for access, masking, row
  filters, and backend scan splits
- establish a repo structure that can support future connectors for other
  engines and languages without forcing them through a JVM-only abstraction

## Chosen Constraints

The design below reflects the following validated decisions:

- first target runtime: self-managed OSS Spark 3.x
- first connector scope: read-only batch reads only
- connector implementation language: Java
- JVM build tool: Maven
- auth propagation model: end-user passthrough
- token acquisition model: explicit bearer token passed via Spark read options
  or session configuration

## Problem

`dal-obscura` already exposes a plan-then-fetch Arrow Flight contract:

1. `get_flight_info` accepts a JSON payload containing `catalog`, `target`,
   `columns`, and optional `row_filter`
2. `PlanAccessUseCase` authenticates the caller, applies policy, validates the
   requested filter, plans backend work, and returns one signed ticket per split
3. `do_get` re-authenticates the caller, verifies ticket freshness, executes
   the planned work, and streams Arrow batches with residual filtering and
   masking applied

This flow is already a strong fit for query-engine integration. The main gap is
that Spark 3.x does not have a Python datasource path suitable for this
integration, so a production connector must live on the JVM side and adapt
Spark's DataSource V2 interfaces to the existing Flight contract.

## Non-Goals

- write support in the first release
- structured streaming support in the first release
- managed-platform-specific deployment work for Databricks, EMR, or Glue
- changing `dal-obscura` into a Spark-specific service
- moving authoritative planning logic into the client or connector
- defining a single runtime-code layer that all future language connectors must
  reuse
- introducing impersonation, delegation, or token-provider plugins in the first
  release

## Core Decision

Build the connector as a polyglot connector workspace with a Maven-based JVM
subtree:

- `connectors/jvm/dal-obscura-client-java`: Java client library for the
  existing Flight plan/get flow
- `connectors/jvm/spark3-datasource`: Spark DataSource V2 adapter built on top
  of the Java client
- `connectors/jvm/connector-testkit-jvm`: shared JVM-side test fixtures
- `connectors/jvm/integration-tests-jvm`: end-to-end tests against a live local
  `dal-obscura` service
- `connectors/contract-fixtures`: language-neutral fixtures and compatibility
  cases for future clients in other languages

The reusable cross-language layer is the protocol contract and test fixtures,
not shared Java runtime code.

## Planning Boundary

`dal-obscura` remains the only system that plans reads authoritatively.

### `dal-obscura` responsibilities

- authenticate the end user
- authorize requested columns and row filters
- combine policy filters with engine filters
- plan backend scan tasks
- mint signed tickets
- validate tickets and policy freshness at fetch time
- apply residual filters and masks during streaming

### `dal-obscura-client-java` responsibilities

- construct the Flight planning request
- inject auth headers
- call `get_flight_info`
- map the returned schema and endpoints into client-side models
- execute `do_get` for a ticket
- expose Arrow data to engine-specific adapters

### `spark3-datasource` responsibilities

- implement Spark DataSource V2 interfaces
- translate Spark projection/filter pushdown into client requests
- turn each returned ticket into a Spark `InputPartition`
- expose streamed Arrow data as Spark columnar batches

Spark still performs execution planning in the limited sense that it schedules
tasks over the already-planned tickets. It does not own access planning, split
planning, masking logic, or backend scan semantics.

## Proposed Repo Structure

```text
/dal-obscura
├── src/dal_obscura/                         # Existing Python service
├── tests/                                   # Existing Python tests
├── docs/
│   └── superpowers/
│       ├── specs/
│       └── plans/
├── connectors/
│   ├── README.md                            # Connector architecture + support matrix
│   ├── contract-fixtures/                   # Language-neutral request/response cases
│   │   ├── plan-requests/
│   │   ├── filter-translation/
│   │   └── error-cases/
│   ├── jvm/
│   │   ├── pom.xml                          # Maven aggregator parent
│   │   ├── dal-obscura-client-java/
│   │   ├── spark3-datasource/
│   │   ├── connector-testkit-jvm/
│   │   └── integration-tests-jvm/
│   ├── python/                              # Reserved for future Python-native clients
│   │   └── README.md
│   └── go/                                  # Reserved for future Go-native clients
│       └── README.md
└── README.md
```

This structure keeps the existing Python toolchain intact while creating a
clear top-level home for JVM and future non-JVM connector families.

## Connector API

The first release should expose a normal Spark read path:

```java
Dataset<Row> df =
    spark.read()
         .format("dal_obscura")
         .option("dal.uri", "grpc+tls://dal.example:8815")
         .option("dal.catalog", "analytics")
         .option("dal.target", "default.users")
         .option("dal.auth.token", userToken)
         .load();
```

Spark operators such as `.select(...)` and `.filter(...)` should participate in
normal projection/filter pushdown through DataSource V2.

### Configuration precedence

- per-read `.option(...)` values override session configuration
- session configuration is the fallback for repeated reads in one Spark session
- the token is required on both planning and fetch calls
- `dal.uri` accepts either `grpc+tcp://host:port` or
  `grpc+tls://host:port`
- the datasource should implement `SessionConfigSupport` with
  `keyPrefix() == "dal_obscura"`, so Spark session configuration falls through
  under the `spark.datasource.dal_obscura.*` prefix, for example:
  - `spark.datasource.dal_obscura.uri`
  - `spark.datasource.dal_obscura.catalog`
  - `spark.datasource.dal_obscura.target`
  - `spark.datasource.dal_obscura.auth.token`

## Runtime Flow

1. Spark constructs a scan through the DataSource V2 adapter.
2. The connector captures required columns and the subset of Spark filters that
   can be translated safely.
3. `dal-obscura-client-java` sends a Flight planning request with:
   - `catalog`
   - `target`
   - explicit projected columns
   - translated `row_filter` SQL when available
   - the bearer token in request headers
4. `dal-obscura` authenticates and authorizes the user, combines policy and
   engine filters, plans backend work, and returns one endpoint per signed
   ticket.
5. The Spark adapter maps each returned ticket to one Spark `InputPartition`.
6. Executor-side partition readers call `do_get` with the same bearer token and
   stream Arrow data for the assigned ticket.
7. The Spark adapter exposes the Arrow result as Spark columnar batches.
8. Any unsupported Spark filters remain residual filters evaluated by Spark, not
   approximated by the connector.

## Filter And Projection Strategy

### Projection

Projection pushdown should always be attempted because `dal-obscura` already
accepts explicit column lists in the plan request.

### Filters

Filter pushdown must be conservative.

The connector should only translate a safe subset of Spark predicates into the
`row_filter` SQL string sent to `dal-obscura`. Any filter that does not map
cleanly to current service semantics must be returned to Spark as a residual
filter.

The first release translates only this subset:

- `EqualTo`
- `GreaterThan`
- `GreaterThanOrEqual`
- `LessThan`
- `LessThanOrEqual`
- `In`
- `IsNull`
- `IsNotNull`
- `And`
- `Or`

Additional constraints for translated filters:

- operands must be direct field references plus literal values
- nested fields are allowed only when Spark exposes them as a direct dotted path
  that matches `dal-obscura` column-path semantics
- `Not`, `EqualNullSafe`, string pattern predicates, user-defined expressions,
  and any function-shaped expressions remain residual filters in Spark

The first release prefers correctness and transparency over aggressive
pushdown. That means:

- push only the explicit subset above
- keep unsupported expressions as residual Spark filters
- never silently weaken or reinterpret a Spark predicate

This aligns with `dal-obscura`'s current row-filter contract, which already
validates SQL expressions and combines them with policy filters.

## Service Impact

The connector does not require a protocol redesign for v1.

The current service contract is already sufficient because:

- `interfaces/flight/contracts.py` accepts `catalog`, `target`, `columns`, and
  optional `row_filter`
- `PlanAccessUseCase` already performs authn/authz, projection handling, row
  filter validation, and ticket minting
- `FetchStreamUseCase` already re-checks caller identity and streams the
  ticket-bound result

The initial service-side work should focus on integration hardening rather than
new semantics:

- document the connector contract explicitly
- ensure connector-facing errors are stable and understandable
- add connector-focused integration coverage
- confirm logs and metrics expose enough information to debug connector reads

Optional future improvements such as richer capability reporting or more
structured error metadata can be layered on later, but the first Spark
connector should not depend on them.

## Packaging And Module Boundaries

### `connectors/jvm/pom.xml`

Acts as a Maven aggregator parent for JVM modules and keeps all JVM build logic
isolated from the Python root.

### `dal-obscura-client-java`

Owns:

- Flight client lifecycle
- request and response models for planning and ticket execution
- auth-header injection
- schema mapping
- endpoint/ticket abstractions
- error mapping from Flight failures into connector-friendly exceptions

Does not own:

- Spark types
- Spark filter translation types
- Spark partition readers

### `spark3-datasource`

Owns:

- `TableProvider`
- scan / batch / partition abstractions
- Spark filter/projection pushdown hooks
- Spark option parsing
- conversion from Arrow vectors to Spark `ColumnarBatch`

Does not own:

- planning authority
- direct policy logic
- connector behavior that should be reusable by other JVM engines

### `connector-testkit-jvm`

Owns reusable JVM-side helpers for:

- local service startup
- sample table fixtures
- connector-side assertions
- common failure-path setup

### `connectors/contract-fixtures`

Owns language-neutral artifacts such as:

- canonical plan request examples
- filter translation cases
- expected error cases
- schema and endpoint compatibility fixtures

## Error-Handling Rules

- authorization failures from `dal-obscura` must fail the Spark read clearly;
  they must not degrade into empty datasets
- invalid or unsupported filter translations remain residual Spark filters
- executor-side Flight failures should fail the Spark task and rely on normal
  Spark retry behavior
- the connector should avoid privileged result caching in v1
- the connector should treat returned tickets as opaque server-planned splits

## Testing Strategy

### Java client tests

Cover:

- request construction
- auth-header injection
- endpoint parsing
- schema mapping
- Flight exception mapping

### Spark adapter tests

Cover:

- option precedence
- projection pushdown
- supported filter pushdown
- residual filter handling
- ticket-to-partition mapping
- columnar batch conversion

### End-to-end JVM integration tests

Run Spark against a live local `dal-obscura` service and verify:

- successful reads from representative Iceberg-backed datasets
- correct masking and row filtering
- correct handling of policy denial
- missing / expired / invalid token behavior
- task retry behavior after stream failures

### Packaging verification

Cover:

- Maven build correctness
- shaded artifact sanity where needed
- local `spark-submit` smoke tests
- documented deployment coordinates for self-managed clusters

## Future Connector Strategy

This design intentionally avoids coupling all future connectors to Spark or to
the JVM.

Expected expansion pattern:

- future JVM engines reuse `dal-obscura-client-java`
- future non-JVM engines build native clients in their own language subtree
- all clients share documented protocol expectations plus language-neutral
  fixtures under `connectors/contract-fixtures`

That keeps the system extensible without forcing a lowest-common-denominator
runtime abstraction too early.

## References

### Current repo references

- `README.md`
- `src/dal_obscura/interfaces/flight/contracts.py`
- `src/dal_obscura/application/use_cases/plan_access.py`
- `src/dal_obscura/application/use_cases/fetch_stream.py`
- `src/dal_obscura/domain/access_control/filters.py`
- `tests/interfaces/flight/test_service_streaming.py`
- `tests/interfaces/flight/test_integration_flight_backends.py`

### External references

- Spark `TableProvider` JavaDoc:
  https://spark.apache.org/docs/3.5.6/api/java/org/apache/spark/sql/connector/catalog/TableProvider.html
- Spark read-side package overview:
  https://spark.apache.org/docs/3.5.6/api/java/org/apache/spark/sql/connector/read/package-summary.html
- Spark `Batch` JavaDoc:
  https://spark.apache.org/docs/3.5.6/api/java/org/apache/spark/sql/connector/read/Batch.html
- Spark `PartitionReaderFactory` JavaDoc:
  https://spark.apache.org/docs/3.5.6/api/java/org/apache/spark/sql/connector/read/PartitionReaderFactory.html
- Spark `DataSourceRegister` JavaDoc:
  https://spark.apache.org/docs/3.5.3/api/java/org/apache/spark/sql/sources/DataSourceRegister.html
- Spark vectorized package:
  https://spark.apache.org/docs/3.5.6/api/java/org/apache/spark/sql/vectorized/package-summary.html
- Spark `ColumnarBatch` JavaDoc:
  https://spark.apache.org/docs/3.5.6/api/java/org/apache/spark/sql/vectorized/ColumnarBatch.html
- Arrow Flight Java guide:
  https://arrow.apache.org/java/current/flight.html
- Arrow `FlightClient` JavaDoc:
  https://arrow.apache.org/java/main/reference/org.apache.arrow.flight.core/org/apache/arrow/flight/FlightClient.html
- Arrow `FlightClient.Builder` JavaDoc:
  https://arrow.apache.org/java/main/reference/org.apache.arrow.flight.core/org/apache/arrow/flight/FlightClient.Builder.html
- Arrow `CallHeaders` JavaDoc:
  https://arrow.apache.org/java/current/reference/org.apache.arrow.flight.core/org/apache/arrow/flight/CallHeaders.html
- Arrow `FlightClientMiddleware` JavaDoc:
  https://arrow.apache.org/docs/dev/java/reference/org/apache/arrow/flight/FlightClientMiddleware.html
