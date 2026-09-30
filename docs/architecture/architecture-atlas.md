# dal-obscura architecture and request/data flow

Current implementation reviewed 2026-09-30. Open `architecture-atlas.html` for the
interactive atlas with zoom, pan, full-size expansion, and source references.

These diagrams describe the current implementation, not the proposed hardening work.
PostgreSQL owns durable configuration and tickets; workers have bounded ephemeral
provider caches. There is no tenant/cell routing or global generation counter.

## C4 · LEVEL 1 · SYSTEM CONTEXT: The governed access boundary

Who uses the product, and which external systems supply identity, metadata, and data?

```mermaid
flowchart TD
    Analyst(("Data consumer<br/>Analyst / application")):::external
    Steward(("Policy administrator<br/>Owner / delegated editor")):::external
    DAL["dal-obscura<br/>Governed data access system"]:::dp
    IDP["Identity provider<br/>OIDC / JWT issuer + JWKS"]:::external
    Catalog["Catalog systems<br/>Iceberg SQL / REST / manifest"]:::external
    Storage[("Data storage<br/>Parquet / Iceberg files and deletes")]:::external
    Analyst -->|"Plans and reads · Arrow Flight"| DAL
    Steward -->|"Configures and evaluates · governance UI / HTTP API"| DAL
    DAL -->|"Validates identity / browser login"| IDP
    DAL -->|"Resolves tables and scan metadata"| Catalog
    DAL -->|"Reads governed source data"| Storage

classDef cp fill:#f4e1d8,stroke:#ac4e2b,color:#40251c
classDef dp fill:#e1eadf,stroke:#58715a,color:#233b29
classDef store fill:#f4ebd3,stroke:#aa8430,color:#493b19
classDef external fill:#f2f0ec,stroke:#8a8881,color:#353732,stroke-dasharray:5 4
```

The system boundary is one deployment. Tenant/cell routing and the global generation counter are absent. Per-resource revisions still protect writes and identify captured policy.

## C4 · LEVEL 2 · CONTAINERS: Separate governance from execution

Workers are replaceable. Durable configuration, sessions, audit, and ticket state live in the database.

```mermaid
flowchart TD
    Consumer["Consumer clients<br/>Python / DuckDB / Polars / Java / Spark"]:::external
    IDP["Identity provider<br/>JWT + JWKS / OIDC login"]:::external
    Sources["Catalogs and file storage<br/>External metadata + data"]:::external
    subgraph Product["dal-obscura deployment"]
        UI["Governance UI<br/>React + TypeScript / static web assets"]:::cp
        CP["Control plane<br/>FastAPI · authenticated HTTP API"]:::cp
        DP["Data plane<br/>Arrow Flight · Arrow + embedded DuckDB"]:::dp
        DB[("PostgreSQL<br/>Config · policy · sessions · audit · tickets")]:::store
        UI -->|"JSON API / session cookie + CSRF"| CP
        CP -->|"Transactions / revisions / audit / revocation"| DB
        DP -->|"Configuration snapshots / ticket exchanges + revocation"| DB
    end
    Consumer -->|"Arrow Flight / gRPC · TLS in production"| DP
    CP -->|"OIDC login / identity validation"| IDP
    DP -->|"JWT validation / key lookup"| IDP
    CP -->|"Discovery / schema admission"| Sources
    DP -->|"Discovery / planned reads"| Sources

classDef cp fill:#f4e1d8,stroke:#ac4e2b,color:#40251c
classDef dp fill:#e1eadf,stroke:#58715a,color:#233b29
classDef store fill:#f4ebd3,stroke:#aa8430,color:#493b19
classDef external fill:#f2f0ec,stroke:#8a8881,color:#353732,stroke-dasharray:5 4
```

DuckDB is embedded in each data-plane worker, not a separate service. SQLite is supported for development. HTTP edge/TLS termination depends on deployment configuration; the diagram shows logical relationships.

## C4 · LEVEL 3 · CONTROL-PLANE COMPONENTS: Control plane: change governed state

Capabilities, revision checks, transactions, and audit protect the administrative write path.

```mermaid
flowchart TD
    HTTP["HTTP routes + request middleware<br/>Validation · safe errors · request IDs"]:::cp
    Auth["Actor and browser-session boundary<br/>Bearer / OIDC · cookie CSRF · rate limits"]:::cp
    Services["Application services<br/>Catalogs · assets · grants · policy · settings"]:::cp
    Rules["Shared domain models<br/>Policy validation / resolution · field identities"]:::dp
    Repo["Repositories + session stores<br/>Locks · CAS revisions · persisted audit"]:::store
    Plugins["Plugin admission + catalog adapters<br/>Lockfile registry · secret/path/egress checks"]:::dp
    DB[("Configuration database")]:::store
    Providers["External catalogs<br/>Discovery / schemas"]:::external
    HTTP -->|"Authenticated actor"| Auth
    Auth --> Services
    Services -->|"Validate and evaluate"| Rules
    Services -->|"Read / mutate under authority"| Repo
    Repo -->|"SQL transactions"| DB
    Services -->|"Discover / admit schemas"| Plugins
    Plugins --> Providers

classDef cp fill:#f4e1d8,stroke:#ac4e2b,color:#40251c
classDef dp fill:#e1eadf,stroke:#58715a,color:#233b29
classDef store fill:#f4ebd3,stroke:#aa8430,color:#493b19
classDef external fill:#f2f0ec,stroke:#8a8881,color:#353732,stroke-dasharray:5 4
```

Policy evaluation uses synthetic rows through the same SQL masking/filter semantics. Saving policy changes affects future plans; explicit ticket revocation controls outstanding access.

## C4 · LEVEL 3 · DATA-PLANE COMPONENTS: Data plane: authorize, capture, execute

Application use cases depend on ports. Infrastructure adapters implement those ports.

```mermaid
flowchart TD
    Flight["Flight service + middleware<br/>Descriptor parsing / transport errors / Arrow streams"]:::dp
    Plan["PlanAccess / GetSchema use cases<br/>Identity · projection · policy · schema checks"]:::dp
    Fetch["FetchStream use case<br/>Ticket + identity verification / guarded execution"]:::dp
    Identity["Identity adapters<br/>JWT / OIDC claim mapping"]:::dp
    Context["LiveConfig access context<br/>Consistent snapshot + bounded provider lease"]:::store
    Source["Catalog / TableFormat adapters<br/>Admitted plugins · split tasks · Arrow scans"]:::dp
    Tickets["HMAC codec + SQL ticket store<br/>References · payloads · atomic exchange · revocation"]:::store
    Transform["Masking + DuckDB transform<br/>Full row filter · masks · visible projection"]:::dp
    Flight --> Plan
    Flight --> Fetch
    Plan --> Identity
    Plan --> Context
    Context --> Source
    Plan -->|"Store task payloads and sign references"| Tickets
    Fetch --> Identity
    Fetch -->|"Load / reserve / ensure active"| Tickets
    Fetch -->|"Execute captured scan task"| Source
    Source -->|"Arrow input batches"| Transform
    Transform -->|"Governed Arrow batches"| Flight

classDef cp fill:#f4e1d8,stroke:#ac4e2b,color:#40251c
classDef dp fill:#e1eadf,stroke:#58715a,color:#233b29
classDef store fill:#f4ebd3,stroke:#aa8430,color:#493b19
classDef external fill:#f2f0ec,stroke:#8a8881,color:#353732,stroke-dasharray:5 4
```

Fetch does not resolve current policy again. Its execution instructions come from the stored ticket. Live configuration snapshots bind source configuration, schema admission, and policy before provider I/O.

## REQUEST FLOW · GET_FLIGHT_INFO: Request 1: plan access

One configuration snapshot becomes multiple bounded ticket endpoints. Source rows are not streamed during planning.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant F as Flight / PlanAccess
    participant I as Identity
    participant S as LiveConfig snapshot
    participant P as Catalog / TableFormat
    participant T as Ticket DB + HMAC
    C->>F: Descriptor: catalog, target, columns, filter + auth
    F->>I: Authenticate request
    I-->>F: Issuer, subject, groups, attributes, expiry
    F->>S: Open bound access context
    S-->>F: Binding, policy, schema admission from one DB snapshot
    Note over S,P: DB snapshot ends before provider I/O, provider is leased
    F->>P: Resolve source and obtain schema
    P-->>F: Arrow schema
    F->>F: Validate paths and filter dependencies, authorize columns
    F->>F: Combine policy + requested row filters, retain hidden dependencies
    F->>P: Plan parallel scan tasks within max tickets
    P-->>F: Tasks + schema, planner verifies schema consistency
    F->>T: Persist captured policy, task, identity binding and expiry
    T-->>F: Sign opaque references
    F-->>C: Masked output schema + Flight endpoints
    Note over C,T: Client may fetch endpoints concurrently, get_schema omits scan planning and ticket minting
```

The database snapshot is request-local. Provider reuse is bounded and keyed by concrete configuration identity; it is not a policy cache or a generation system.

## REQUEST FLOW · DO_GET: Request 2: fetch governed data

Re-authentication, exchange reservation, and output guards protect each captured read.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant F as Flight / FetchStream
    participant I as Identity
    participant T as SQL ticket store
    participant S as Captured TableFormat
    participant D as DuckDB transform
    C->>F: Signed reference + fresh auth headers
    F->>F: Verify reference signature and expiry
    F->>I: Re-authenticate caller
    I-->>F: Current principal context
    F->>T: Load stored payload
    T-->>F: Payload + hash + exchange state
    F->>F: Check payload integrity, nonce, issuer, subject, context digest
    F->>T: Atomically reserve exchange if active, unexpired, not exhausted
    T-->>F: Reservation succeeds
    F->>F: Decode captured scan task
    F->>S: Execute captured partition
    loop Stream Arrow batches
        S-->>D: Original values + execution columns
        D->>D: Bound batch, full filter then masks/projection
        D-->>F: Governed batch
        F->>F: Check ticket / identity expiry and stream deadline
        F->>T: Ensure ticket is not revoked
        T-->>F: Active
        F-->>C: Emit governed Arrow batch
    end
    Note over F,D: Failure, cancellation or early termination closes upstream iterators
```

Admission occurs once when the lazy transform starts; batch limits apply throughout. Deadline checks occur between yielded batches, not as hard interruption of every blocked provider/query operation.

## DATA FLOW · FILTER BEFORE MASK: Raw data becomes governed Arrow

Backend pushdown is an optimization. The final transform enforces the complete filter on original values.

```mermaid
flowchart LR
    Raw["Source Arrow batch<br/>Original values<br/>Visible + hidden dependencies"]:::store
    Filter["DuckDB WHERE<br/>Complete policy AND<br/>requested filter"]:::dp
    Mask["DuckDB projection<br/>Masks + visible fields<br/>Governed Arrow output"]:::dp
    Output["Flight output boundary<br/>Batch limits / expiry<br/>deadline / revocation"]:::store
    Raw --> Filter --> Mask --> Output

classDef cp fill:#f4e1d8,stroke:#ac4e2b,color:#40251c
classDef dp fill:#e1eadf,stroke:#58715a,color:#233b29
classDef store fill:#f4ebd3,stroke:#aa8430,color:#493b19
classDef external fill:#f2f0ec,stroke:#8a8881,color:#353732,stroke-dasharray:5 4
```

The client receives only governed Arrow batches. WHERE and masking projection are parts of one row-local SQL query; boxes represent semantic order, not separate materialized tables. Hidden filter dependencies are removed from output. Arrow avoids unnecessary copies where supported; zero-copy is not guaranteed across every transform.

## ARCHITECTURE · DEPENDENCY DIRECTION: Ports keep the core independent

Runtime calls go outward through ports; implementation dependencies point toward application and domain contracts.

```mermaid
flowchart TD
    subgraph Outer["Outer adapters"]
        Interfaces["Interfaces<br/>Flight / HTTP / CLI"]:::cp
        Infra["Infrastructure<br/>SQL stores / identity / plugins / DuckDB"]:::dp
    end
    subgraph Core["Application + domain"]
        UseCases["Application use cases / services<br/>Planning · fetch · governance"]:::dp
        Ports["Ports<br/>Identity · access context · tickets · row transform"]:::store
        Domain["Common domain models<br/>Policy · PlanRequest · ScanTask · TicketPayload"]:::store
    end
    Interfaces -->|"Depends on"| UseCases
    UseCases -->|"Depends on contracts"| Ports
    UseCases --> Domain
    Infra -->|"Implements"| Ports
    Infra -->|"Consumes / returns"| Domain

classDef cp fill:#f4e1d8,stroke:#ac4e2b,color:#40251c
classDef dp fill:#e1eadf,stroke:#58715a,color:#233b29
classDef store fill:#f4ebd3,stroke:#aa8430,color:#493b19
classDef external fill:#f2f0ec,stroke:#8a8881,color:#353732,stroke-dasharray:5 4
```

The public plugin SDK is a separate package. Plugins implement SDK contracts and are admitted by locked identity; callers do not select arbitrary module paths.

## C4 · LEVEL 4 · SELECTED CODE RELATIONSHIPS: The captured-read contract

A focused code view of the planning and fetch boundary; intentionally not a complete class inventory.

```mermaid
classDiagram
    class AccessContextPort {
        open(catalog, target) AccessContext
    }
    class AccessContext {
        TableFormat table_format
        AuthorizationPort authorizer
    }
    class TableFormat {
        get_schema() Schema
        plan(request, max_tickets) Plan
        execute(partition) ArrowStream
    }
    class Plan {
        Schema schema
        ScanTask[] tasks
        RowFilter full_row_filter
    }
    class ScanTask {
        TableFormat table_format
        Schema schema
        InputPartition partition
    }
    class TicketPayload {
        asset_id
        principal_id / issuer / identity_context
        columns / scan / policy_version
        ticket_id / nonce / expires_at
    }
    AccessContextPort ..> AccessContext : yields
    AccessContext --> TableFormat : binds
    TableFormat ..> Plan : plans
    Plan "1" *-- "many" ScanTask : contains
    ScanTask --> TableFormat : executes through
    TicketPayload ..> ScanTask : scan contains serialized task
```

Current implementation serializes trusted internal tasks with pickle + base64. Typed data-only descriptors and keyed authentication of complete stored payloads are recommendations, not implemented components.

## Source map

- [Flight transport](../../src/dal_obscura/data_plane/interfaces/flight/server.py)
- [Plan access](../../src/dal_obscura/data_plane/application/use_cases/plan_access.py)
- [Fetch stream](../../src/dal_obscura/data_plane/application/use_cases/fetch_stream.py)
- [Live configuration snapshot and leases](../../src/dal_obscura/data_plane/infrastructure/adapters/live_config.py)
- [DuckDB filtering and masking](../../src/dal_obscura/data_plane/infrastructure/adapters/duckdb_transform.py)
- [Ticket exchange store](../../src/dal_obscura/data_plane/infrastructure/adapters/ticket_store_sqlalchemy.py)
- [Control-plane composition](../../src/dal_obscura/control_plane/interfaces/api.py)
- [Public SDK execution bridge](../../src/dal_obscura/data_plane/infrastructure/adapters/public_plugin_adapter.py)
- [Execution invariants](../../docs/read-execution-invariants.md)
