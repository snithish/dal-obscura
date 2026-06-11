# Concepts

dal-obscura separates policy management from governed reads. The control plane
owns configuration and publication. The data plane performs reads through Arrow
Flight and applies the active policy version.

## System Shape

```mermaid
flowchart TB
    subgraph Users
        admin["Platform admin"]
        owner["Asset owner"]
        reader["Data consumer"]
    end

    subgraph ControlPlane["Control plane"]
        ui["UI"]
        api["HTTP API"]
        repo["Config repository"]
    end

    subgraph DataPlane["Data plane"]
        flight["Arrow Flight service"]
        planner["Planner"]
        executor["Table executor"]
        transform["DuckDB filters and masks"]
    end

    idp["IAM provider"]
    db["Config database"]
    catalog["Catalog"]
    warehouse["Table storage"]

    admin --> ui
    owner --> ui
    ui --> api
    api --> repo --> db
    reader --> flight
    flight --> idp
    flight --> repo
    flight --> planner --> catalog
    executor --> warehouse
    executor --> transform
```

## Core Objects

| Object | Meaning |
| --- | --- |
| Catalog | A configured source that can discover tables. |
| Discovered table | A table found by catalog discovery. |
| Asset | A governed table that owners can manage. |
| Owner | A principal or group allowed to edit policy for an asset. |
| Policy rule | A grant, column selection, row filter, or mask. |
| Policy version | An asset-scoped submitted policy snapshot. |
| Publication | The active policy version used by reads. |
| Ticket | A short-lived opaque reference used by Flight `do_get`. |

The UI is asset-first. Internal runtime details such as tenant or cell IDs are
kept out of normal user workflows.

## Asset Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Discovered: Catalog discovery
    Discovered --> Governed: Promote to asset
    Governed --> Drafting: Edit owners or policy
    Drafting --> Submitted: Publish policy version
    Submitted --> Active: Activate publication
    Active --> Drafting: Start next change
```

## Read Lifecycle

```mermaid
sequenceDiagram
    participant Client
    participant Flight as "Flight service"
    participant IAM
    participant Repo as "Policy repository"
    participant Catalog
    participant Engine as "DuckDB transform"

    Client->>Flight: get_flight_info(request)
    Flight->>IAM: Authenticate principal
    Flight->>Repo: Load active policy version
    Flight->>Catalog: Resolve table and plan scan tasks
    Flight->>Repo: Store scan payload and mint opaque tickets
    Flight-->>Client: Schema and ticket endpoints
    Client->>Flight: do_get(ticket)
    Flight->>IAM: Re-authenticate principal
    Flight->>Repo: Verify ticket and policy version
    Flight->>Catalog: Execute scan tasks
    Flight->>Engine: Apply row filters and masks
    Flight-->>Client: Arrow record batches
```

## Policy Evaluation

Policy rules decide whether a principal can read an asset, which columns are
visible, which row filter applies, and which masks are applied to columns.

Row filters and masks are DuckDB SQL expressions. This keeps policy behavior
close to the execution engine and makes expressions testable.

## Persistence

Use Postgres for persistent control-plane state in shared and deployed
environments. SQLite is useful for local development and tests, but it is not
the recommended datastore when state must survive restarts reliably.
