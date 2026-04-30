# API-First Control Plane Design

## Summary

dal-obscura will split into a write-side control plane and a read/serve-side data
plane.

The control plane is a separate FastAPI service that provisions, validates,
publishes, and rolls back DAL configuration. It stores state in an RDBMS-oriented
schema.

The data plane remains the Arrow Flight service. It no longer loads YAML. It
bootstraps from environment variables, reads published configuration directly
from the shared RDBMS by `cell_id`, and keeps the last good in-memory view so
existing reads can continue through transient control-plane or DB read failures.

This follows AWS Well-Architected control-plane/data-plane guidance: the control
plane provides administrative APIs, while the data plane provides the primary
service function and should continue operating for existing resources when the
control plane is impaired.

Reference: https://docs.aws.amazon.com/wellarchitected/latest/reducing-scope-of-impact-with-cell-based-architecture/control-plane-and-data-plane.html

## Goals

- Replace YAML-managed DAL configuration with an API-first control plane.
- Remove YAML fixtures as well as `app.yaml`, `catalogs.yaml`, `policies.yaml`,
  and generated runtime YAML.
- Make the control plane the only provisioning path for tests, examples,
  benchmarks, and connector testkits.
- Keep data-plane serving independent from the control-plane HTTP service.
- Support 100k+ data assets in a SaaS setting without requiring each data-plane
  instance to load every asset into memory.
- Preserve current authorization semantics: masks and row filters remain DuckDB
  SQL expressions, deny rules cannot carry masks or row filters, and tickets are
  invalidated when a dataset policy version changes.
- Keep the service stateless from the request-serving perspective. Persistent
  state is added only to the control-plane/config-store side, as approved.

## Non-Goals

- Do not build a full admin UI in this change.
- Do not store secret values in the control-plane database.
- Do not implement fine-grained control-plane RBAC beyond a first admin
  authentication mechanism.
- Do not add data-plane writes to the configuration database.
- Do not migrate to a separate repository.
- Do not support every future backend in the first implementation; preserve the
  existing Iceberg/file-backed behavior and validation boundaries.

## Key Decisions

- Control plane is a separate FastAPI service in this repository.
- Configuration state is stored in an RDBMS-oriented schema. The first
  implementation should target Postgres-compatible semantics and use SQLite in
  tests where possible.
- SQLAlchemy 2 should be used for RDBMS access, with migration support through
  Alembic or an equivalent migration layer.
- Secrets are stored as references only, for example `env:NAME` or future
  external secret manager references. Secret values stay in environment
  variables or a dedicated secrets manager.
- The data plane reads the RDBMS directly. It does not call the control-plane
  HTTP API at runtime.
- Data planes are scoped by `cell_id` through deployment configuration. Local
  development and examples use a single `default` cell.
- Tenant identity is resolved from authenticated principal attributes. Local and
  single-tenant examples use a `default` tenant.
- Published configuration uses scalable snapshot semantics: an immutable
  publication manifest plus indexed compiled rows, not one giant JSON blob.

## Architecture

### Control Plane

The control plane owns all writes:

- Create and update tenants and cells.
- Assign tenants to cells.
- Provision auth providers.
- Provision catalogs and assets.
- Provision policies, masks, row filters, and path/schema inference rules.
- Validate draft resources.
- Compile draft resources into immutable published rows.
- Activate and roll back publications by atomically updating a cell's active
  publication pointer.

The FastAPI service is the only administrative interface for DAL configuration.
Tests and examples should use the API directly through `TestClient` or a running
control-plane process.

### Data Plane

The data plane keeps the Arrow Flight API and existing application use cases:

- `get_schema`
- `get_flight_info`
- `do_get`

It changes only how runtime adapters are wired. Instead of loading YAML, it:

1. Reads process bootstrap settings from environment variables.
2. Resolves the active publication for its configured `cell_id`.
3. Resolves the requested asset by `tenant_id`, `catalog`, and `target`.
4. Adapts published rows into existing authorization, catalog, identity,
   ticket, masking, and row-transform ports.

Bootstrap configuration that remains outside the control plane:

- Database URL.
- Data-plane `cell_id`.
- Flight bind address.
- Logging level/format.
- TLS cert/key references.
- Secret-provider wiring.
- Control-plane admin token or equivalent admin auth bootstrap.

## Data Model

The model has two sides:

- Authoring tables: mutable normalized resources owned by the control plane.
- Published tables: immutable, compiled, indexed rows consumed by the data
  plane.

### Entity Relationships

```text
tenants 1..n <-> n..1 cells through cell_tenants

cells 1 -> n catalogs
tenants 1 -> n catalogs
catalogs 1 -> n assets
assets 1 -> n policy_rules

cells 1 -> n auth_providers
cells 1 -> 1 cell_runtime_settings

cells 1 -> n config_publications
cells 1 -> 1 active_publications
config_publications 1 -> n published_catalogs
config_publications 1 -> n published_assets
config_publications 1 -> 1 published_cell_runtime
```

### Authoring Tables

#### `tenants`

| Column | Type | Example |
| --- | --- | --- |
| `id` | `uuid primary key` | `018f...a9` |
| `slug` | `varchar(120) unique not null` | `acme` |
| `display_name` | `text not null` | `Acme Analytics` |
| `status` | `varchar(24) not null` | `active` |
| `created_at` | `timestamptz not null` | `2026-04-29T10:00:00Z` |

#### `cells`

| Column | Type | Example |
| --- | --- | --- |
| `id` | `uuid primary key` | `0f2b...71` |
| `name` | `varchar(120) unique not null` | `cell-eu-1` |
| `region` | `varchar(64) not null` | `eu-west-1` |
| `status` | `varchar(24) not null` | `active` |
| `created_at` | `timestamptz not null` | `2026-04-29T10:00:00Z` |

#### `cell_tenants`

| Column | Type | Example |
| --- | --- | --- |
| `cell_id` | `uuid foreign key -> cells.id` | `0f2b...71` |
| `tenant_id` | `uuid foreign key -> tenants.id` | `018f...a9` |
| `shard_key` | `varchar(120) not null` | `default` |

Primary key: `(cell_id, tenant_id)`.

#### `cell_runtime_settings`

| Column | Type | Example |
| --- | --- | --- |
| `cell_id` | `uuid primary key foreign key -> cells.id` | `0f2b...71` |
| `ticket_ttl_seconds` | `integer not null` | `900` |
| `max_tickets` | `integer not null` | `64` |
| `path_rules_json` | `jsonb not null` | `[{"glob": "s3://raw/*", "sample_rows": 20000}]` |

#### `catalogs`

| Column | Type | Example |
| --- | --- | --- |
| `id` | `uuid primary key` | `74a1...8d` |
| `cell_id` | `uuid foreign key -> cells.id` | `0f2b...71` |
| `tenant_id` | `uuid foreign key -> tenants.id` | `018f...a9` |
| `name` | `varchar(160) not null` | `analytics` |
| `module` | `text not null` | `dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog` |
| `options_json` | `jsonb not null` | `{"type": "sql", "uri": "sqlite:///..."}` |

Unique key: `(cell_id, tenant_id, name)`.

#### `assets`

| Column | Type | Example |
| --- | --- | --- |
| `id` | `uuid primary key` | `91da...c4` |
| `cell_id` | `uuid foreign key -> cells.id` | `0f2b...71` |
| `tenant_id` | `uuid foreign key -> tenants.id` | `018f...a9` |
| `catalog_id` | `uuid foreign key -> catalogs.id` | `74a1...8d` |
| `target` | `text not null` | `default.users` |
| `backend` | `varchar(48) not null` | `iceberg` |
| `table_identifier` | `text null` | `prod.users` |
| `options_json` | `jsonb not null` | `{"format": "iceberg"}` |

Unique key: `(cell_id, tenant_id, catalog_id, target)`.

#### `policy_rules`

| Column | Type | Example |
| --- | --- | --- |
| `id` | `uuid primary key` | `a37b...21` |
| `asset_id` | `uuid foreign key -> assets.id` | `91da...c4` |
| `ordinal` | `integer not null` | `10` |
| `effect` | `varchar(16) not null` | `allow` |
| `principals_json` | `jsonb not null` | `["group:analyst"]` |
| `when_json` | `jsonb not null` | `{"tenant": "acme"}` |
| `columns_json` | `jsonb not null` | `["id", "email"]` |
| `masks_json` | `jsonb not null` | `{"email": {"type": "email"}}` |
| `row_filter_sql` | `text null` | `region = 'us'` |

Unique key: `(asset_id, ordinal)`.

#### `auth_providers`

| Column | Type | Example |
| --- | --- | --- |
| `id` | `uuid primary key` | `10b2...ff` |
| `cell_id` | `uuid foreign key -> cells.id` | `0f2b...71` |
| `ordinal` | `integer not null` | `1` |
| `module` | `text not null` | `dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider` |
| `args_json` | `jsonb not null` | `{"issuer": "https://...", "audience": "dal-obscura"}` |
| `enabled` | `boolean not null` | `true` |

Unique key: `(cell_id, ordinal)`.

### Published Tables

#### `config_publications`

| Column | Type | Example |
| --- | --- | --- |
| `id` | `uuid primary key` | `pub-01` |
| `cell_id` | `uuid foreign key -> cells.id` | `0f2b...71` |
| `schema_version` | `integer not null` | `1` |
| `status` | `varchar(24) not null` | `published` |
| `manifest_hash` | `char(64) not null` | `sha256...` |
| `created_at` | `timestamptz not null` | `2026-04-29T10:05:00Z` |

Index: `(cell_id, created_at)`.

#### `active_publications`

| Column | Type | Example |
| --- | --- | --- |
| `cell_id` | `uuid primary key foreign key -> cells.id` | `0f2b...71` |
| `publication_id` | `uuid foreign key -> config_publications.id` | `pub-01` |
| `activated_at` | `timestamptz not null` | `2026-04-29T10:06:00Z` |

This table is the atomic rollout/rollback pointer.

#### `published_cell_runtime`

| Column | Type | Example |
| --- | --- | --- |
| `publication_id` | `uuid primary key foreign key -> config_publications.id` | `pub-01` |
| `auth_chain_json` | `jsonb not null` | `{"providers": [...]}` |
| `ticket_json` | `jsonb not null` | `{"ttl_seconds": 900, "max_tickets": 64}` |
| `path_rules_json` | `jsonb not null` | `[{"glob": "s3://raw/*"}]` |

#### `published_catalogs`

| Column | Type | Example |
| --- | --- | --- |
| `publication_id` | `uuid foreign key -> config_publications.id` | `pub-01` |
| `tenant_id` | `uuid foreign key -> tenants.id` | `018f...a9` |
| `catalog` | `varchar(160) not null` | `analytics` |
| `config_json` | `jsonb not null` | `{"module": "...IcebergCatalog", "options": {...}}` |

Primary key: `(publication_id, tenant_id, catalog)`.

#### `published_assets`

| Column | Type | Example |
| --- | --- | --- |
| `publication_id` | `uuid foreign key -> config_publications.id` | `pub-01` |
| `tenant_id` | `uuid foreign key -> tenants.id` | `018f...a9` |
| `catalog` | `varchar(160) not null` | `analytics` |
| `target` | `text not null` | `default.users` |
| `backend` | `varchar(48) not null` | `iceberg` |
| `compiled_config_json` | `jsonb not null` | `{"catalog": {...}, "target": {...}, "policy": {...}}` |
| `policy_version` | `bigint not null` | `931287432` |

Primary key: `(publication_id, tenant_id, catalog, target)`.

This is the hot data-plane lookup table. Every query should include
`publication_id`, `tenant_id`, `catalog`, and `target`.

### Example Published Asset

```json
{
  "publication_id": "pub-01",
  "tenant_id": "018f-acme",
  "catalog": "analytics",
  "target": "default.users",
  "backend": "iceberg",
  "policy_version": 931287432,
  "compiled_config_json": {
    "catalog": {
      "module": "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog",
      "options": {
        "type": "sql",
        "uri": "sqlite:////runtime/spark_catalog.db",
        "warehouse": "/runtime/warehouse"
      }
    },
    "target": {
      "backend": "iceberg",
      "table": "prod.users"
    },
    "policy": {
      "rules": [
        {
          "principals": ["group:analyst"],
          "columns": ["id", "email", "region"],
          "masks": {
            "email": { "type": "email" }
          },
          "row_filter": "region = 'us'"
        }
      ]
    }
  }
}
```

## API Surface

Initial control-plane API resources:

- `POST /v1/tenants`
- `POST /v1/cells`
- `PUT /v1/cells/{cell_id}/tenants/{tenant_id}`
- `PUT /v1/tenants/{tenant_id}/cells/{cell_id}/catalogs/{name}`
- `PUT /v1/tenants/{tenant_id}/cells/{cell_id}/assets/{catalog}/{target}`
- `PUT /v1/assets/{asset_id}/policy-rules`
- `PUT /v1/cells/{cell_id}/auth-providers`
- `PUT /v1/cells/{cell_id}/runtime-settings`
- `POST /v1/cells/{cell_id}/publications`
- `POST /v1/cells/{cell_id}/publications/{publication_id}/activate`
- `POST /v1/cells/{cell_id}/rollback`

API payloads use JSON only. There is no YAML import path.

## Publish Flow

1. Control plane reads draft authoring resources for one `cell_id`.
2. It validates strict schemas, module paths, backend values, secret references,
   policy rule shapes, masks, and DuckDB SQL row filters.
3. It compiles draft resources into immutable published rows:
   `published_cell_runtime`, `published_catalogs`, and `published_assets`.
4. It writes a `config_publications` row with a `manifest_hash`.
5. It activates the publication by updating `active_publications(cell_id)` in a
   single DB transaction.
6. Rollback changes the active pointer to an earlier valid publication.

Published rows are immutable. Later draft edits do not change previously
published rows.

## Data-Plane Read Flow

For a Flight request:

1. Authenticate the request.
2. Resolve `tenant_id` from principal attributes, defaulting to `default` only
   in local/single-tenant examples.
3. Read or reuse cached `active_publications(cell_id)`.
4. Read or reuse cached `published_cell_runtime(publication_id)`.
5. Read or reuse cached `published_assets(publication_id, tenant_id, catalog, target)`.
6. Adapt the compiled row into the current catalog and authorization ports.
7. Run the existing use case flow for schema, planning, ticket minting, and
   streaming.

For `do_get`, the ticket's `policy_version` is compared against the current
published asset row for the ticket's target. A mismatch rejects the ticket.

## Error Handling

### Control Plane

- Unknown fields produce typed validation errors.
- Unsupported backend values are rejected before publish.
- Invalid catalog/auth provider module paths are rejected before publish.
- Invalid DuckDB SQL row filters are rejected before activation.
- Deny rules with masks or row filters are rejected.
- Secret values are never returned in API responses because they are not stored.
- Publish failures leave the current active publication unchanged.

### Data Plane

- Startup fails if no active publication exists for the configured `cell_id`.
- After successful startup, transient DB read failures use last-good in-memory
  config for already cached runtime/asset rows.
- Requests for unknown catalog/target return the existing unknown target style
  error.
- Logs include `cell_id`, `publication_id`, `tenant_id`, `catalog`, and
  `target`, but never secret values.

## Tests

Tests should be written first for meaningful behavior, regression risk, and
production failure modes. Do not add trivial tests that only assert that a model
exists, a route returns any response, or a getter returns a stored value.

High-signal tests:

- Compiler publishes immutable rows from draft resources.
- Publish activation is atomic and rollback restores the previous active
  publication.
- Invalid row-filter SQL is rejected before activation.
- Deny rules with masks or row filters are rejected.
- Per-asset `policy_version` changes when ABAC clauses, masks, columns, or row
  filters change.
- Data-plane lookup always includes `cell_id`, `tenant_id`, `publication_id`,
  `catalog`, and `target`.
- Data plane does not load all assets for a cell.
- Data plane uses last-good cached config after transient DB read failure.
- `do_get` rejects tickets after a published policy version changes.
- Examples, e2e tests, benchmarks, Python SDK tests, and JVM connector testkits
  provision through the control-plane API and do not write YAML.

## Migration Plan

This is an approved breaking change.

Remove:

- `--app-config`.
- `app_config.py` YAML loader.
- `service_config.py` YAML loader, while preserving or replacing useful runtime
  dataclasses.
- `policy_file_authorizer.py` YAML loader, replacing it with a published-config
  authorizer.
- Generated `app.yaml`, `catalogs.yaml`, and `policies.yaml`.
- Example `fixture/fixture.yaml`, `config/auth.yaml`, and
  `config/transport.yaml`.
- Test helpers that write YAML.
- Connector fixture builders that emit YAML.
- PyYAML dependency after no remaining code path needs it.

Add:

- `dal-obscura-control-plane` console entry point.
- FastAPI control-plane interface package.
- RDBMS schema and repository adapters.
- Publication compiler.
- Published-config reader for the data plane.
- Control-plane provisioning helpers for tests/examples/connectors.

Update:

- README runtime documentation.
- Dockerfile comments and example commands.
- Compose examples so setup provisions through the control-plane API and starts
  data plane with DB URL and `cell_id`.
- JVM testkit so it starts/provisions control plane before data plane.
- Benchmarks so they provision DB state programmatically before measuring Flight
  paths.

## Scale Notes

The snapshot design scales because the active publication is a manifest/pointer,
not a monolithic payload. At 100k+ assets:

- Data planes read one active publication row per cell.
- Data planes read one compiled asset row per requested catalog/target.
- Hot assets can be cached independently.
- Rollout and rollback remain atomic pointer updates.
- Old publication retention can be managed by policy without touching active
  rows.

Future SaaS scaling can add cell-scoped or tenant-shard-scoped publications
without changing the data-plane contract.

## Security Notes

- Control-plane API requires admin authentication from day one, likely through a
  bootstrap admin token in the first implementation.
- The first implementation should avoid storing control-plane user identities or
  RBAC policy unless needed by tests and examples.
- Secret references are stored as data, but secret values are not.
- Data-plane DB credentials should be read-only for published tables.
- Control-plane DB credentials are write-capable and should not be shared with
  data-plane deployments.

## Open Implementation Choices

These are implementation choices, not unresolved product requirements:

- Exact SQLAlchemy/Alembic package layout.
- Whether the first control-plane API tests call FastAPI `TestClient` directly
  or run a local HTTP service process for e2e tests.
- Cache invalidation mechanics: polling active publication id, request-time
  freshness check, or database notification where available.
- The exact shape of control-plane admin auth after the bootstrap token.

