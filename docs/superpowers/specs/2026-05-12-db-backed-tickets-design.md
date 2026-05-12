# DB-Backed Tickets Design

## Goal

Prevent untrusted client-supplied ticket contents from becoming the source of
executable scan instructions during `do_get`.

Tickets should still carry the current payload for debugging, but the data
plane must treat the database record as authoritative before deserializing any
scan work. A ticket can be exchanged for data at most a configured number of
times, scoped by the data plane cell.

## Problem

The current Flight ticket is an HMAC-signed JSON payload. During
`FetchStreamUseCase.execute()`, the service verifies that signature and expiry,
then base64-decodes and `pickle.loads()` the embedded `scan.read_payload`.

That creates the wrong trust boundary. A signed payload is still client-supplied
input at fetch time. If a signing key leaks, if an old ticket is replayed too
often, or if a bug signs malformed executable content, `do_get` can deserialize
work that was not loaded from server-owned state for this cell.

## Non-Goals

- Do not remove the existing ticket fields from the client-visible ticket.
- Do not make the data plane call the control-plane HTTP API during reads.
- Do not introduce durable state outside the existing config-store database.
- Do not redesign table-format task serialization in this change.
- Do not keep compatibility with legacy tickets that lack `ticket_id`.
- Do not add tests that only assert field plumbing without protecting a
  security boundary.

## Decision

Add a `ticket_id` to `TicketPayload` and persist every planned ticket in a
cell-scoped database table before returning it to the client.

The client ticket remains a signed debug envelope containing the same planning
information as today plus `ticket_id`. During `do_get`, the data plane verifies
the signed envelope, looks up the database row by `(cell_id, ticket_id)`,
compares a canonical SHA-256 hash of the signed payload against the stored
hash, reserves one exchange, and then uses the canonical database payload for
execution.

This creates a two-key trust model:

- A client needs a valid signed ticket.
- The signed ticket must match an unexpired, non-exhausted row owned by the
  current data-plane cell.

Neither the client ticket nor the database row is sufficient by itself.

## Runtime Configuration

Extend cell runtime ticket settings with `max_exchanges`.

Authoring runtime settings should contain:

- `ticket_ttl_seconds`
- `max_tickets`
- `max_ticket_exchanges`
- `path_rules`

Published runtime settings should expose:

```json
{
  "ticket": {
    "ttl_seconds": 900,
    "max_tickets": 64,
    "max_exchanges": 1
  }
}
```

`max_exchanges` is a cell-level setting because ticket minting and ticket
exchange both happen in the data plane configured for a specific `cell_id`.
The data-plane composition root reads it beside the existing ticket TTL and fan
out settings.

## Data Model

Add a cell-owned ticket table to the shared config-store schema.

### `data_plane_tickets`

| Column | Type | Notes |
| --- | --- | --- |
| `ticket_id` | `uuid primary key` | Random server-generated identifier. |
| `cell_id` | `uuid not null foreign key -> cells.id` | Data-plane cell owner. |
| `tenant_id` | `text not null` | Runtime tenant identifier from the principal. |
| `catalog` | `text null` | Catalog in the planned request. |
| `target` | `text not null` | Target in the planned request. |
| `principal_id` | `text not null` | Principal that planned the ticket. |
| `policy_version` | `integer not null` | Policy version used at planning time. |
| `expires_at` | `integer not null` | Epoch seconds. |
| `max_exchanges` | `integer not null` | Copied from published cell runtime. |
| `exchange_count` | `integer not null default 0` | Number of reserved exchanges. |
| `payload_hash` | `char(64) not null` | SHA-256 over canonical payload JSON. |
| `payload_json` | `json not null` | Canonical server-owned ticket payload. |
| `created_at` | `timestamptz not null` | Insert timestamp. |
| `last_exchanged_at` | `timestamptz null` | Last successful reservation. |

Recommended indexes:

- unique primary key on `ticket_id`
- lookup index on `(cell_id, ticket_id)`
- cleanup index on `(cell_id, expires_at)`
- cleanup index on `(cell_id, exchange_count, max_exchanges)`

`ticket_id` is globally random, but every use-case lookup still filters by
`cell_id`. This preserves the existing cell-based architecture even when cells
share one database.

## Canonical Payload Hash

Add a small common ticket helper for canonical JSON and payload hashing.

Canonical payload JSON should use:

- `sort_keys=True`
- compact separators
- UTF-8 bytes
- only JSON-compatible values from `TicketPayload.to_dict()`

The hash must cover the full payload including `ticket_id`, catalog, target,
columns, scan metadata, policy version, principal, expiry, nonce, and tenant.

`FetchStreamUseCase` compares the hash of the verified signed payload with the
stored `payload_hash` using `hmac.compare_digest`. Any mismatch is unauthorized
and must happen before scan payload decoding.

## Planning Flow

`PlanAccessUseCase` should receive a ticket store port in addition to the ticket
codec.

For each planned task:

1. Generate a random `ticket_id`.
2. Build the existing `TicketPayload` with the same debug information as today
   and the new `ticket_id`.
3. Compute the canonical payload hash.
4. Persist a `data_plane_tickets` row for the configured `cell_id`.
5. Sign and return the payload to Flight.

Ticket persistence must happen before returning Flight endpoints. If
persistence fails, planning fails rather than returning an unredeemable or
client-authoritative ticket.

The use case should also call a scoped cleanup method opportunistically. Cleanup
failure should be logged by the adapter but must not hide a successful planning
result unless the insert itself fails.

## Fetch Flow

`FetchStreamUseCase.execute()` should follow this order:

1. Verify the HMAC ticket and ticket expiry.
2. Reject tickets without `ticket_id`.
3. Authenticate the caller.
4. Load the ticket row by `(cell_id, ticket_id)`.
5. Compare the verified signed payload hash with the stored hash.
6. Re-check principal and tenant against the stored payload.
7. Reserve one exchange atomically.
8. Use the stored database payload, not the client payload, from this point on.
9. Re-check current policy version.
10. Decode the stored scan payload.
11. Execute the scan and apply full row filters and masks.

The exchange is counted after authentication and database hash verification,
but before scan execution begins. This prevents replay-by-aborting and
concurrent stream races from getting more than `max_exchanges` reads.

Failed authentication, invalid signatures, missing rows, cross-cell lookups,
hash mismatches, principal or tenant mismatches, and expired tickets do not
consume an exchange.

## Exchange Reservation

The ticket store should expose one method to load a row by `(cell_id,
ticket_id)` and one transactional method that verifies availability and
reserves an exchange.

For SQLite and other basic SQL backends, this can be implemented as an atomic
conditional update:

```sql
UPDATE data_plane_tickets
SET exchange_count = exchange_count + 1,
    last_exchanged_at = :now
WHERE cell_id = :cell_id
  AND ticket_id = :ticket_id
  AND expires_at >= :now_epoch
  AND exchange_count < max_exchanges
```

After the update, read the row in the same transaction. If zero rows are
updated, treat the ticket as unauthorized. The application calls this only
after signature verification, row lookup, payload hash comparison, and
principal/tenant checks have passed. Stronger database-specific adapters can
later use row locks without changing the application port.

## Cleanup

Expired and exhausted tickets should be removed through a cell-scoped store
method:

```text
delete where cell_id = current_cell_id
  and (expires_at < now or exchange_count >= max_exchanges)
```

Run cleanup in three places:

- opportunistically during planning, before or after inserting new tickets
- after fetch rejects an expired or exhausted ticket, when the adapter can do so
  without weakening the reservation semantics
- through a repository method that can later be called by a control-plane job,
  CLI command, or scheduler

The initial implementation does not need a background worker. It only needs the
store API and opportunistic cleanup so storage growth is bounded under normal
read traffic.

## Security Properties

The implementation must fail closed:

- Missing `ticket_id` is unauthorized.
- Missing database row is unauthorized.
- Row belonging to another `cell_id` is unauthorized.
- Signed payload hash mismatch is unauthorized.
- Expired or exhausted row is unauthorized.
- Database lookup or reservation failure is unauthorized.
- Scan payload deserialization never happens before database validation and
  exchange reservation.

Threats addressed:

- Client-side payload tampering is blocked by HMAC and DB hash comparison.
- Leaked signing keys cannot mint redeemable tickets without a DB row.
- DB-only row injection cannot be redeemed without a matching signed ticket.
- Cross-cell replay is blocked by `(cell_id, ticket_id)` lookup.
- Cross-tenant and cross-principal replay remains blocked by re-auth checks.
- Policy updates continue to invalidate old tickets through the existing policy
  version check.
- Concurrent replay is bounded by atomic exchange reservation.
- Storage growth is bounded by scoped cleanup.

## Testing Strategy

Use TDD for behavior that guards security boundaries, not for trivial field
plumbing.

High-value tests:

- Hashing produces stable canonical hashes and changes when any executable or
  authorization-relevant ticket field changes.
- Planning persists one DB row per returned Flight ticket with the current
  `cell_id`, payload hash, expiry, principal, tenant, target, and
  `max_exchanges`.
- Fetch rejects a legacy signed ticket without `ticket_id`.
- Fetch rejects a validly signed ticket when no DB row exists.
- Fetch rejects a validly signed ticket whose DB row belongs to a different
  `cell_id`.
- Fetch rejects a validly signed ticket when the signed payload hash differs
  from the stored hash, and proves scan payload deserialization is not reached.
- Fetch uses the DB payload, not the client payload, after hash verification.
- Fetch consumes at most `max_exchanges` exchanges, including a focused
  concurrent reservation test at the store level.
- Cleanup deletes expired and exhausted tickets only for the configured cell.
- Control-plane publication carries `max_ticket_exchanges` into published
  runtime ticket settings.

Avoid tests that only assert that dataclass constructors accept a new field or
that a mock was called with values already covered by a higher-level security
test.

## Implementation Touchpoints

Expected files:

- `src/dal_obscura/common/ticket_delivery/models.py`
- `src/dal_obscura/common/config_store/orm.py`
- `src/dal_obscura/control_plane/domain/models.py`
- `src/dal_obscura/control_plane/interfaces/api.py`
- `src/dal_obscura/control_plane/application/provisioning.py`
- `src/dal_obscura/control_plane/application/compiler.py`
- `src/dal_obscura/control_plane/infrastructure/repositories.py`
- `src/dal_obscura/data_plane/application/ports/`
- `src/dal_obscura/data_plane/application/use_cases/plan_access.py`
- `src/dal_obscura/data_plane/application/use_cases/fetch_stream.py`
- `src/dal_obscura/data_plane/infrastructure/adapters/`
- `src/dal_obscura/data_plane/interfaces/cli/main.py`
- focused tests under `tests/application`, `tests/control_plane`, and
  `tests/infrastructure/adapters`

The application layer should depend on a ticket-store port, not SQLAlchemy.
The SQLAlchemy adapter belongs in infrastructure and should receive the
configured `cell_id` at construction.

## Open Implementation Notes

`pickle` remains inside trusted DB payloads for this change. That is acceptable
only because the DB row is now the authoritative server-owned source. A future
hardening pass should replace pickled table-format task payloads with explicit
JSON-safe task descriptors per table format.

The ticket table is operational state. This is an explicit exception to the
previously stateless serving rule because replay prevention requires persistent
state. It remains bounded, cell-scoped, and derived from planned reads rather
than becoming user data storage.
