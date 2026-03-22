# Agent Guide (dal-obscura)

## Purpose
This repo implements a data access layer for Iceberg tables and file-backed datasets exposed via Arrow Flight, with policy-based masking and row filters. The runtime is organized as a hexagonal/clean architecture with explicit ports and adapters.

## Ground Rules
- Keep the service stateless; do not add persistent state without explicit approval.
- Masks and row filters must be expressed as DuckDB SQL expressions.
- Avoid unnecessary data copies; prefer Arrow + DuckDB zero-copy paths where possible.
- Follow TDD: add tests and expectations first, get review from user only then start code implementation.

## How to Run
```bash
uv venv
uv sync
uv run dal-obscura --help
```

## Tests
```bash
uv sync --dev
uv run pytest
uv run ruff check .
uv run ruff format .
uv run ty check
```

## Architecture (Current)
- **Interfaces (transport adapters)**
  - `interfaces/flight/`: Arrow Flight server, header middleware, request parsing, streaming.
  - `interfaces/cli/`: composition root that wires adapters and starts the server.
- **Application (use cases + ports)**
  - `application/use_cases/plan_access.py`: authenticate, authorize, plan, mint tickets.
  - `application/use_cases/fetch_stream.py`: verify ticket, re-auth, execute, stream.
  - `application/ports/`: identity, authorization, ticket, masking, row transforms.
- **Domain (pure models + policies)**
  - `domain/access_control/`: policy models + resolution rules.
  - `domain/query_planning/`: plan request/read spec.
  - `domain/ticket_delivery/`: ticket payload value object.
  - `domain/catalog/`, `domain/format_handler/`: catalog/format ports.
- **Infrastructure (adapters)**
  - Catalog + format registries, policy file authorizer, JWT identity, HMAC ticket codec.
  - Iceberg handler (pyiceberg) and DuckDB file handler.
  - DuckDB masking + row-transform implementation.

### Request Flow
1. `get_flight_info` -> `PlanAccessUseCase`
   - Authenticate via JWT headers.
   - Resolve catalog/target -> `ResolvedTable`.
   - Resolve format handler -> schema + plan tasks.
   - Authorize columns/filters/masks via policy.
   - Mint HMAC-signed tickets with policy version and scan payload.
   - Return masked output schema + ticket endpoints.
2. `do_get` -> `FetchStreamUseCase`
   - Verify ticket signature + expiry.
   - Re-authenticate and confirm principal and policy version.
   - Execute format handler tasks to stream Arrow batches.
   - Apply row filters + masking via DuckDB and stream results.

## Repo Map (Key Files)
- `src/dal_obscura/interfaces/cli/main.py`: composition root / CLI wiring
- `src/dal_obscura/interfaces/flight/server.py`: Flight server adapter
- `src/dal_obscura/interfaces/flight/contracts.py`: header middleware + request parsing
- `src/dal_obscura/application/use_cases/plan_access.py`: planning + ticket minting
- `src/dal_obscura/application/use_cases/fetch_stream.py`: ticket verification + streaming
- `src/dal_obscura/domain/access_control/`: policy models + resolution
- `src/dal_obscura/infrastructure/adapters/`: catalog registry, format registry, masking, etc.
- `tests/`: unit tests

## Common Tasks
- Add a new catalog: implement `CatalogPlugin` (see `domain/catalog/ports.py`) and configure it in the service config.
- Add a new format handler: implement `FormatHandler` and register it in `DynamicFormatRegistry` (entry points or built-in).
- Extend policy: update policy parsing/resolution and add tests.
- Add a new mask type: update `_mask_expression` and any schema adjustments, plus tests.
- Change ticket payloads: update `TicketPayload`, `HmacTicketCodecAdapter`, both use cases, and tests.
- Tune fan-out: adjust `--max-tickets` for ticket planning.

## New Rules (Self-Guidance)
- Keep domain and application layers free of transport concerns (no Flight types in use cases).
- Any new policy feature must update both `authorize()` and `current_policy_version()` paths.
- Ticket content is the single source of truth for `do_get`; never trust client replays of `PlanRequest`.
- Masking changes must update both the DuckDB projection logic and the masked schema logic.
- Catalog resolution must stay deterministic; never mutate shared config or registry state during requests.
- Avoid pickling arbitrary user input; only serialize trusted, internal task payloads.

## Style
- Prefer small, composable functions.
- Keep public APIs stable; document changes in README.
