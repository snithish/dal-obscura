# Agent Guide (dal-obscura)

## Purpose
This repo implements a data access layer for Iceberg tables exposed via Arrow Flight with policy-based masking and row filters.

## Ground Rules
- Keep the service stateless; do not add persistent state without explicit approval.
- Enforce Iceberg format versions v2 or v3 only.
- Masks and row filters must be expressed as DuckDB SQL expressions.
- Avoid unnecessary data copies; prefer Arrow + DuckDB zero-copy paths where possible.
- Follow TDD: add or update tests alongside changes.

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
uv run ty
```

## Repo Map
- `src/dal_obscura/service.py`: Arrow Flight server
- `src/dal_obscura/backend/iceberg.py`: Iceberg backend
- `src/dal_obscura/policy.py`: Policy parsing + authz
- `src/dal_obscura/masking.py`: DuckDB mask expression builder
- `src/dal_obscura/tickets.py`: Ticket schema + HMAC signing
- `tests/`: unit tests

## Common Tasks
- Add a new backend: implement `Backend` in `src/dal_obscura/backend/base.py` and wire into CLI.
- Extend policy: update `Policy` parsing + `resolve_access` and add tests.
- Add a new mask type: update `masking._mask_expression` + tests.
- Tune parallelism: change `--max-tickets` to control ticket fanout.

## Style
- Prefer small, composable functions.
- Keep public APIs stable; document changes in README.
