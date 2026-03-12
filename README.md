# dal-obscura

Data access layer with Arrow Flight, Iceberg, masking, and row filters.

## Highlights
- Arrow Flight plan/ticket flow with HMAC-signed tickets
- Iceberg v2/v3 backend (pyiceberg)
- DuckDB-powered row filters and column masks
- YAML/JSON policy-driven authz

## Requirements
- Python 3.10+
- `uv` for package management

## Quickstart (uv)

```bash
uv venv
uv sync
uv run dal-obscura --help
```

## Policy Example

```yaml
version: 1
datasets:
  - table: "catalog.db.table"
    rules:
      - principals: ["user1", "group:analyst"]
        columns: ["id", "email", "user.address.zip"]
        masks:
          email: { type: "redact", value: "***" }
          "user.address.zip": { type: "hash" }
        row_filter: "region = 'us'"
```

## Running

```bash
uv run dal-obscura \
  --policy policy.yaml \
  --ticket-secret supersecret \
  --catalog my_catalog \
  --catalog-options '{"uri": "http://catalog:8181"}' \
  --api-keys '{"apikey123": "user1"}'
```

## Development

```bash
uv sync --dev
uv run pytest
uv run ruff check .
uv run ruff format .
uv run ty
```

## Notes
- Mask expressions are executed in DuckDB SQL.
- Nested field masks use DuckDB `struct_update` to update nested structs.

## Logging
- JSON logs by default. Override with `--log-plain`.
- Configure level via `--log-level` or `DAL_OBSCURA_LOG_LEVEL`.
- Configure JSON via `DAL_OBSCURA_LOG_JSON=true|false`.
