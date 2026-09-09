# W00 baseline and removal inventory

Recorded 2026-09-09 at commit `7cbf2c923910abc9b22f97d5f9f6574db595d435`.

## Environment and commands

- Python 3.12.10; uv 0.7.5.
- JVM connector build declares Java 11/Spark 3.5.6 by default, with an
  optional Java 17/Spark 4.0.2 profile.
- The repository pre-commit suite (`ruff format`, `ruff check`, `ty check`,
  non-heavy pytest) passed for each implementation commit above.
- A direct `pytest -q --ignore=tests/benchmarks` run was blocked for 30
  Flight/health/e2e tests because this execution sandbox denies local socket
  binds. The failing error is `Operation not permitted` at `0.0.0.0:0`; it is
  an environment limitation, not a changed assertion. Non-socket tests ran.

## Retained pilot surface

- Iceberg catalog/table format, Arrow Flight, DuckDB final enforcement,
  OIDC JWT validation, SQL-backed ticket store, Python/DuckDB client, and JVM
  Java/Spark consumers.

## Scheduled removal inventory (W10)

- `data_plane/infrastructure/table_formats/delta.py`
- `data_plane/infrastructure/table_formats/files.py`
- `data_plane/infrastructure/adapters/unity_catalog.py`
- File/Delta/Unity construction paths in `catalog_registry.py` and
  `published_config.py`
- Delta/Avro dependencies in `pyproject.toml`
- React control-plane authoring UI and web authoring API after the operator CLI
  replacement exists
- Legacy identity providers and dynamic module loading after fixed OIDC/runtime
  configuration replaces their entry points

The ticket serializer is intentionally excluded from this deletion inventory
for now by the owner’s explicit instruction.

## Finding-to-package ledger

| Finding | Package | Current status |
| --- | --- | --- |
| Parent mask bypass | W01/W03 | Regression and interim fix committed. |
| Changed group can use old ticket | W01/W04 | Regression and interim fix committed. |
| Invalid masks disappear | W01/W02/W03 | Regression and compiler rejection committed. |
| Asset physical binding ignored | W01/W05 | Regression and binding fix committed. |
| Python SDK materializes batches | W01/W08 | Pending. |
| Per-ticket writes | W06 | Pending. |
| Unsupported backends/UI/plugins | W10 | Admission narrowed; deletion pending replacement entry paths. |
