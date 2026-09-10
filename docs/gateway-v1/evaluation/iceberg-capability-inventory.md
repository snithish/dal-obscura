# Iceberg capability inventory

Recorded: 2026-09-10.

Runtime lock currently resolves `pyiceberg` 0.11.1 and `pyarrow` 23.0.1.
Gateway execution uses PyIceberg `StaticTable`, `Table.scan().plan_files()` and
`ArrowScan.to_record_batches()`; it does not implement a second Parquet read
path.

| Capability | Runtime status | Evidence / rule |
| --- | --- | --- |
| Iceberg format v2 file planning and execution | bounded implementation | `IcebergTableFormat` plans native file tasks and executes them with `ArrowScan`. Unit regressions cover projection, residual-filter separation, multi-file task fan-out, and append-after-plan snapshot pinning. Delete differential fixtures are still required. |
| Positional/equality deletes | unknown | Delegated to `ArrowScan`; no gateway differential fixture exists. Treat as a W07 blocker. |
| Nested field IDs | bounded implementation | Native test proves `StaticTable`/`ArrowScan` schema preserves top-level and nested `PARQUET:field_id` metadata while reading a nested projection. Schema evolution/rebinding across snapshots remains unproven and blocks W07. |
| Iceberg format v3 | rejected | Current code rejects v3 because no native conformance inventory or differential fixture proves v3 scan/delete semantics. |
| Pinned snapshot replay | bounded implementation | Native test plans two files, appends a later snapshot, then proves planned tasks emit only original rows. It still depends on retained trusted internal task serialization; full immutable plan metadata is a W06/W07 blocker. |
| Sub-file splitting | unsupported | Planner exposes native file tasks only. Large single files remain one task; no raw-Parquet workaround is used. |

Unsupported or unknown capability never becomes an implicit compatibility
claim. Completing W07 requires real native scan differential fixtures for
snapshot pinning, deletes, nested field IDs and a selected production REST
catalog/object-store path.
