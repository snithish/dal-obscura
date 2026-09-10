# Iceberg capability inventory

Recorded: 2026-09-10.

Runtime lock currently resolves `pyiceberg` 0.11.1 and `pyarrow` 23.0.1.
Gateway execution uses PyIceberg `StaticTable`, `Table.scan().plan_files()` and
`ArrowScan.to_record_batches()`; it does not implement a second Parquet read
path.

| Capability | Runtime status | Evidence / rule |
| --- | --- | --- |
| Iceberg format v2 file planning and execution | bounded implementation | `IcebergTableFormat` plans native file tasks and executes them with `ArrowScan`. Existing unit regressions cover projection and residual-filter separation. Native snapshot/delete differential fixtures are still required. |
| Positional/equality deletes | unknown | Delegated to `ArrowScan`; no gateway differential fixture exists. Treat as a W07 blocker. |
| Schema evolution and field-ID rebinding | unknown | Canonical request paths can bind Arrow field-ID metadata, but planner tickets do not yet carry immutable schema/snapshot context. Treat as a W07 blocker. |
| Iceberg format v3 | rejected | Current code rejects v3 because no native conformance inventory or differential fixture proves v3 scan/delete semantics. |
| Pinned snapshot replay | unknown | Current native task serialization is retained by owner instruction. It does not yet prove immutable snapshot replay. Treat as a W07 blocker. |
| Sub-file splitting | unsupported | Planner exposes native file tasks only. Large single files remain one task; no raw-Parquet workaround is used. |

Unsupported or unknown capability never becomes an implicit compatibility
claim. Completing W07 requires real native scan differential fixtures for
snapshot pinning, deletes, nested field IDs and a selected production REST
catalog/object-store path.
