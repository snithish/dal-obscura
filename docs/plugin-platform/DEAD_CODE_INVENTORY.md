# Dead-code and compatibility inventory

This inventory is the X21 deletion gate. It records the source evidence used
before removing a module or dependency. A module is a deletion candidate only
when its callers, packaging entry points, documentation references, and
compatibility obligations have all been checked. Static search is evidence for
review, not proof that an optional entry point is safe to remove.

The inventory deliberately preserves the trusted pickle boundary. The Iceberg
format and the shared scan-task contracts are part of the serialized ticket
compatibility surface; this work does not rename, move, or edit those classes.

## Reviewed modules

| Module | Observed callers and entry points | Compatibility impact | Disposition | Next proof before any removal |
| --- | --- | --- | --- | --- |
| `src/dal_obscura/common/config_store/plugin_bindings.py` | `common/config_store/cli.py` imports `inspect_plugin_bindings` and `apply_plugin_bindings`; migration CLI tests exercise the dry-run/apply path. | Additive plugin-binding migration state; no ticket or pickle payload. | **KEEP** | Exercise migration against a populated PostgreSQL store and retain restart/rollback evidence. |
| `src/dal_obscura/common/catalog/ports.py` | `data_plane/infrastructure/adapters/catalog_registry.py`, `tests/infrastructure/adapters/test_catalog_registry.py`, `tests/support/use_cases.py`, and public plugin bridges use `CatalogPlugin`, `CatalogTableDescriptor`, `TableFormat`, and listings. | Public core port used to adapt independently packaged catalogs; changing paths would break plugin imports. | **KEEP-COMPATIBILITY** | Version the public port only through an explicit SDK migration and independent wheel test. |
| `src/dal_obscura/common/table_format/ports.py` | Iceberg, public plugin adapter, data-plane fetch, catalog registry, support fixtures, benchmarks, and ticket compatibility fixtures import `InputPartition`, `ScanTask`, and `Plan`. | Trusted scan-task classes are serialized in the existing ticket/pickle path. | **KEEP-PICKLE** | Preserve import paths and run the X00 fixture round-trip plus mixed-version upgrade lane. |
| `src/dal_obscura/data_plane/infrastructure/adapters/public_plugin_adapter.py` | Lazily imported by `catalog_registry.py`; conformance and adapter tests import its descriptors and execution bridge. | Security boundary for third-party SDK output, schema bounds, cancellation, and Arrow validation. | **KEEP** | Real independently built plugin wheels and provider failure-injection conformance. |
| `src/dal_obscura/data_plane/infrastructure/table_formats/iceberg.py` | Built-in catalog registry, data-plane CLI startup, control-plane schema/discovery paths, Flight tests, benchmarks, and Iceberg support fixtures. | `IcebergInputPartition` and `IcebergTableFormat` remain trusted serialized task classes. | **KEEP-PICKLE** | Run clean-artifact Iceberg and mixed-worker compatibility tests; do not move or rename classes. |
| `src/dal_obscura/connectors/python_sdk.py` | Exported from `connectors/__init__.py`; Python SDK, DuckDB, Polars, consumer, and nested-value tests import it. | Advertised consumer API and the reference Arrow/DuckDB interoperability path. | **KEEP-PUBLIC-API** | Execute the TLS/OIDC Python and DuckDB consumer matrix for every released SDK version. |
| `src/dal_obscura/flight/v1/read_pb2.py` | Imported by `common/flight_contract.py`, Flight request contracts, and streaming tests. Generated protobuf transport module. | Wire compatibility for Flight clients; generated names must remain stable for the protocol version. | **KEEP-GENERATED** | Regenerate only from the pinned `.proto` source and compare descriptors in protocol CI. |

## Already removed or excluded paths

The former top-level `dal_obscura.application`, `dal_obscura.domain`,
`dal_obscura.infrastructure`, and `dal_obscura.interfaces` packages are
intentionally absent and are guarded by
`tests/architecture/test_package_boundaries.py::test_legacy_root_packages_are_removed`.
They are not candidates for another cleanup pass. The legacy Delta/Avro/Unity
runtime branches are outside the admitted Iceberg/plugin scope and must remain
absent; adding compatibility shims for them would weaken the release boundary.

No module currently has sufficient evidence for deletion. In particular,
absence from a direct import search cannot discharge optional entry points,
generated protocol code, public SDK callers, or old ticket fixtures. The next
cleanup pass may remove a module only after this table is updated with zero
callers, packaging/docs checks, focused behavioral coverage, and a separate
atomic commit.

## Repeatable review procedure

1. Search `src`, `tests`, `docs`, package metadata, CI, and deployment files for
   the module path and exported symbols.
2. Inspect optional entry points and generated artifacts, not only static
   imports.
3. Check the X00 ticket fixture manifest and pickle import paths before moving
   any serialized class.
4. Add or retain a focused behavioral test, run the relevant fast lane, and
   record the exact command and artifact identity in `STATUS.md`.
5. Delete only in a standalone conventional-commit unit; update this inventory
   and the rollback note in the same change.

