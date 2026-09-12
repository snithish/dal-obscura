# Acceptance case registry

This registry maps each acceptance scenario to its owning implementation packet.
It is intentionally a manifest, not a claim that the scenario already passes.
Each row names the first executable test location; later packets may add required
integration, browser, consumer, or production evidence.

| Case | Owner packet | Initial test location | Required evidence |
| --- | --- | --- | --- |
| A01 | X01 | `tests/interfaces/control_plane/test_workspace_api.py` | SQLite + governed Flight |
| A02 | X02 | `tests/interfaces/control_plane/test_schema_api.py` | strict review API |
| A03 | X03 | `tests/integration/control_plane/test_publication_races.py` | PostgreSQL barriers |
| A04 | X03 | `tests/control_plane/test_publication_store.py` | rollback/idempotency |
| A05 | X04 | `tests/interfaces/control_plane/test_schema_api.py` | evaluation/Flight goldens |
| A06 | X04/X05 | `tests/control_plane/test_evaluation_service.py` | nested Arrow goldens |
| A07 | X05 | `tests/control_plane/test_schema_service.py` | canonical digest/bounds |
| A08 | X06 | `tests/control_plane/test_schema_evolution.py` | stale/new ticket reads |
| A09 | X07 | `tests/interfaces/control_plane/test_catalogs_api.py` | import/secret spies |
| A10 | X07 | `tests/integration/test_io_boundary.py` | redirects/DNS/path roots |
| A11 | X08 | `tests/control_plane/test_catalog_discovery.py` | bounded cleanup |
| A12 | X03/X19 | `tests/interfaces/control_plane/test_actor_auth.py` | auth matrix/processes |
| A13 | X09 | `apps/governance-ui/src/*.test.tsx` | deferred response browser tests |
| A14 | X10 | `apps/governance-ui/src/*.test.tsx` | authoring/a11y journeys |
| A15 | X10/X13 | `tests/interfaces/control_plane/test_config_activation.py` | active generation |
| A16 | X12/X14 | `tests/plugin_platform/test_registry.py` | installed wheel admission |
| A17 | X08/X12 | `tests/interfaces/flight/test_service_streaming.py` | task/cancel lifecycle |
| A18 | X16/X17 | `tests/plugin_conformance/test_pairs.py` | independent distributions |
| A19 | X18 | `tests/consumers/test_governed_reads.py` | Python/DuckDB/Spark |
| A20 | X19 | `tests/production/test_local_parity.py` | clean artifacts/TLS/OIDC |
| A21 | X21 | `tests/benchmarks/` | fixed runner thresholds |
| A22 | X20 | `tests/integration/test_recovery_upgrade.py` | restore/rotation/pickle |
| A23 | X23 | CI release manifest | candidate artifact evidence |

When a path does not exist yet, the packet must create it. An absent path is an
open deliverable. Update this table only when ownership changes; record passing
commands and artifacts in [the progress ledger](../../docs/plugin-platform/STATUS.md).
