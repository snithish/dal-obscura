# X00 compatibility baseline

This baseline freezes the trusted ticket/task boundary before plugin work. It is
an inventory and diagnostic record; it is not a release qualification.

| Item | Value |
| --- | --- |
| Baseline source commit | `5208eee9af35e38b5ab8294524a0485611c2b0a7` |
| Inventory commit | recorded in the X00 implementation ledger entry |
| Captured | 2026-09-12, Europe/Amsterdam |
| Host | Darwin 25.6.0, arm64 (`Nithishs-MacBook-Air.local`) |
| Python dependencies | pyarrow 23.0.1; duckdb 1.5.0; pyiceberg 0.11.1; sqlglot 30.2.1; pytest 9.0.2 |
| Fixture command | `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/acceptance/test_x00_compatibility_inventory.py -q` |
| Fixture timing | 0.597 s wall, 0.49 s user, 0.09 s system |

The immutable manifest is [ticket_compat_manifest.json](../../tests/acceptance/fixtures/ticket_compat_manifest.json).
It names the serializer call sites and import paths that existing trusted
pickle-backed task payloads rely on, plus a canonical JSON ticket fixture used to
detect accidental changes to the signed ticket representation. The fixture is
synthetic and contains no customer data or credentials.

The retained pickle path remains in `PlanAccessUseCase.execute`,
`FetchStreamUseCase` scan decoding, and `IcebergTableFormat` planning/execution.
This packet does not move, rewrite, or decode those payloads. Any future change
to those symbols requires a separately reviewed compatibility fixture and an
explicit owner decision.

The X00 acceptance registry maps all A01–A23 cases to their first executable test
or integration gate. Missing test paths remain open work; this baseline does not
turn planned gates into passing claims.
