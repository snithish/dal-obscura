# Remaining-work ledger

Review baseline: c46415282a2a796cf737a9c3b7f7ba941f19bf7b (2026-09-13).
**Release HOLD. Implementation continues; unresolved live gates remain VERIFY.**
Execution authority: [implementation plan](IMPLEMENTATION_PLAN.md).
Previous completion notes: [archived ledger](STATUS_ARCHIVE_20260913.md).

Second review at c6230b1 found no intervening implementation changes. Queue status
is unchanged. F11–F14 refine N01/N02/N03/N09/N11/N12: current operational guidance,
removal of obsolete test/API contracts, bounded audit queries and editor-to-publisher
draft handoff. B05/B13/B15/B16/B19 specify their exact expected outcomes.

## Queue

Only N packets are active. Completed parts of X00–X23 were removed through the
[reconciliation](IMPLEMENTATION_REVIEW.md); do not repeat that implementation.
All N packets below contain remaining changes or qualification, not claims that
their underlying functionality is wholly absent.

- N01 — Baseline/toolchain: READY; B01/B02.
- N02 — Contracts/deletion: OPEN; requires N01; B03/G01–G04.
- N03 — Pairing/mutation contracts: OPEN; requires N02; B04/B05.
- N04 — Secret/IO/resource enforcement: OPEN; requires N02/N03; B06.
- N05 — Identity/normal authentication: OPEN; requires N03; B07/B08.
- N06 — Design foundation/shell: OPEN; requires N01; B09.
- N07 — Async UI/recovery: OPEN; requires N03/N05/N06; B10.
- N08 — Asset/policy authoring: OPEN; requires N03/N06/N07; B11/B12.
- N09 — Review/publication/history: OPEN; requires N07/N08; B13.
- N10 — Connection/plugin lifecycle: OPEN; requires N03/N04/N06/N07; B14.
- N11 — Access/settings/audit/consumers: OPEN; requires N05/N07/N09/N10; B15.
- N12 — Two-process transactional qualification: OPEN; requires N03/N04/N05; B16.
- N13 — Independent packages/live consumers: OPEN; requires N02/N03/N04/N05/N12; B17.
- N14 — Lean tests/capacity: OPEN; requires N01/N07/N08/N10/N12/N13; B18/B19.
- N15 — Deployment/recovery/artifact integrity: OPEN; requires N05/N12/N13/N14; B20/B21.
- N16 — Independent security/UX acceptance: OPEN; requires N01–N15; B22/all G/B.

READY means all prerequisites pass. OPEN means remaining work with prerequisites
not yet complete. ACTIVE means an agent owns a bounded slice. VERIFY means code
exists but required execution or human evidence is missing. DONE requires every
FR/NFR and acceptance subcase to pass on the candidate. Missing access/environment
is recorded explicitly; it is never PASS or a waived skip.

## Evidence inherited, with limits

The historical ledger reports Python/socket-enabled suite and PostgreSQL checks
and a local Python/DuckDB probe. This review did not rerun them. Repository-level
race tests do not establish two-process API races; stub consumer reads and pair
descriptors do not qualify live catalogs; backup helper tests do not establish
timed restore. See F08/F09 and N12/N13/N15. Reuse the existing harnesses.

## Implementation update — c1b4866 (2026-09-13)

- Packet / status / candidate commit / owner: N11 partial / VERIFY / `c1b4866` / control-plane + UI.
- Observable behavior delivered: `/v1/audit/events/page` provides bounded keyset pagination ordered by `(created_at, id)`; non-admin visibility is enforced with database `EXISTS` predicates and asset-specific reads still require the read capability. The existing list endpoint now uses the same scoped query. Activity loads 50 events and can request later pages without an unbounded fetch.
- Changed paths: audit application/repository/service, policy routes, UI API/activity view, ORM and migration `20260913_0016_audit_keyset_index.py`. No pickle serializer, payload class, or import path changed.
- Primary tests: `tests/interfaces/control_plane/test_audit_api.py`, `tests/architecture/test_control_plane_route_inventory.py`, `tests/common/config_store/test_schema_migrations.py`.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_audit_api.py tests/architecture/test_control_plane_route_inventory.py tests/common/config_store -q` — 20 passed. `pnpm --dir apps/governance-ui check` — passed. `ruff check` on changed Python paths — passed. UI production build is VERIFY because local Corepack could not fetch pinned `pnpm@12.3.4` from the npm registry.
- Remaining N11/B15 work: complete permission matrix, settings/access/consumer qualification, production browser evidence, and independent security/UX evidence.

## Implementation update — 018929a (2026-09-13)

- Packet / status / candidate commit / owner: N11 partial / VERIFY / `018929a` / control-plane.
- Observable behavior delivered: the audit page accepts bounded actor, action, resource type, outcome, correlation/request ID, and inclusive time-window filters. Filters are applied in SQL before the stable keyset limit, with text lengths enforced at the HTTP boundary.
- Primary test: `tests/interfaces/control_plane/test_audit_api.py::test_audit_page_filters_before_pagination` covers compound filtering, actor filtering, and overlong input rejection.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_audit_api.py tests/architecture/test_control_plane_route_inventory.py -q` — 5 passed; changed Python `ruff check` — passed.
- Remaining N11/B15 work: complete permission matrix, settings/access/consumer qualification, production browser evidence, and independent UX/security evidence.

## Implementation update — e4764f5 (2026-09-13)

- Packet / status / candidate commit / owner: N11 partial / VERIFY / `e4764f5` / governance UI.
- Observable behavior delivered: Activity now exposes actor, action, request ID, resource type, outcome, and time-window filters. Applying or clearing filters reloads the server-scoped keyset query; loading more retains the same filter set.
- Evidence: `apps/governance-ui/node_modules/.bin/tsc -p apps/governance-ui/tsconfig.json` — passed. The pnpm wrapper remains environment-blocked while resolving the pinned package manager from the npm registry.
- Remaining N11/B15 work: full permission matrix, settings and consumer handoff qualification, production browser evidence, and independent UX/security review.

The follow-up `35e3192` fences filter changes by invalidating the prior management epoch and clearing old pages before the replacement request, so a stale audit cursor or deferred response cannot cross filter scopes. Direct TypeScript/Vite production verification passed again.

Regression evidence after the N11 slices: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane tests/common/config_store -q` — all collected tests passed (100%); no skips were introduced.

Direct UI production verification also passed: `apps/governance-ui/node_modules/.bin/tsc -p apps/governance-ui/tsconfig.json && apps/governance-ui/node_modules/.bin/vite build` produced the Vite bundle (260.29 kB JavaScript, 14.22 kB CSS). The package-manager wrapper remains blocked only when it attempts to fetch its pinned Corepack metadata.

Plugin-platform and architecture regression evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/plugin_platform tests/architecture -q` — all collected tests passed (100%).

## Implementation update — identity migration (working slice, 2026-09-13)

- Packet / status / candidate commit / owner: N05/F01 partial / VERIFY / pending atomic commit / control-plane.
- Observable behavior delivered: `dal-obscura-migrate identity-keys` previews and, with `--apply`, transactionally converts legacy federated owner, grant, draft, audit, publication, ticket, and policy-rule values when exactly one configured issuer (including its historical trailing-slash variant) identifies the value. Delimiter and percent characters are escaped with the same canonical encoder used by runtime actor admission. Unknown or ambiguous history fails closed before mutation.
- Changed paths: `common/identity.py`, `common/config_store/identity_migration.py`, migration CLI, access encoder integration, and focused migration tests. Pickle serializer and payload paths are unchanged.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/common/config_store/test_identity_migration.py -q` — 2 passed; changed-path `ruff check` — passed; operator procedure is documented in `docs/operators.md`.
- Remaining N05/F01 work: structured principal-kind persistence or an independently approved encoding contract, two-process OIDC freshness, browser login/logout/expiry, and ambiguous-history operator evidence.

Follow-up `bf62ca0` closes a migration edge case: local `local|...` and
`group:local|...` keys are intentionally left untouched, with a focused mixed
workspace regression test (3 migration tests passed).

Follow-up `79314a7` also distinguishes exact-issuer canonical escaped keys from
slash-stripped history and rejects the latter when escapes make the original
subject ambiguous (4 migration tests passed).

Post-slice regression: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane tests/common/config_store tests/plugin_platform tests/architecture -q` — all collected tests passed; UI lifecycle tests and direct TypeScript/Vite build also passed. The already-running local endpoints remain healthy (`/healthz` 200, UI root 200). Release remains HOLD pending live OIDC, process-boundary, real catalog/consumer, capacity, recovery, and independent review gates.

## Implementation update — 7c20db5 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B04 partial / VERIFY / `7c20db5` / plugin SDK + control-plane + governance UI.
- Observable behavior delivered: v1 plugin descriptors now carry explicit `output_formats` and `handle_versions`. Pair listing, asset binding, publication compilation, schema discovery, and public adapter execution require the catalog's declared format ID, a shared handle version, and capability compatibility; capability overlap alone cannot admit a pair. Resolved public handles are checked before format factory execution. Discovery with multiple admitted formats requires an explicit UI selection.
- Changed paths: duplicated public/core SDK descriptors and static descriptor loader, built-in and external plugin descriptors, pair route, compiler, asset/schema services, public adapter, UI DTO/Connections view, and regression tests. No pickle serializer, payload class, or import path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_plugins_api.py tests/plugin_platform tests/control_plane/test_asset_service_plugins.py tests/control_plane/test_publication_compiler.py tests/control_plane/test_schema_service.py -q` — all collected tests passed; changed-path Ruff passed; direct UI `tsc` and Vite production build passed (261.51 kB JavaScript, 14.22 kB CSS).
- Remaining N03/B04 work: real three-pair built-wheel compatibility, explicit mutation preconditions, provider/consumer live qualification, and browser evidence. Release remains HOLD.

## Implementation update — UI stale-response fencing (working slice, 2026-09-13)

- Packet / status / candidate commit / owner: N07/F03 partial / VERIFY / pending atomic commit / governance UI.
- Observable behavior delivered: policy restore captures the active asset/load and edit epochs before awaiting the server and cannot overwrite a newer editor; successful restore advances the edit fence. Publish pending state is cleared only for the active load, while logout clears it with private state. Initial-load authentication options and catalog discovery responses are fenced after awaits; selecting a different catalog cannot be overwritten by an older discovery response. Multiple admitted format choices remain explicit.
- Changed path: `apps/governance-ui/src/main.tsx`; protected API and pickle boundaries unchanged.
- Evidence: `node --test tests/lifecycle.test.mjs tests/schema_tree.test.mjs` — 5 passed; direct TypeScript/Vite production build passed (261.81 kB JavaScript, 14.22 kB CSS).
- Remaining N07/F03 work: rendered browser interleaving tests for restore/publish/discovery, settings/error/permission workflow completeness, and independent UX/security review.

## Implementation update — settings/provider editor (working slice, 2026-09-13)

- Packet / status / candidate commit / owner: N11/F04 partial / VERIFY / `a48c75b` / governance UI + control-plane settings.
- Observable behavior delivered: authenticated Settings exposes editable OIDC issuer, audience, JWKS URL, group claims, and enabled state through the existing validated provider-chain route. Save messaging distinguishes staged draft state from serving configuration. Legacy `[redacted]` sensitive values are accepted only as round-trip placeholders and preserved from the server-side row. A transient management read failure keeps the last scoped data visible with an unavailable notice rather than rendering an empty success state.
- Changed paths: UI API and Settings view, auth-provider validation, repository redaction preservation. No pickle serializer or payload path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_settings_api.py tests/control_plane/test_auth_provider_validation.py -q` — 5 passed; changed-path Ruff passed; direct UI TypeScript/Vite build passed (263.56 kB JavaScript, 14.22 kB CSS).
- Remaining N11/F04 work: revision preconditions for provider/runtime writes, last-admin-safe recovery, production OIDC/browser evidence, and independent permission/UX review.

## Implementation update — e7ecdf5 (2026-09-13)

- Packet / status / candidate commit / owner: N05/F01 partial / VERIFY / `e7ecdf5` / control-plane + UI.
- Observable behavior delivered: federated actor keys preserve the exact issuer string, and percent/delimiter characters in subjects and groups are escaped before composing owner, grant, draft, audit, and session lookup keys. Local actors retain their existing unscoped representation. The Access view no longer strips a trailing issuer slash in its guidance.
- Primary test: `tests/control_plane/test_access_identity.py` covers exact trailing-slash issuers, delimiter/percent escaping, and local compatibility; existing actor-auth tests remain green.
- Evidence: actor-auth and identity tests — 34 passed; direct UI TypeScript/Vite build — passed; changed Python `ruff check` — passed. Pickle boundary unchanged.
- Remaining N05/F01 work: structured principal-kind storage, explicit offline conversion for existing federated records, ambiguous-history rejection, and live two-process OIDC freshness/browser evidence.

## Per-packet record template

Replace the corresponding queue entry and keep one current record per packet.
Link detailed logs/artifacts instead of appending repeated full narratives.

- Packet / status / candidate commit / owner:
- Observable behavior delivered; FR/NFR and B/G subcases:
- Changed and deleted paths; old callers removed; protected pickle check:
- Production/test logical SLOC delta; dependencies added/removed and reason:
- Primary invariant test owners; tests consolidated/deleted:
- Exact commands, exit codes, UTC date, runtime versions, environment:
- Artifact and fixture hashes; evidence locations:
- Remaining subcases; blocker and next concrete action:
- Atomic implementation commits:
- Human acceptance, if required: reviewer/date/artifact or VERIFY:

Do not mark N16 complete on build/lint success or substitute agent self-review
for independent review. Until candidate-bound gates all pass, release is HOLD.
