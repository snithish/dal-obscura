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

## Implementation update — a589f3f (2026-09-13)

- Packet / status / candidate commit / owner: N09/F14 partial / VERIFY / `a589f3f` / control-plane + governance UI.
- Observable behavior delivered: evaluation, server review, and policy-version publication accept an explicit saved `draft_id` plus revision. Review tokens bind the selected asset draft ID, author, revision, and content hash, so a publisher can activate an editor-owned draft without falling back to the publisher's personal draft. Foreign or discarded draft references are concealed as not found; stale revisions and token/draft mismatches fail closed. The UI carries the draft ID/revision from load through test, review, and publish.
- Changed paths: policy/evaluation/review/publication services, repository draft lookup, route schemas, governance UI API/editor state, and cross-identity API regression fixtures. No pickle serializer, payload class, or import path changed.
- Primary tests: `tests/interfaces/control_plane/test_schema_api.py::test_publisher_can_review_and_publish_editor_draft_by_explicit_id` plus existing schema/policy-version/control-plane suites.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_schema_api.py tests/interfaces/control_plane/test_policy_versions_api.py tests/control_plane -q` — all collected tests passed. `ruff check` on changed Python paths — passed. `cd apps/governance-ui && npm run build` — TypeScript and Vite production build passed (263.71 kB JavaScript, 14.22 kB CSS). UI lifecycle/schema tests — 5 passed.
- Remaining N09/F14 work: rendered browser handoff flow, two-process publisher/editor race evidence, token replay/expiry matrix, and independent security/UX review. Release remains HOLD.

Follow-up `1e83d78` adds an authorized `GET /v1/assets/{asset_id}/draft/{draft_id}`
handoff endpoint and a same-origin UI link format (`/?asset=...&draft=...#assets`).
Opening that link loads the immutable saved draft into a read-only editor while
retaining server-side evaluation, review, and publish actions. Route inventory,
API, and UI regression/build checks pass; browser rendering and multi-process
handoff races remain VERIFY.

Follow-up `af81794` aligns the selected-draft error contract with B05: stale
evaluation revisions return 409, missing selected drafts remain concealed as
404, and the cross-identity regression proves an editor change invalidates a
publisher's expected revision.

Follow-up `65adae6` returns and renders separate draft-author and reviewer
identities in completed review evidence, closing the authorship distinction in
the handoff response. Full before/after diff rendering and independent browser
acceptance remain VERIFY.

Follow-up `67e6475` keeps the selected draft ID on read-only handoff refreshes,
so reload/access actions cannot silently switch back to the publisher's draft.

## Implementation update — 2d005bb (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 partial / VERIFY / `2d005bb` / plugin registry.
- Observable behavior delivered: runtime admission and lock-file loading now require the exact five-part distribution/version/API/descriptor-digest/artifact-digest lock. The registry always reads a factory-free static descriptor (or an explicitly injected descriptor loader in tests); the fabricated three-part descriptor fallback is deleted.
- Changed paths: `common/plugin_api/registry.py` and plugin registry tests. No pickle serializer, payload class, or import path changed.
- Primary tests: `tests/plugin_platform/test_registry.py`, `tests/plugin_platform/test_lockfile.py` — all passed; changed-path Ruff passed.
- Remaining N02/B03 work: remove legacy module-name aliases and permissive serving readers, retain bounded offline conversion for known persisted records, and prove clean installed-wheel migration.

## Implementation update — cebe127 (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 plus N09 draft integrity / VERIFY / `cebe127` / control-plane + data-plane.
- Observable behavior delivered: the control-plane composition root now admits one authoritative built-in registry when no external lock is supplied; duplicate route descriptors and an unused validator alias are deleted. Serving manifests require canonical plugin IDs, reject retired module identities, and merge migrated binding columns before runtime validation. Restoring history updates the existing draft's base policy generation together with its revision.
- Changed paths: control-plane app/plugin route, catalog validator, repository draft persistence, data-plane published-config adapter, and focused tests. No pickle serializer, payload class, or import path changed.
- Primary tests: audit, policy-version, catalog-option, published-config, and plugin-route suites passed; changed-path Ruff passed.
- Remaining N02/B03 work: remove remaining legacy module readers from migration-only boundaries where safe, enforce explicit secret scopes, and prove clean installed-wheel migration. Remaining N09 work includes race and browser evidence.

## Implementation update — 56decd6 (2026-09-13)

- Packet / status / candidate commit / owner: N02/N04/B03/B06 partial / VERIFY / `56decd6` / secret provider + catalog validation.
- Observable behavior delivered: secret references are now always shaped as `{secret, scope}` with a non-empty scope equal to the requesting boundary. Unscoped references fail before provider lookup; catalog descriptors and inline-secret validation enforce the same rule; data-plane identity resolution uses the explicit `identity` scope.
- Changed paths: secret provider, catalog option validation, data-plane identity loader, and migrated focused fixtures. No pickle serializer, payload class, or import path changed.
- Primary tests: secret-provider, catalog-option, catalog API, schema, and data-plane interface suites passed; changed-path Ruff passed.
- Remaining N04/B06 work: prove scoped secret behavior across all provider/config callers, leak-boundary checks, and production credential rotation evidence.

## Implementation update — 2467b29 (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 partial / VERIFY / `2467b29` / publication compiler.
- Observable behavior delivered: compiled Iceberg manifests now persist the canonical `iceberg.sql` catalog plugin ID instead of the retired Python class-path identity. Internal catalog construction metadata remains unchanged, and the strict data-plane binding validator accepts newly published rows.
- Primary tests: publication compiler suite and elevated end-to-end Iceberg Flight smoke passed; pickle boundary unchanged.
- Remaining N02/B03 work: clean installed-wheel migration proof and removal of remaining legacy readers from migration-only code.

Regression evidence after the canonical manifest cutover: elevated local-socket
`UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest -q` completed with
all collected tests passing and only the repository's existing marked skips. The
socket-backed connector, Flight, and E2E Iceberg lanes also passed separately.
Direct UI TypeScript/Vite production build remains green (264.44 kB JavaScript,
14.22 kB CSS). Release remains HOLD for the unresolved N03–N16 live qualification,
capacity, recovery, and independent review gates.

Regression after the handoff slices: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv
run --no-sync pytest tests/control_plane tests/interfaces/control_plane
tests/common/config_store tests/plugin_platform tests/architecture -q` passed
all collected tests. Plugin conformance passed (2 tests); direct UI TypeScript/
Vite production build passed (264.88 kB JavaScript, 14.22 kB CSS); UI lifecycle
and schema tests passed (5). No pickle fixture or serializer paths changed.

## Implementation update — 4e5f314 (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 partial / VERIFY / `4e5f314` / plugin SDK + packaging.
- Observable behavior delivered: `dal-obscura-plugin-api` is an explicit runtime dependency resolved from the repository SDK package and included in the Docker builder context. The runtime, control plane, lock builder, and tests import shared descriptor/configuration contracts directly from the public SDK. The duplicate `src/dal_obscura/common/plugin_api/contracts.py` module and its re-export surface were deleted; `PluginKind` is exported by the SDK. Pickle serializers, serialized classes/import paths, and task payload semantics were not changed.
- Changed and deleted paths: `pyproject.toml`, `uv.lock`, `Dockerfile`, public SDK `__init__.py`, plugin registry/lock adapters, built-in plugin wiring, lock-builder script, and contract/registry tests; deleted internal duplicate contracts module.
- Primary tests: `tests/plugin_platform`, `tests/control_plane`, `tests/interfaces/control_plane`, `tests/infrastructure/adapters`, and `tests/architecture` all passed; changed-path Ruff passed. `uv build` passed and wheel metadata contains `Requires-Dist: dal-obscura-plugin-api`.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/plugin_platform tests/control_plane tests/interfaces/control_plane tests/infrastructure/adapters -q`; `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/plugin_platform tests/architecture -q`; targeted `ruff check`; `uv build`.
- Remaining N02/B03 work: strict old-input rejection and maintenance-mode conversion for known persisted records, removal of any remaining migration-only legacy readers where safe, and clean installed-wheel migration proof. Release remains HOLD.

## Implementation update — 38f9dc2 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `38f9dc2` / control-plane repository + API.
- Observable behavior delivered: existing catalog updates and asset metadata, owner, grant, and schema replacements now require the current revision. Missing preconditions fail with HTTP 428 and stale values retain HTTP 409 compare-and-set behavior; create-at-zero remains valid. Rejected writes leave the resource revision and data unchanged.
- Changed paths: control-plane revision error, repository guards, shared route error mapping, and catalog/asset/repository regression tests. No pickle serializer, payload class, or import path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_catalogs_api.py tests/interfaces/control_plane/test_assets_api.py tests/control_plane/test_publication_store.py tests/control_plane/test_asset_grant_authorization.py -q` — 34 passed; changed-path Ruff and `git diff --check` passed.
- Remaining N03/B05 work: runtime/auth-provider write preconditions, explicit activation generation precondition, safe structured error envelopes, process race evidence, and generated DTO/browser proof. Release remains HOLD.

Follow-up `d9ddea4` migrates the existing control-plane, integration, connector,
and E2E fixture callers to send the revision required by the new CAS contract,
including sequential owner/grant updates. `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache
uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane -q`
passed; changed test paths pass Ruff. Socket-backed Flight and benchmark lanes
remain environment-blocked in this sandbox and are still VERIFY.

The clean wheel gate for N02 also passed on 2026-09-13: the server and SDK
wheels were installed into `/tmp/dal-obscura-wheel-smoke`, with the process
started from `/tmp` and no checkout path, and imports resolved from that target
for both `dal_obscura` and `dal_obscura_plugin_api`. The SDK wheel was built
from `packages/plugin-api`; the server wheel metadata declares the SDK
dependency. This is artifact evidence only; live plugin admission and
maintenance-mode conversion remain open.

Follow-ups `d9ddea4` and `051bfe0` complete the existing owner/grant/schema
fixture migration for the CAS contract, including multi-step schema drift and
cross-identity draft handoff cases. The combined regression lane
`UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest
tests/plugin_platform tests/control_plane tests/interfaces/control_plane
tests/infrastructure/adapters -q` passed with no failures. Socket-bound Flight,
benchmark, and live process-boundary lanes remain VERIFY in this environment.

## Implementation update — 9fc6197 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `9fc6197` / workspace activation route + UI.
- Observable behavior delivered: publication activation now requires an explicit serving-generation precondition. A caller sends the current publication ID for replacement or JSON `null` for first activation; an omitted body/field returns 428 before mutation. The UI always supplies the precondition and preserves the existing 409 stale-generation CAS behavior.
- Changed paths: workspace route, UI transport, activation API tests, and missing-precondition regression coverage. No pickle serializer or payload path changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/interfaces/control_plane/test_config_activation.py tests/interfaces/control_plane/test_workspace_api.py -q` — 7 passed; UI `tsc` and changed-path Ruff passed.
- Remaining N03/B05 work: runtime/auth-provider write preconditions, safe structured error envelopes, process race evidence, and generated DTO/browser proof. Release remains HOLD.

## Implementation update — 21290df (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `21290df` / runtime settings + control-plane UI.
- Observable behavior delivered: runtime ticket settings now carry a persisted revision through migration `20260913_0017`. First creation remains revision zero; existing updates require `expected_revision`, stale values return 409, and omitted values return 428 before mutation. Authenticated GET/PUT responses expose the revision, while the UI sends it as a request precondition without submitting it as an unknown field.
- Changed paths: runtime ORM/repository/workspace service, settings route schemas, Alembic migration, UI transport, schema migration/settings/inventory tests. Pickle serializers and task payloads are unchanged.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane -q` — all passed; runtime/settings migration tests and changed-path Ruff passed; UI TypeScript passed.
- Remaining N03/B05 work: auth-provider chain revision preconditions, safe structured error envelopes, process race evidence, and generated DTO/browser proof. Release remains HOLD.

## Implementation update — 14edd5b (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 partial / VERIFY / `14edd5b` / compiler + data-plane manifest reader + offline migration.
- Observable behavior delivered: newly compiled catalog and asset manifests carry canonical `catalog.type` metadata without a Python module identity. The serving reader rejects legacy module-shaped catalog config before registry/provider resolution. The explicit `dal-obscura-migrate plugin-bindings --apply` path rewrites only the known Iceberg shape, reports changes once, and is idempotent on rerun; unknown records remain unsupported.
- Changed paths: publication compiler, published-config adapter, plugin-binding migration, and focused migration/serving tests. Pickle serializer and serialized task paths remain untouched.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/common/config_store/test_plugin_bindings.py tests/infrastructure/adapters/test_published_config.py tests/control_plane/test_publication_compiler.py -q` — all passed; changed-path Ruff and `git diff --check` passed.
- Remaining N02/B03 work: strict old lock/secret/API input matrix, migration qualification on populated PostgreSQL with maintenance cutover, and release artifact admission. Release remains HOLD.

## Implementation update — 2e23b25 (2026-09-13)

- Packet / status / candidate commit / owner: N02/N05 UI authentication cleanup / VERIFY / `2e23b25` / governance UI.
- Observable behavior delivered: the shipped UI no longer contains the development `?demo` workspace, local fixture asset, demo persona buttons, or demo-login client method. Workspace reads, policy evaluation, draft saves, reviews, publishes, and restores now always use the authenticated control plane. SSO and explicitly enabled local bootstrap-token login remain available for production and local parity respectively.
- Changed and deleted paths: `apps/governance-ui/src/main.tsx`, `apps/governance-ui/src/api.ts`, deleted `apps/governance-ui/src/fixtures.ts`. No backend demo route was changed, preserving existing server-side compatibility tests and local operator fixtures. No pickle serializer, payload class, or import path changed.
- Evidence: `node --experimental-strip-types --test tests/*.test.mjs` — 5 passed; `node_modules/.bin/tsc -p tsconfig.json && node_modules/.bin/vite build` — passed (263.59 kB JavaScript, 14.22 kB CSS); generated bundle contains no `demo-login`, `Use demo persona`, `Explicit demo`, or `?demo` strings. `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/architecture/test_local_demo_ui.py tests/examples/test_ui_smoke.py -q` — 3 passed.
- Remaining N02/N05 work: backend demo endpoint retirement and fixture migration require a separate compatibility decision; live OIDC/browser expiry, two-process freshness, and independent security/UX review remain VERIFY. Release remains HOLD.

## Implementation update — 96bfdf6 (2026-09-13)

- Packet / status / candidate commit / owner: N02/B03 partial / VERIFY / `96bfdf6` / control-plane + governance UI + test fixtures.
- Observable behavior delivered: public `/policy-rules` and `/policy-preview` routes are absent from OpenAPI and return side-effect-free 404 tombstones. The UI and test fixtures use revisioned `/draft`; evaluation uses `/policy-evaluate`. Workspace publication consumes the latest saved draft per asset, and inventory policy status recognizes non-empty saved drafts.
- Changed paths: policy route adapter, repository publication/status assembly, UI API and README, route inventory, and migrated API fixtures. Internal policy helpers and protected pickle paths remain intact.
- Evidence: migrated control-plane/API suites and route inventory passed; direct UI TypeScript/Vite build passed. `rg` finds no production callers for retired route methods. Release remains HOLD.
- Remaining N02/B03 work: remove remaining duplicate SDK/contract definitions, three-part lock and alias residue, enforce strict old-input rejection and maintenance-mode offline conversion, and prove clean installed-wheel migration.

## Implementation update — d139566 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `d139566` / auth-provider settings + governance UI.
- Observable behavior delivered: authentication provider chains now persist a shared revision through migration `20260913_0018`. Initial creation uses revision zero; existing replacements require `expected_revision`, stale writes return 409, and omitted preconditions return 428 before mutation. Responses expose the revision and the UI sends it with provider saves. Redacted secret arguments remain preserved.
- Changed paths: auth-provider ORM/repository/service/route, migration, validation redaction, UI API/editor, and CAS regression fixtures. Pickle serializers, serialized task classes, and import paths were not changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane -q` — all passed; `tests/plugin_platform` and `tests/infrastructure/adapters` passed; UI `tsc` and changed-path Ruff passed.
- Remaining N03/B05 work: safe structured error envelopes, two-process freshness/race evidence, and generated DTO/browser proof. Release remains HOLD.

## Implementation update — a3bef98 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `a3bef98` / control-plane API.
- Observable behavior delivered: HTTP 409 revision conflicts and 428 missing-precondition failures now include a safe `error` object with stable code, message, and request ID while retaining the existing `detail` field for current clients. Successful responses and unrelated error contracts are unchanged.
- Changed paths: FastAPI HTTP exception handler and CAS regression assertions. Pickle serializers and task payloads are unchanged.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane -q` — all passed; changed-path Ruff, format, and diff checks passed.
- Remaining N03/B05 work: field-level errors where useful, two-process freshness/race evidence, and generated DTO/browser proof. Release remains HOLD.

## Implementation update — 87d4817 (2026-09-13)

- Packet / status / candidate commit / owner: N12/B16 partial / VERIFY / `87d4817` / PostgreSQL race harness.
- Observable behavior delivered: the integration race suite now provisions an auth chain and exercises barrier-coordinated concurrent runtime-settings and auth-provider replacements. Each scenario requires exactly one revision-zero commit and one `PublicationConflictError`, complementing existing draft, grant, and binding races.
- Changed paths: `tests/integration/control_plane/test_publication_races.py`; no production or pickle paths changed.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/integration/control_plane/test_publication_races.py -q` — 5 scenarios skipped because `DAL_OBSCURA_POSTGRES_TEST_URL` is not configured in this sandbox; Ruff passed. CI PostgreSQL execution is still required for acceptance.
- Remaining N12/B16 work: run the full two-process PostgreSQL/API/Flight interleaving matrix with termination and lost-response evidence. Release remains HOLD.

## Implementation update — 7fae923 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B04 partial / VERIFY / `7fae923` / control-plane API contracts.
- Observable behavior delivered: runtime and authentication-provider settings routes now publish explicit Pydantic response models. OpenAPI describes nullable runtime settings and redacted provider records, including revisions, so generated clients can consume a stable schema.
- Changed paths: settings response DTOs, route annotations, and OpenAPI regression assertions. Pickle serializers and task payloads are unchanged.
- Evidence: settings and UI-shell tests passed; changed-path Ruff/format checks passed. Full generated TypeScript client regeneration remains open because the UI currently uses handwritten transport types.
- Remaining N03/B04 work: generate/check browser DTOs from the OpenAPI contract, complete real pair compatibility, and run live consumer/browser evidence. Release remains HOLD.

## Implementation update — 923ad50 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `923ad50` / configuration migration tests.
- Observable behavior delivered: the idempotent schema migration regression now tracks Alembic head `20260913_0018`, covering the auth-provider revision column introduced in the preceding atomic slice.
- Evidence: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/common/config_store/test_schema_migrations.py -q` — 7 passed.
- Remaining N03/B05 work: complete generated browser DTO wiring and live process-boundary evidence. Release remains HOLD.

## Implementation update — 21dacc0 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 partial / VERIFY / `21dacc0` / auth-provider revision persistence.
- Observable behavior delivered: the auth-chain revision is persisted on the cell as well as provider rows, so removing every provider does not reset compare-and-swap state. An authenticated revision endpoint lets the UI continue guarded writes after an empty chain; delete and recreate regressions advance revisions 1→2→3.
- Changed paths: cell ORM/migration, repository/service/route revision endpoint, UI settings loading, and API tests. Pickle serializers and task payloads are unchanged.
- Evidence: settings, OpenAPI, and migration tests passed; UI TypeScript passed; changed-path Ruff/format checks passed.
- Remaining N03/B05 work: generated DTO/browser proof and live process-boundary execution. Release remains HOLD.

## Implementation update — local parity verification (2026-09-13)

- Packet / status / candidate commit / owner: N03/B05 / VERIFY / working tree verification / control-plane + UI.
- Observable behavior delivered: the disposable UI database was upgraded from `20260913_0016` to packaged head `20260913_0018`; the updated control plane restarted successfully and serves the authenticated provider list and revision endpoint. `GET /v1/settings/auth-providers/revision` with the local admin bearer returned `{"revision":0}`, the provider list returned `[]`, and the Vite UI root returned HTTP 200.
- Evidence: `DAL_OBSCURA_DATABASE_URL=sqlite+pysqlite:////private/tmp/dal-obscura-ui-dev-8821.db uv run dal-obscura-migrate upgrade`; live curl checks against `127.0.0.1:8821` and `127.0.0.1:5173`.
- Remaining release gates: production OIDC/browser lifecycle, generated DTO/browser proof, real PostgreSQL race matrix, live catalog/consumer qualification, capacity/recovery, and independent security/UX review. Release remains HOLD.

## Verification update — broad regression (2026-09-13)

- `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest tests/control_plane tests/interfaces/control_plane tests/plugin_platform tests/infrastructure/adapters -q` passed with all collected tests green after the persistent auth revision changes. The UI TypeScript/Vite build and changed-path Ruff checks also pass. No pickle serializer or task payload files were modified.
- Release remains HOLD for the required PostgreSQL two-process barriers, live OIDC/browser lifecycle, generated DTO/browser proof, real catalog/consumer matrix, capacity/recovery, and independent security/UX acceptance.

## Implementation update — 1a33f92 (2026-09-13)

- Packet / status / candidate commit / owner: N05/B08 partial / VERIFY / `1a33f92` / control-plane security headers.
- Observable behavior delivered: API and auth responses now emit a strict same-origin Content-Security-Policy with no `unsafe-eval`, unrestricted scripts, framing, or form targets, alongside existing anti-sniff/referrer protections.
- Evidence: actor-auth security-header regression passed; changed-path Ruff passed. A built-browser CSP inspection remains part of the live OIDC/browser gate.
- Remaining N05/B08 work: real OIDC code/PKCE browser journey, cookie/expiry/role freshness, and production profile bootstrap/recovery evidence. Release remains HOLD.

## Implementation update — 64642e1 (2026-09-13)

- Packet / status / candidate commit / owner: N03/B04 partial / VERIFY / `64642e1` / plugin configuration forms.
- Observable behavior delivered: admitted plugin descriptors now validate boolean, integer, numeric, enum, string, URI, and secret-reference values with declared choices. The Connections UI renders typed controls and serializes values without coercing booleans or numbers to strings; unsupported descriptor types still fail closed.
- Changed paths: catalog descriptor validation, Connections form rendering/serialization, and focused tests. Pickle serializers and task payloads are unchanged.
- Evidence: plugin option and route tests passed; UI TypeScript passed; changed-path Ruff/format checks passed.
- Remaining N03/B04 work: generated DTO/browser proof, real three-pair compatibility and consumer qualification. Release remains HOLD.

## Implementation update — 7884cca (2026-09-13)

- Packet / status / candidate commit / owner: N03/B04 partial / VERIFY / `7884cca` / plugin descriptor validation.
- Observable behavior delivered: numeric descriptor fields now require finite numbers; booleans, integers, enums, strings, URIs, and secret references retain their declared types and reject coercion or non-finite values before plugin factory use.
- Evidence: catalog option validation tests passed; changed-path Ruff/format checks passed. Browser and built-wheel typed-form qualification remain open.
- Remaining N03/B04 work: generated DTO/browser proof, real three-pair compatibility and consumer qualification. Release remains HOLD.

## Implementation update — 9854b45 (2026-09-13)

- Packet / status / candidate commit / owner: N06/B09 partial / VERIFY / `9854b45` / governance UI shell.
- Observable behavior delivered: account identity and sign-out remain visible below the mobile breakpoint, and users can select System, Light, or Dark theme. Only the non-sensitive theme preference is persisted; dark-mode surfaces and controls use the documented contrast-oriented palette.
- Evidence: UI TypeScript and Vite production build passed; no private session or policy state is persisted by the theme control. Manual responsive/accessibility review remains required.
- Remaining N06/B09 work: command palette, icon system, menu drawer, rendered 390/768/1440 layouts, keyboard/screen-reader and axe review. Release remains HOLD.

## Implementation update — 1bea336 (2026-09-13)

- Packet / status / candidate commit / owner: N06/B09 partial / VERIFY / `1bea336` / governance UI navigation.
- Observable behavior delivered: Cmd/Ctrl+K opens a keyboard-accessible command palette for authorized destination navigation and workflow help. It closes on Escape/backdrop, supports filtering, and contains no publish/delete/revoke action. Palette state is ephemeral and does not persist policy or identity data.
- Evidence: UI TypeScript and Vite production build passed. Manual keyboard focus restoration, responsive rendering, and screen-reader review remain required.
- Remaining N06/B09 work: menu drawer, Lucide icon system, focus restoration, rendered layout/a11y checks, and full page state coverage. Release remains HOLD.

## Implementation update — 81b4fee (2026-09-13)

- Packet / status / candidate commit / owner: N06/B09 partial / VERIFY / `81b4fee` / governance UI accessibility.
- Observable behavior delivered: closing the command palette by Escape, backdrop, or command selection restores focus to the element that invoked it, preserving keyboard navigation continuity.
- Evidence: UI TypeScript and Vite production build passed. Manual keyboard, screen-reader, and responsive layout acceptance remains required.
- Remaining N06/B09 work: menu drawer, Lucide icon system, full responsive rendering and accessibility review. Release remains HOLD.

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
