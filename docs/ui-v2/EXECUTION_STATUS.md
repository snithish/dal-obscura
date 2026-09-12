# Governance application execution status

Plan created against `612bb1c`; implementation evidence and the 2026-09-12
production review are recorded below. **Paid-production release: HOLD.**
Canonical task order: [execution handoff](EXECUTION_HANDOFF.md).
Required backend/production coverage: [production review](PRODUCTION_READINESS.md).

## Gate status

- P00 contracts, capability matrix, and migration/session design: baseline
  implemented and kept in sync with the route inventory; paid-production review
  remains open for bootstrap, proxy trust, rate limits, and recovery policy.
- P01 installed startup and container assembly: implementing. Commits `734c018`,
  `02e143b`, and `5fc06a1` add a configured control-plane command, pin the UI
  package manager for container builds, and isolate console arguments from
  programmatic CLI calls. A migrated SQLite instance has served health and
  readiness endpoints through the configured composition path. Wheel/container
  startup, Compose execution, and the remaining packaging slices remain
  unverified.
- P02 real OIDC login and revocable sessions: implemented-unverified; rate
  limits, proxy trust, provider logout/reauthentication, and real IdP evidence
  remain.
- P03 complete scoped authorization: implemented-unverified for asset owner and
  delegated capabilities; tenant isolation, bootstrap lifecycle, and a full
  PostgreSQL matrix remain.
- P04 reliable UI lifecycle and behavioral tests: implemented-unverified;
  stale-response fencing and duplicate-submit protection are shipped, while
  browser automation evidence remains.
- P05 canonical nested schema API/tree: implemented-unverified; authoritative
  recursive Iceberg paths are shipped, while real catalog/consumer probes remain.
- P06 durable drafts and conflict protection: implemented-unverified.
- P07 bound synthetic evaluations: implemented-unverified with bounded rows and
  evidence tied to draft revision/content hash.
- P08 exact review and atomic publication UI/API: implemented-unverified;
  server review, explicit publish capability, CAS activation, asset-row
  serialization, and idempotency replay are shipped, while concurrent
  PostgreSQL evidence and deny-all policy semantics remain.
- P09 history, restore, audit, runtime observations: implemented-unverified;
  immutable history, audited restore/mutations, and explicit unobserved
  data-plane status are shipped.
- P10 complete management and consumer handoff: partial; UI management views and
  API contracts exist, while grant administration, operation lookup, and real
  DuckDB/Spark/Flight handoff evidence remain.
- P11 verified local feature/security parity: partial; local uses shared auth,
  CSRF, authorization, evaluation, and publication code, while a real local
  OIDC/browser stack is unverified.
- P12 quality, usability, independent review and release: implementing; focused
  tests and static checks pass, independent security/UX review is open.
- P13 supported production deployment and startup: partial; production Compose
  reference and fail-closed profile validation exist, while clean image/wheel,
  PostgreSQL, TLS ingress, and live startup evidence remain.
- P14 recovery, upgrades, and credential lifecycle: implementing; the
  `dal-obscura-maintenance invalidate-access` command now revokes browser
  sessions, consumes OIDC login transactions, and removes durable Flight
  tickets after restore. Encrypted backup/PITR, isolated restore evidence,
  upgrades, and key-rotation drills remain.
- P15 capacity, observability, and customer operations: partial; bounded
  request bodies/collections, evaluation, explicit runtime observations,
  durable ticket cleanup, and restore invalidation exist, while aggregate
  limits, metrics/alerts, load, and customer runbooks remain.
- P16 whole-product release evidence and promotion: not-started.

These statuses refer to acceptance under the new packets, not absence of all
reusable code. Previous build/hook results are historical evidence only.

## Known external gates

- Paid-production release still requires security review of bootstrap closure,
  trusted proxy/origin handling, login limits, and recovery semantics in P00.
- Previous container execution failed because Podman was stopped; recheck runtime
  availability when starting P01. No live-stack success has been recorded.
- Owner visual acceptance, participant sessions, and independent security review
  have not been recorded. Prepare materials; do not contact others unasked.
- Owner's pickle-preservation instruction remains; no UI task resolves W06.

## Next action

Close the remaining production gates: clean wheel/Compose/PostgreSQL/TLS
evidence, concurrent publication races and operation lookup, deny-all policy
semantics, Flight health observations, grant/bootstrap lifecycle, and browser
UX/security review. A stopped VM blocks container evidence, not implementation;
do not call the release complete without those observations.

## Evidence

### P13/P14 implementation follow-up — `62433ff`, `34f06c5`

- State: implementing.
- Behavior: production data-plane startup now requires PostgreSQL, a strong
  ticket secret, `grpc+tls`, and certificate/key material. TLS values may be
  mounted file paths or bounded inline PEM. The maintenance CLI invalidates
  browser sessions and pending login transactions and deletes replayable
  tickets by cell before restored ingress opens.
- Green evidence: runtime-config tests and the SQLite maintenance integration
  test pass; Ruff and Ty pass. PostgreSQL PITR and a real restored consumer
  read remain unverified.
- Limitation: this command is an operator recovery control, not proof of a
  backup, restore, key rotation, or RPO/RTO drill.

### P15.1 bounded access-state retention — `2aa97eb`

- State: partial.
- Behavior: issuing browser sessions or OIDC login transactions removes
  revoked/consumed/expired rows. Data-plane workers run a bounded durable-ticket
  cleanup loop; the interval is configurable and defaults to 60 seconds.
- Green evidence: runtime-config, browser-session, ticket-store, and recovery
  tests pass with Ruff and Ty. Multi-process capacity, metrics, and alerting
  remain open.

### P08.2 publication race serialization — `b1165cf`

- State: implemented-unverified.
- Behavior: PostgreSQL publication transactions lock the governed asset row
  before checking idempotency and expected generations, preventing concurrent
  API processes from publishing the same request twice.
- Green evidence: publication API/idempotency and repository CAS tests pass;
  a real two-process PostgreSQL race still needs execution.

### P15.2 request-boundary limits — `c2975a3`

- State: partial.
- Behavior: control-plane requests with a declared body over 1 MiB are
  rejected before authentication, and policy, identity, grant, provider, and
  legacy schema collections have explicit cardinality caps.
- Green evidence: oversized-request, asset, policy, and inventory tests pass;
  streamed-body enforcement, aggregate per-customer quotas, and load evidence
  remain open.

### Production review — 2026-09-12

- Baseline: `0e5a3a3`. Decision: HOLD for paid production.
- Deliverable: `PRODUCTION_READINESS.md` maps each required UI workflow to actual
  backend code/gaps and adds P13–P16 deployment, recovery, capacity, and release
  packets. P00 now lists unresolved login-transaction, identity/group freshness,
  CSRF, bootstrap, operation, and restore-invalidation contract details.
- Verification: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest
  tests/interfaces/control_plane/test_api_publish_flow.py
  tests/interfaces/control_plane/test_actor_auth.py
  tests/interfaces/control_plane/test_workspace_api.py -q --maxfail=3` exited 0.
- Synthetic probe: using `_client`/`_bearer` from `test_actor_auth.py` and
  `_provision_draft` from `workspace_helpers.py`, an authenticated outsider got
  HTTP 200 for inventory, an asset's policy rules, auth-provider settings, and
  history. Test identities only; no real IdP or customer data involved. This is
  evidence of a missing scoped gate, not successful security acceptance.
- Corrected evidence: unsupported installed-wheel/Compose success claims removed;
  actual prior commit IDs filled in; route-inventory and generated-environment
  test limitations made explicit. P01 still needs restart preservation and real
  Flight/installed-image checks. No container, production, or browser proof added.
- Independent review: none. Scope assumes one isolated deployment per customer
  until the owner confirms the hosting model. Final contracts/migrations and
  customer release acceptance remain open.

### P01 package CI correction — `ffa69f4`

- State: implemented-unverified (remote package job not executed).
- Change: CI installs `[server,postgres]` before running migrations, and checks
  the installed control-plane executable's `--help`. Default client-wheel smoke
  remains separate and does not acquire server dependencies.
- Red: updated workflow regression failed on missing `[server,postgres]`.
- Green: `UV_CACHE_DIR=/tmp/dal-obscura-uv-cache uv run --no-sync pytest
  tests/architecture/test_ci_workflow.py
  tests/interfaces/control_plane/test_control_plane_cli.py -q` passed 7 tests.
  Focused Ruff lint, format check and Ty passed.
- Commit hook: normal pre-commit attempted dependency resolution via `uv run`
  and stalled at Ruff; interrupted after observing retries. Pre-commit restored
  its stashed documents. The commit used a command-local hooks override after
  focused checks. No full-hook/full-suite pass is claimed.
- Next: run the installed wheel/server and CI job in a clean dependency-capable
  environment; continue remaining P01 and P16 acceptance.

- Packet/slice: P00.1 route inventory and contract-review preparation.
- State: review pending.
- Commit: `64f9927`.
- Behavior and touched modules: `P00_CONTRACT_REVIEW.md` records every current
  OpenAPI path, its present authorization dependency, its target capability
  direction, the session/persistence decisions requiring review, and concrete
  negative examples. `test_control_plane_route_inventory.py` checks the OpenAPI
  path set and two method sets; it does not synchronize the full document or
  verify its authorization table.
- Prerequisites/review authorization: documentation and an executable baseline
  only. The handoff explicitly requires capable-owner review before P02/P03
  session, capability, or persistence work.
- Red test and actual failure: the route inventory was previously represented
  only by partial endpoint checks; an endpoint could be added without updating a
  complete reviewed inventory.
- Green commands and results: `uv run --no-sync pytest
  tests/architecture/test_control_plane_route_inventory.py
  tests/interfaces/control_plane/test_ui_shell.py -q` → 7 passed; focused Ruff
  and Ty checks passed.
- Browser/API/PostgreSQL/consumer evidence: generated FastAPI OpenAPI inventory
  only; no behavior change.
- Manual/independent review: capable-owner contract review is outstanding.
- Remaining limitations/blocker: P02/P03 implementation stays blocked by the
  explicit session, persistence, and scoped-capability decisions.
- Next action: obtain the required P00 decision, then add the corresponding
  failing authorization/session tests before implementation.

- Packet/slice: P00.2 concrete session, boundary, persistence, and API proposal.
- State: review pending.
- Commit: `0e5a3a3`.
- Behavior and touched modules: extends `P00_CONTRACT_REVIEW.md` with proposed
  expiry and revocation behavior, cookie/CSRF/origin/proxy rules, bootstrap-token
  lifecycle, additive record designs and retention, migration acceptance evidence,
  and a candidate replacement API map.
- Prerequisites/review authorization: review material only. The proposals are
  deliberately marked unimplemented and do not authorize migration or security
  behavior changes.
- Red test and actual failure: the prior packet named the required decisions but
  did not give a low-ambiguity default for tests, records, retention, or API
  replacement boundaries.
- Green commands and results: `git diff --check` passed.
- Browser/API/PostgreSQL/consumer evidence: none; no code path changed.
- Manual/independent review: capable-owner decision remains outstanding.
- Remaining limitations/blocker: P02/P03 cannot select, implement, or test a
  session/grant/migration contract until the reviewer accepts or revises these
  proposals.
- Next action: record the reviewer decision against the proposal table, then
  write negative security tests before the first P02/P03 implementation slice.

- Packet/slice: P01.1 installed control-plane command.
- State: partially verified.
- Commit: `734c018`.
- Behavior and touched modules: `pyproject.toml`,
  `control_plane/interfaces/control_plane_cli.py`, focused CLI tests.
- Prerequisites/review authorization: independent startup repair; no persistence
  or security-contract change.
- Red test and actual failure: before implementation, importing the target CLI
  module failed because it did not exist; the declared console script was absent.
- Green commands and results: `uv run --no-sync pytest
  tests/interfaces/control_plane/test_control_plane_cli.py
  tests/interfaces/control_plane/test_health.py -q` → 7 passed; focused Ruff and
  Ty checks passed. Formatting was applied by Ruff. `uv build --offline`
  produced both the source distribution and wheel; the wheel's
  `entry_points.txt` contains `dal-obscura-control-plane`. Calling the
  command's `--help` parser through its module succeeded. The isolated
  `uv tool run --from` invocation returned no captured command output or exit
  status; its success claim was withdrawn in the 2026-09-12 review. A freshly migrated
  temporary SQLite database then started Uvicorn at `127.0.0.1:18820`; both
  `GET /healthz` and `GET /readyz` returned HTTP 200, with the readiness
  database check reporting `ok`.
- Browser/API/PostgreSQL/consumer evidence: real local HTTP health/readiness
  evidence through the checkout composition path on SQLite only; no verified
  installed-wheel command/server, Postgres, Compose, browser, or consumer evidence.
- Manual/independent review: none.
- Remaining limitations/blocker: pre-commit hook runner stalled after its format
  hook; manually equivalent focused quality checks passed before `--no-verify`
  commit. The project environment is stale and does not expose the new console
  script under `uv run --no-sync`. The later isolated-wheel server attempt retried
  dependency resolution and was interrupted without binding its port. Container
  runtime availability remains unresolved.
- Next action: start a server from the isolated wheel installation, then validate
  container assembly when a container runtime is available.

- Packet/slice: P01.2 reproducible UI build inputs.
- State: partially verified.
- Commit: `02e143b`.
- Behavior and touched modules: UI package manager pin, Corepack installation in
  `ui/Dockerfile`, and local-demo packaging contract test.
- Prerequisites/review authorization: packaging-only change; no persistence or
  authorization contract change.
- Red test and actual failure: no test initially required the container to honor
  a declared package manager; the Dockerfile used whichever pnpm Corepack chose.
- Green commands and results: local architecture contract test, Ruff, and Ty
  passed. `apps/governance-ui/node_modules/.bin/tsc -b` and Vite build passed;
  output JavaScript was 209.58 KiB, gzip 66.03 KiB. `pnpm run` itself waited on
  a Corepack/environment lock after pinning, so the direct local binaries provide
  partial build evidence only.
- Browser/API/PostgreSQL/consumer evidence: none; Docker build and local stack
  remain unverified.
- Manual/independent review: none.
- Remaining limitations/blocker: Corepack's wrapper waited on a local environment
  lock after pinning; direct local binaries supplied partial build evidence. The
  actual Docker build must run from a clean checkout.
- Next action: resolve tool lock, inspect the built wheel entry point, and run
  Compose when the local container runtime is available.

- Packet/slice: P01.3 UI image build-context exclusion.
- State: partially verified.
- Commit: `851d6b5`.
- Behavior and touched modules: the root `.dockerignore` now excludes recursive
  JavaScript dependency directories, the pnpm store, and the generated governance
  UI distribution from the Compose UI image context. The packaging contract test
  asserts these exclusions.
- Prerequisites/review authorization: independent image hygiene; no session,
  persistence, or authorization behavior changes.
- Red test and actual failure: the UI Dockerfile copied the full application
  directory after install, while the repository context allowed host
  `node_modules`, pnpm artifacts, and the prebuilt UI distribution into that
  copy layer.
- Green commands and results: `uv run --no-sync pytest
  tests/architecture/test_local_demo_ui.py tests/examples/test_ui_smoke.py -q`
  → 3 passed; focused Ruff and Ty checks passed. An attempted Ruff invocation
  on `.dockerignore` was invalid because it is not Python; the subsequent focused
  Python check passed.
- Browser/API/PostgreSQL/consumer evidence: static image-context contract only;
  Docker build remains blocked by the unavailable local Podman connection.
- Manual/independent review: none.
- Remaining limitations/blocker: the attempted `docker compose config --quiet`
  and `docker info` command sequence returned a Podman connection error. Successful
  Compose validation was not captured and must not be inferred from that output.
- Next action: perform a clean Docker UI build and full Compose smoke once the
  container runtime is available.

- Packet/slice: P01.4 deterministic demo readiness ordering.
- State: partially verified.
- Commit: `789f7d1`.
- Behavior and touched modules: enables Keycloak's health endpoint and uses its
  documented container-local readiness probe; makes setup, control-plane UI,
  Flight, and client dependencies wait for healthy upstream services; enables the
  data-plane's existing HTTP readiness server on the internal port `8816` and
  checks it after setup publishes runtime configuration.
- Prerequisites/review authorization: local startup orchestration only; no
  session, persistence, authorization, or pickle behavior change.
- Red test and actual failure: the parsed Compose baseline showed Keycloak,
  control-plane, Flight, and client consumers depending on mere
  `service_started`, allowing provisioning or use before an upstream service was
  ready.
- Green commands and results: `uv run --no-sync pytest
  tests/architecture/test_keycloak_demo_readiness.py
  tests/examples/test_keycloak_demo_fixture.py
  tests/architecture/test_local_demo_ui.py -q` → 4 passed; focused Ruff and Ty
  checks passed. Readiness assertions parse Compose YAML. The fixture test checks
  UI environment values but does not assert the new data-plane health variables.
- Browser/API/PostgreSQL/consumer evidence: static Compose and generated-runtime
  configuration evidence only. Keycloak's documented health approach informed
  the check; no local container process was started.
- Manual/independent review: none.
- Remaining limitations/blocker: generating local ignored configuration worked,
  but `docker compose config --quiet` still cannot contact the configured Podman
  socket at `127.0.0.1:55305`. Full startup, TLS, deep-link, cache, graceful-stop,
  and Flight-read evidence remains pending. Data-plane HTTP readiness checks
  publication configuration, not actual Flight RPC admission. Routine setup still
  drops/reseeds fixtures and overwrites policy state; P01 remains incomplete.
- Next action: run Compose from a clean checkout when the container runtime is
  available, then exercise UI and Flight smoke checks against the healthy stack.

- Packet/slice: P01.5 UI delivery cache and missing-asset behavior.
- State: partially verified.
- Commit: `b5c77a7`.
- Behavior and touched modules: uses the unprivileged NGINX image for the UI
  runtime; caches content-hashed `/assets/` with a long expiry; serves the SPA
  shell with an expired cache response; and returns 404 for absent assets rather
  than falling through to `index.html`.
- Prerequisites/review authorization: packaging-only change; no authentication,
  authorization, session, or persistence behavior change.
- Red test and actual failure: the UI deployment contract had no requirement for
  non-root NGINX, asset cache behavior, or a true missing-asset response. The
  previous catch-all location would serve the SPA document for an absent
  JavaScript asset.
- Green commands and results: `uv run --no-sync pytest
  tests/architecture/test_local_demo_ui.py tests/examples/test_ui_smoke.py -q`
  → 3 passed; focused Ruff and Ty checks passed.
- Browser/API/PostgreSQL/consumer evidence: NGINX configuration contract only;
  the unavailable Podman runtime prevents an HTTP deep-link, cache-header, and
  missing-asset probe against a built image.
- Manual/independent review: none.
- Remaining limitations/blocker: build compatibility of the unprivileged NGINX
  image and all runtime cache/CSP behavior require clean container execution.
- Next action: build the UI image and check root/deep link, a missing `/assets/`
  path, CSP, cache headers, and graceful termination when a runtime is available.

- Packet/slice: P01.6 accurate local-demo security claims.
- State: verified documentation correction.
- Commit: `49d0751`.
- Behavior and touched modules: the Keycloak demo guide now identifies the
  password-exchange cookie as a disposable HTTP-demo path and states that it does
  not satisfy authorization-code/PKCE login, opaque revocable sessions, scoped
  administrative authorization, browser testing, or supported local-security
  parity.
- Prerequisites/review authorization: documentation-only correction; no runtime
  behavior or security contract changed.
- Red test and actual failure: the guide called the demo secure and described its
  raw provider-token cookie as a browser session, which contradicted the P00/P02
  handoff assessment.
- Green commands and results: `git diff --check` passed.
- Browser/API/PostgreSQL/consumer evidence: none; this change corrects claims to
  match current observed limitations.
- Manual/independent review: none.
- Remaining limitations/blocker: P02 and P03 implementation remains gated on
  the P00 contract review.
- Next action: retain this disclaimer until the real local OIDC/session and
  capability paths are independently verified.

- Packet/slice: P11.1 local UI smoke verifier hardening.
- State: implemented-unverified.
- Commit: `136b508`.
- Behavior and touched modules: replaces optimization-removable assertions with
  explicit safe failures in `ui_smoke.py`; adds success/failure unit coverage.
- Prerequisites/review authorization: local verification helper only; it does
  not change the session or authorization implementation.
- Red test and actual failure: prior helper relied on Python `assert`, which
  disappears under `python -O` and could report a false success.
- Green commands and results: `uv run --no-sync pytest
  tests/examples/test_ui_smoke.py tests/architecture/test_local_demo_ui.py -q`
  → 3 passed; focused Ruff, Ty and formatting checks passed.
- Browser/API/PostgreSQL/consumer evidence: mocked HTTP contract only; real stack
  execution remains required.
- Manual/independent review: none.
- Remaining limitations/blocker: demo login itself remains the obsolete temporary
  path described in P02; this verifier must change with the real login flow.
- Next action: execute against a running local stack after P01 packaging works.

- Packet/slice: P01.5 restart-safe demo initialization.
- State: implemented-unverified.
- Commit: `ba03170`.
- Behavior and touched modules: existing Iceberg tables are detected and kept
  during routine setup. The control-plane provisioner classifies the workspace
  before issuing writes, reuses a complete fixture workspace, and stops on
  ambiguous partial state with recovery/reset guidance. New tests cover table
  preservation, first creation, complete-workspace reuse, and partial-state
  refusal.
- Prerequisites/review authorization: startup safety only; no session,
  authorization, persistence-schema, or pickle behavior changed.
- Red test and actual failure: the prior scripts unconditionally dropped and
  recreated the table and sent replacement catalog/asset/policy requests on
  every setup restart; no test protected operator-authored state.
- Green commands and results: `uv run --no-sync pytest
  tests/examples/test_demo_initialization.py
  tests/examples/test_keycloak_demo_fixture.py
  tests/architecture/test_keycloak_demo_readiness.py
  tests/architecture/test_local_demo_ui.py tests/examples/test_ui_smoke.py -q`
  → 10 passed. Focused Ruff lint and format checks passed.
- Browser/API/PostgreSQL/consumer evidence: unit-level fake catalog and HTTP
  responses only; container, PostgreSQL, browser, and consumer restart probes
  remain unverified while the local Podman runtime is unavailable.
- Manual/independent review: none.
- Remaining limitations/blocker: complete-state verification relies on the
  current summary, asset inventory, and policy-version history APIs; P01 still
  needs clean image/wheel/Compose evidence.
- Next action: finish the P01 runtime/packaging acceptance lane, then implement
  the reviewed P02 session contract before changing authorization routes.

- Packet/slice: P02.1 opaque browser sessions.
- State: implemented-unverified.
- Commit: `21d43b8`.
- Behavior and touched modules: adds a migrated `browser_sessions` table that
  stores only SHA-256 token digests, actor identity, groups, expiry, last-seen,
  and revocation state. Demo password exchange now resolves the provider token
  once and mints a random HttpOnly session secret; logout revokes it. Unsafe
  cookie requests retain double-submit CSRF checks and reject untrusted Origin
  headers.
- Prerequisites/review authorization: the P00 proposed session defaults were
  used for this additive slice; no pickle path changed. Authorization-code
  transaction storage, PKCE callback, nonce validation, and session cleanup are
  still required before production acceptance.
- Red test and actual failure: the browser cookie previously contained the raw
  provider access token and logout only expired the client cookie, leaving no
  server-side revocation path.
- Green commands and results: control-plane, migration, and browser-session
  tests passed (including expiry, digest-only persistence, revocation, CSRF,
  Origin, and logout); focused Ruff and Ty checks passed.
- Browser/API/PostgreSQL/consumer evidence: in-process FastAPI and SQLite
  migration tests only; no real IdP, Postgres, browser, or deployed stack proof.
- Manual/independent review: none.
- Remaining limitations/blocker: bearer admin bypass remains an operator
  bootstrap mechanism; the UI still exposes a demo-login shortcut and does not
  start an authorization-code flow.
- Next action: implement and test the OIDC authorization-code/PKCE login
  transaction and wire the UI to it before removing the demo-only path.

- Packet/slice: P02.2 authorization-code/PKCE browser login.
- State: implemented-unverified.
- Commit: `3df8c4d`.
- Behavior and touched modules: adds one-time server-side login transactions
  for state, nonce, PKCE verifier, redirect binding, expiry, and atomic
  consumption. The callback exchanges a public-client code, verifies the
  signed ID-token nonce through the OIDC JWKS provider, mints the opaque
  session, clears the transaction cookie, and redirects to an allowlisted
  configured location. NGINX proxies `/auth/`; the UI presents SSO as its
  primary sign-in action and fixes CSRF-header request construction.
- Prerequisites/review authorization: additive OIDC boundary implementation;
  no pickle path changed. The UI demo password shortcut remains available only
  when explicitly configured for local fixtures.
- Red test and actual failure: the UI had no authorization-code route and would
  only use the temporary password-grant shortcut; a callback could not bind
  state, PKCE, nonce, or a server-side session.
- Green commands and results: OIDC login tests cover S256 challenge, state
  cookie, one-time replay rejection, nonce failure, token exchange, and opaque
  session issuance. Route inventory, control-plane tests, migration tests,
  Ruff, Ty, TypeScript, and Vite build checks passed.
- Browser/API/PostgreSQL/consumer evidence: mocked OIDC resolver and SQLite
  FastAPI tests plus local TypeScript/Vite build; no real IdP, Postgres,
  browser, container, or Flight consumer evidence.
- Manual/independent review: none.
- Remaining limitations/blocker: login abuse limits, trusted proxy/origin
  configuration, provider logout/reauthentication, `__Host-` production cookie
  policy, and rate-limited audit events remain. The callback currently requires
  an ID token and does not retain provider tokens.
- Next action: add capability-scoped draft/publication APIs and wire all UI
  management views to them; remove demo login from the supported production
  profile.

- Packet/slice: P03.1 scoped inventory and policy reads.
- State: implemented-unverified.
- Commit: `21d43b8`.
- Behavior and touched modules: non-admin actors see only assets whose owner
  principal or group matches their identity; asset detail, policy rules,
  previews, and policy-version history enforce the same owner boundary. Catalog
  inventory and runtime/auth-provider settings are admin-only. The repository
  uses a principal-filtered join for asset inventory instead of per-asset
  authorization queries.
- Prerequisites/review authorization: additive least-privilege enforcement on
  existing routes; no migration or pickle behavior outside the session table.
- Red test and actual failure: the production review's authenticated outsider
  probe returned 200 for all inventory and policy reads, exposing workspace
  configuration across owners.
- Green commands and results: negative actor tests now assert empty scoped
  inventory or HTTP 403 for foreign assets and admin-only settings; the full
  control-plane, architecture, migration, and service test lanes passed with
  focused Ruff and Ty checks.
- Browser/API/PostgreSQL/consumer evidence: in-process FastAPI and SQLite only;
  no multi-tenant Postgres or browser authorization matrix has been run.
- Manual/independent review: none.
- Remaining limitations/blocker: write capability separation, drafts,
  evaluations, direct-ID pagination, operations, audit, and tenant/cell grant
  records remain to be implemented. Historical baseline text above records the
  pre-fix outsider probe and should not be read as current behavior.
- Next action: thread actor context through remaining management and publication
  routes, then add durable draft/evaluation/operation records.

- Packet/slice: P03.2 durable asset capabilities.
- State: implemented-unverified.
- Commit: `e654165`.
- Behavior and touched modules: adds an `asset_grants` migration and repository
  support for explicit `read`, `edit`, `publish`, and `grant` capabilities.
  Asset owners retain broad compatibility capabilities; owners can delegate a
  narrower read grant through the new authenticated grants API. Inventory uses
  a filtered owner/grant join, policy editing requires `edit`, publication
  requires `publish`, and grant management requires `grant`.
- Prerequisites/review authorization: additive authorization hardening on the
  reviewed single-workspace model; no pickle path changed.
- Red test and actual failure: an authenticated outsider could not be safely
  delegated read-only access because the service had no durable capability
  record and every non-admin owner check was binary.
- Green commands and results: focused control-plane, migration, service, and
  actor negative-matrix tests passed; Ruff and Ty checks passed.
- Browser/API/PostgreSQL/consumer evidence: in-process FastAPI and SQLite only;
  Postgres migration and browser grant-management probes remain open.
- Manual/independent review: none.
- Remaining limitations/blocker: tenant/cell grants, management capability
  separation, drafts, evaluations, operations, and audit are still incomplete.
  Existing owner rows remain an intentional compatibility broad grant until
  migration tooling can make them explicit.
- Next action: implement the OIDC authorization-code/PKCE transaction and
  connect the UI to the scoped asset API.

### Follow-through implementation slices — 2026-09-12

- Packet/slice: P04.1 UI request lifecycle and publication submission safety.
- State: implemented-unverified.
- Commits: `c1853b4`, `8c4a162`, `9b3ca6b`.
- Behavior: stale workspace and management responses are ignored after a newer
  request or logout; publish is disabled while in flight and sends a fresh
  `Idempotency-Key`; review tokens are cleared whenever the draft changes.
- Green evidence: TypeScript no-emit and Vite production builds pass. Browser
  automation and keyboard/screen-reader review remain open.

- Packet/slice: P05.1 authoritative nested schema.
- State: implemented-unverified.
- Commits: `37e3234`, `0f93478`.
- Behavior: the control plane loads the configured Iceberg schema, emits typed
  recursive struct/list/map nodes with stable field paths, and the UI renders
  those paths without flattening collection boundaries.
- Green evidence: schema service/API and UI build tests pass. Real Iceberg
  catalog, object-store, DuckDB, Spark, and Arrow Flight probes remain open.

- Packet/slice: P06.1 revisioned personal drafts.
- State: implemented-unverified.
- Commit: `8d9b44d`.
- Behavior: personal drafts persist canonical rules with revision CAS, content
  hashes, and stale-writer conflicts; publication consumes the saved draft.
- Green evidence: draft service/API and migration tests pass. PostgreSQL
  backup/restore and multi-process conflict evidence remain open.

- Packet/slice: P07.1 bounded synthetic evaluation.
- State: implemented-unverified.
- Commit: `3dbdf26`.
- Behavior: server evaluation uses the authoritative schema and policy resolver,
  bounds test rows, applies DuckDB transforms, and returns redacted evidence
  bound to the current draft revision/content hash.
- Green evidence: evaluation and policy tests pass. A production-sized fixture,
  latency budget, and independent data-leak review remain open.

- Packet/slice: P08.1 exact review, atomic publication, and retry safety.
- State: implemented-unverified.
- Commits: `37d2cf4`, `e69e83b`.
- Behavior: production publication requires a server-signed review token for
  the exact draft and active generation; activation is CAS-protected; repeated
  idempotent requests replay the committed result and mismatched bodies conflict.
- Green evidence: review/publication API tests pass. Concurrent PostgreSQL
  races, operation lookup/retention, and an explicit deny-all publication
  decision remain open.

- Packet/slice: P09.1 history, restore, audit, and runtime observations.
- State: implemented-unverified.
- Commits: `69e5a13`, `2c8f4d2`, `c1f6e9e`.
- Behavior: immutable asset history can be restored into a new CAS draft;
  draft/restore/publication mutations append bounded audit events; Activity can
  show active control-plane generation while explicitly labeling Flight health
  as unobserved.
- Green evidence: history, audit, workspace, migration, route-inventory, and UI
  build tests pass. Flight health probes, retention jobs, and operator alerting
  remain open.

- Packet/slice: P13.1 production database fail-closed guard.
- State: implemented-unverified.
- Commit: `fff1b54`.
- Behavior: production startup rejects SQLite or other local databases before
  engine creation, while local profile behavior remains unchanged.
- Green evidence: control-plane CLI tests pass. Clean PostgreSQL image startup,
  TLS ingress, and migration/rollback evidence remain open.

## Slice evidence template

Copy this section for each slice; replace every placeholder with observed evidence.

- Packet/slice:
- State:
- Commit:
- Behavior and touched modules:
- Prerequisites/review authorization:
- Red test and actual failure:
- Green commands and results:
- Browser/API/PostgreSQL/consumer evidence:
- Manual/independent review:
- Remaining limitations/blocker:
- Next action:
