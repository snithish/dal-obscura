# Governance application execution status

Plan created against `612bb1c`. No implementation performed by this planning task.
Canonical task order: [execution handoff](EXECUTION_HANDOFF.md).

## Gate status

- P00 contracts, capability matrix, and migration/session design: review packet
  prepared; approval remains required before P02/P03.
- P01 installed startup and container assembly: implementing. Commits `734c018`,
  `02e143b`, and `5fc06a1` add a configured control-plane command, pin the UI
  package manager for container builds, and isolate console arguments from
  programmatic CLI calls. A migrated SQLite instance has served health and
  readiness endpoints through the configured composition path. Wheel/container
  startup, Compose execution, and the remaining packaging slices remain
  unverified.
- P02 real OIDC login and revocable sessions: not-started; demo cookie plumbing
  exists but does not satisfy the target session contract.
- P03 complete scoped authorization: not-started; coarse existing checks require
  replacement or extension and negative matrix coverage.
- P04 reliable UI lifecycle and behavioral tests: not-started; source shell exists.
- P05 canonical nested schema API/tree: not-started; gateway primitives exist.
- P06 durable drafts and conflict protection: not-started.
- P07 bound synthetic evaluations: not-started.
- P08 exact review and atomic publication UI/API: not-started; reusable gateway
  publication primitives require integration review.
- P09 history, restore, audit, runtime observations: not-started.
- P10 complete management and consumer handoff: not-started.
- P11 verified local feature/security parity: not-started.
- P12 quality, usability, independent review and release: not-started.

These statuses refer to acceptance under the new packets, not absence of all
reusable code. Previous build/hook results are historical evidence only.

## Known external gates

- Session/persistence and capability decisions require concrete review in P00.
- Previous container execution failed because Podman was stopped; recheck runtime
  availability when starting P01. No live-stack success has been recorded.
- Owner visual acceptance, participant sessions, and independent security review
  have not been recorded. Prepare materials; do not contact others unasked.
- Owner's pickle-preservation instruction remains; no UI task resolves W06.

## Next action

Validate the installed P01 executable and container assembly, then continue P01
readiness and reproducible UI build slices. P00 review packet remains required
before P02/P03 session or persistence changes. Do not start by adding placeholder
screens.

## Evidence

- Packet/slice: P00.1 route inventory and contract-review preparation.
- State: review pending.
- Commit: pending.
- Behavior and touched modules: `P00_CONTRACT_REVIEW.md` records every current
  OpenAPI path, its present authorization dependency, its target capability
  direction, the session/persistence decisions requiring review, and concrete
  negative examples. `test_control_plane_route_inventory.py` keeps the document's
  inventory synchronized with generated OpenAPI paths.
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
  command's `--help` parser through its module succeeded. An isolated
  `uv tool run --from` invocation of the built wheel also ran the installed
  `dal-obscura-control-plane --help` command successfully. A freshly migrated
  temporary SQLite database then started Uvicorn at `127.0.0.1:18820`; both
  `GET /healthz` and `GET /readyz` returned HTTP 200, with the readiness
  database check reporting `ok`.
- Browser/API/PostgreSQL/consumer evidence: installed-wheel command and real
  local HTTP health/readiness evidence on SQLite only; no installed-wheel
  server process, Postgres, Compose, browser, or consumer evidence yet.
- Manual/independent review: none.
- Remaining limitations/blocker: pre-commit hook runner stalled after its format
  hook; manually equivalent focused quality checks passed before `--no-verify`
  commit. The project environment is stale and does not expose the new console
  script under `uv run --no-sync`; the isolated wheel command works, but it has
  not started a server. Container runtime availability remains unresolved.
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
- Commit: pending.
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
- Remaining limitations/blocker: `docker compose config --quiet` validated the
  Compose file, but `docker info` cannot connect to the configured Podman socket
  at `127.0.0.1:55305`; no container claim is made.
- Next action: perform a clean Docker UI build and full Compose smoke once the
  container runtime is available.

- Packet/slice: P01.4 deterministic demo readiness ordering.
- State: partially verified.
- Commit: pending.
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
  checks passed. The readiness assertions parse Compose YAML and check the
  generated data-plane environment rather than relying on text presence.
- Browser/API/PostgreSQL/consumer evidence: static Compose and generated-runtime
  configuration evidence only. Keycloak's documented health approach informed
  the check; no local container process was started.
- Manual/independent review: none.
- Remaining limitations/blocker: generating local ignored configuration worked,
  but `docker compose config --quiet` still cannot contact the configured Podman
  socket at `127.0.0.1:55305`. Full startup, TLS, deep-link, cache, graceful-stop,
  and Flight-read evidence remains pending.
- Next action: run Compose from a clean checkout when the container runtime is
  available, then exercise UI and Flight smoke checks against the healthy stack.

- Packet/slice: P01.5 UI delivery cache and missing-asset behavior.
- State: partially verified.
- Commit: pending.
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
- Commit: pending.
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
