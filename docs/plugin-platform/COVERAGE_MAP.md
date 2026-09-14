# Acceptance coverage map

Updated 2026-09-14. This map names the primary behavioral owner for each acceptance
block. A `VERIFY` status means the implementation or focused regression exists but
the acceptance command still needs live, clean-artifact, multi-process, browser,
or independent evidence. Skips are explicit in the authoritative suite and do not
count as completion.

| Block | Primary behavioral owner | Current evidence | Remaining proof |
| --- | --- | --- | --- |
| G01 | `tests/control_plane/test_publication_compiler.py`, `tests/interfaces/control_plane/test_policy_versions_api.py`, `tests/integration/control_plane/test_publication_races.py` | Local transaction/CAS and publication tests pass | PostgreSQL two-process race lane (VERIFY) |
| G02 | `tests/domain/access_control/test_policy_resolution.py`, `tests/domain/access_control/test_row_filters.py`, `tests/infrastructure/adapters/test_duckdb_transform.py`, `tests/common/query_planning/test_field_paths.py` | Mask/filter/nested-path regressions pass | Real three-pair reads and 10k-node capacity (VERIFY) |
| G03 | `tests/plugin_platform/test_registry.py`, `tests/plugin_platform/test_lockfile.py`, `tests/infrastructure/adapters/test_path_rules.py`, `tests/integration/test_io_boundary.py` | Admission, lock, path, secret and boundary tests pass | Live destination counters, clean wheels, cancellation (VERIFY) |
| G04 | `tests/interfaces/control_plane/test_actor_auth.py`, `tests/interfaces/control_plane/test_oidc_login.py`, `tests/integration/test_recovery_upgrade.py`, `apps/governance-ui/tests/*.test.mjs` | Local auth/session/UI lifecycle checks pass | Real OIDC, browser a11y, cross-process revocation and recovery (VERIFY) |
| B01 | `tests/architecture/test_ci_workflow.py`, `docs/plugin-platform/BASELINE_20260914.md` | Baseline and CI contract recorded | Clean Node install/image/advisory evidence (VERIFY) |
| B02 | `pyproject.toml`, `uv.lock`, `apps/governance-ui/package.json`, UI lock, `tests/architecture/test_ci_workflow.py` | Locked runtime/toolchain checks pass | Independent clean-install execution (VERIFY) |
| B03 | `tests/plugin_platform/test_lockfile.py`, `tests/common/config_store/test_plugin_bindings.py`, `tests/architecture/test_package_boundaries.py` | Strict five-part locks and explicit conversion pass | Populated-record maintenance cutover (VERIFY) |
| B04 | `tests/control_plane/test_asset_service_plugins.py`, `tests/control_plane/test_publication_compiler.py`, `tests/interfaces/control_plane/test_plugins_api.py` | Explicit output-format/version/capability pairing passes | Real external pair and generated DTO/browser proof (VERIFY) |
| B05 | `tests/interfaces/control_plane/test_assets_api.py`, `tests/interfaces/control_plane/test_policy_versions_api.py`, `tests/interfaces/control_plane/test_schema_api.py`, `tests/interfaces/control_plane/test_actor_auth.py` | Revision, draft handoff, error-envelope and authority cases pass | Multi-process and rendered handoff journey (VERIFY) |
| B06 | `tests/control_plane/test_catalog_option_validation.py`, `tests/infrastructure/adapters/test_iceberg_phase0_regressions.py`, `tests/integration/test_io_boundary.py` | Secret/path/metadata/delete guards pass | Actual provider request counters and cancellation cleanup (VERIFY) |
| B07 | `tests/infrastructure/adapters/test_identity_claims.py`, `tests/infrastructure/adapters/test_identity_oidc_jwks.py`, `tests/interfaces/control_plane/test_actor_auth.py` | Exact issuer and typed principal regressions pass | Provider freshness/revocation timing (VERIFY) |
| B08 | `tests/interfaces/control_plane/test_oidc_login.py`, `tests/interfaces/control_plane/test_actor_auth.py`, `tests/architecture/test_secure_local_profile.py` | Cookie, CSRF, bootstrap and rate-limit checks pass | Real OIDC code/PKCE browser journey (VERIFY) |
| B09 | `tests/interfaces/control_plane/test_ui_shell.py`, `tests/architecture/test_local_demo_ui.py`, `apps/governance-ui/tests/*.test.mjs` | Shell, route, build and keyboard lifecycle checks pass | Rendered 390/768/1440, axe and screen-reader review (VERIFY) |
| B10 | `apps/governance-ui/src/lifecycle.ts`, `apps/governance-ui/src/async.ts`, `apps/governance-ui/tests/*.test.mjs` | Epoch/query/cancellation unit checks pass | Rendered deferred-response interleavings (VERIFY) |
| B11 | `tests/control_plane/test_policy_authorization.py`, `tests/control_plane/test_policy_drafts_api.py`, `apps/governance-ui/src/components/AssetWorkspace.tsx` | Lossless draft/mask/deny-all behavior passes | Browser authoring journey (VERIFY) |
| B12 | `tests/common/query_planning/test_field_paths.py`, `tests/common/test_schema_bounds.py`, `apps/governance-ui/src/components/AssetWorkspace.tsx` | Nested schema bounds and virtualized tree code exist | 10k-node rendered editing and responsive proof (VERIFY) |
| B13 | `tests/control_plane/test_policy_version_service.py`, `tests/interfaces/control_plane/test_policy_versions_api.py`, `tests/interfaces/control_plane/test_schema_api.py` | Review token, history and restore regressions pass | Browser review/publish and race evidence (VERIFY) |
| B14 | `tests/control_plane/test_catalog_discovery.py`, `tests/control_plane/test_asset_service_plugins.py`, `tests/plugin_platform/test_registry.py` | Catalog/plugin lifecycle and pair checks pass | Live provider lifecycle and worker propagation (VERIFY) |
| B15 | `tests/interfaces/control_plane/test_assets_api.py`, `tests/interfaces/control_plane/test_audit_api.py`, `tests/interfaces/control_plane/test_settings_api.py`, `tests/consumers/test_governed_reads.py` | Capability/audit/settings contracts pass; consumer node is opt-in | Real Python/DuckDB/Spark consumer cells (VERIFY) |
| B16 | `tests/integration/control_plane/test_publication_races.py`, `tests/interfaces/flight/test_service_streaming.py` | Local race/streaming regressions pass | Competing API processes, revocation and termination (VERIFY) |
| B17 | `packages/plugin-conformance/tests/test_conformance_runner.py`, `tests/plugin_conformance/test_pairs.py`, `tests/consumers/test_governed_reads.py`, `connectors/jvm` | Descriptor/conformance and stub consumer checks pass | Clean wheels, real SQL/REST/manifest datasets and Spark/JVM (VERIFY) |
| B18 | `.pre-commit-config.yaml`, `tests/architecture`, `docs/plugin-platform/BASELINE_20260914.md` | Docs-only hooks now skip broad checks; prose guards removed | Warm/cold timing target and path-aware CI evidence (VERIFY) |
| B19 | `tests/benchmarks`, `evaluation/capacity`, `scripts/run_capacity_benchmarks.sh`, `tests/interfaces/control_plane/test_audit_api.py` | Existing benchmark/capacity owners retained | Fixed-runner 10M-row, 1M-audit and 16-consumer measurements (VERIFY) |
| B20 | `deployment/production`, `deployment/local-secure`, `scripts`, `tests/integration/test_recovery_upgrade.py` | Deployment contracts and backup helper tests pass | Timed secure restore/rotation/restart (VERIFY) |
| B21 | `.github/workflows/ci.yml`, `tests/architecture/test_ci_workflow.py` | Candidate-chain workflow assertions pass | Exact artifact digests/SBOM/advisory execution (VERIFY) |
| B22 | `docs/plugin-platform/ACCEPTANCE.md`, this map, independent review record | No self-review claim made | Independent security/UX/product acceptance (VERIFY) |

The deleted `test_dead_code_inventory.py`, `test_operator_plugin_lock_docs.py`,
and `test_capacity_runbook.py` were prose/source-string guards. They have no
behavioral rows because their unique assertions were not product invariants; the
owners above retain the executable checks they previously shadowed. Pickle-based
serializers, serialized classes/import paths, and payload semantics are outside the
cleanup scope and remain protected.
