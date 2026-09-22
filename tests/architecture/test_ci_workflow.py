from __future__ import annotations

from pathlib import Path


def test_ci_exercises_packaged_migration_and_driver_extras() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    assert "name: Python package smoke" in workflow
    assert "uv build" in workflow
    assert "wheel_path" in workflow
    assert "[server,postgres]" in workflow
    assert "dal-obscura-control-plane --help" in workflow
    assert "dal-obscura-migrate upgrade" in workflow
    assert "dal-obscura-migrate check" in workflow


def test_ci_python_jobs_have_single_clear_responsibilities() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    assert "name: Python quality" in workflow
    assert "name: Python contract and security tests" in workflow
    assert "tests/plugin_platform/test_registry.py" in workflow
    assert "name: Python integration tests" in workflow
    assert "name: PostgreSQL concurrency gates" in workflow
    assert "DAL_OBSCURA_POSTGRES_TEST_URL" in workflow
    assert "tests/integration/test_recovery_upgrade.py" in workflow
    assert "tests/consumers/test_governed_reads.py" in workflow
    assert "DAL_OBSCURA_RUN_CONSUMER_TESTS" in workflow
    assert "timeout-minutes: 20" in workflow
    assert "name: Python package smoke" in workflow
    assert "name: Build, scan, and publish image" in workflow


def test_ci_renders_each_deployment_profile_before_contracts() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    assert "name: Render deployment profiles" in workflow
    assert "docker compose --env-file deployment/production/.env.example" in workflow
    assert "docker compose --env-file deployment/local-secure/.env.example" in workflow


def test_ci_requires_locked_dependency_audit_before_image_promotion() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    assert "name: Dependency vulnerability audit" in workflow
    assert "uv export --frozen --no-dev --extra server --extra postgres" in workflow
    assert "uvx --from pip-audit pip-audit" in workflow
    assert "pnpm --dir apps/governance-ui audit --prod --audit-level high" in workflow
    container_block = workflow.split("  container:\n", 1)[1]
    assert "      - dependency-audit" in container_block


def test_ci_governance_ui_runs_lifecycle_tests_before_build() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    assert "pnpm --dir apps/governance-ui test" in workflow


def test_ci_promotes_the_exact_scanned_image_digest() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    assert "id: image" in workflow
    assert "steps.image.outputs.digest" in workflow
    assert "Scan exact candidate digest" in workflow
    assert "docker buildx imagetools create --tag" in workflow
    assert "name: Publish image" not in workflow
    assert "candidate-{2}" in workflow
    assert "github.sha" in workflow


def test_ci_generates_plugin_lock_from_installed_wheels() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    assert "Generate and validate lock from installed plugin wheels" in workflow
    assert "scripts/build_plugin_lock.py" in workflow
    assert "--plugin catalog:iceberg.rest" in workflow
    assert "--plugin catalog:manifest" in workflow
    assert "--plugin table_format:parquet.dataset" in workflow
    assert "load_plugin_lock_file" in workflow


def test_ci_builds_scans_and_records_the_exact_ui_image() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    assert "name: Build, scan, and publish UI image" in workflow
    assert "file: ui/Dockerfile" in workflow
    assert "Scan exact UI candidate digest" in workflow
    assert "Promote the scanned immutable UI digest" in workflow
    assert "ui_digest: ${{ steps.image.outputs.digest }}" in workflow
    assert "Write candidate release manifest" in workflow
    assert "needs.ui-image.outputs.ui_digest" in workflow
    assert "actions/upload-artifact@v4" in workflow


def test_ci_binds_published_candidates_to_attestations_and_lanes() -> None:
    workflow = Path(".github/workflows/ci.yml").read_text()

    assert "name: Verify pushed candidate attestations" in workflow
    assert "docker buildx imagetools inspect" in workflow
    assert "vnd.docker.reference.type" in workflow
    assert "jq -e" in workflow
    assert "https://slsa.dev/provenance/v1" in workflow
    assert "https://spdx.dev/Document" in workflow
    assert "server_attestation_index_sha256" in workflow
    assert "ui_attestation_index_sha256" in workflow
    assert "server_attestation_evidence_sha256" in workflow
    assert "ui_attestation_evidence_sha256" in workflow
    assert "workflow_run_id" in workflow
    assert "mandatory_lanes" in workflow
    assert "not-published-on-pull-request" in workflow
