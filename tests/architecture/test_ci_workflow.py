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
    assert "name: Python integration tests" in workflow
    assert "name: PostgreSQL concurrency gates" in workflow
    assert "DAL_OBSCURA_POSTGRES_TEST_URL" in workflow
    assert "timeout-minutes: 20" in workflow
    assert "name: Python package smoke" in workflow
    assert "name: Build, scan, and publish image" in workflow


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
