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
    assert "timeout-minutes: 20" in workflow
    assert "name: Python package smoke" in workflow
    assert "name: Build, scan, and publish image" in workflow
