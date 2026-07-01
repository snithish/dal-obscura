from __future__ import annotations

from pathlib import Path

import pytest

from dal_obscura.common.config_store import db


def _pyproject_text() -> str:
    return Path("pyproject.toml").read_text()


def test_postgres_driver_is_optional_extra_not_core_dependency() -> None:
    pyproject = _pyproject_text()
    dependencies = pyproject.split("[project.optional-dependencies]", 1)[0]

    assert "psycopg[binary]>=3.2.0" not in dependencies
    assert 'postgres = ["psycopg[binary]>=3.2.0"]' in pyproject


def test_sqlite_extra_is_available_for_install_selection() -> None:
    pyproject = _pyproject_text()

    assert "sqlite = []" in pyproject


def test_package_data_includes_all_alembic_migration_files() -> None:
    pyproject = _pyproject_text()

    assert '"migrations/env.py"' in pyproject
    assert '"migrations/script.py.mako"' in pyproject
    assert '"migrations/versions/*.py"' in pyproject


def test_missing_postgres_driver_error_points_to_postgres_extra(monkeypatch) -> None:
    def raise_missing_driver(*args, **kwargs):
        raise ModuleNotFoundError("No module named 'psycopg'")

    monkeypatch.setattr(db, "create_engine", raise_missing_driver)

    with pytest.raises(RuntimeError, match=r"dal-obscura\[postgres\]"):
        db.create_engine_from_url("postgresql+psycopg://user:pass@localhost/db")
