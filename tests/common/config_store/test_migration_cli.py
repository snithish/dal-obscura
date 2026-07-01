from __future__ import annotations

from pathlib import Path

from sqlalchemy import inspect

from dal_obscura.common.config_store.cli import run
from dal_obscura.common.config_store.db import create_engine_from_url


def _database_url(path: Path) -> str:
    return f"sqlite+pysqlite:///{path}"


def test_migration_cli_check_fails_before_upgrade(tmp_path: Path, capsys) -> None:
    database_url = _database_url(tmp_path / "config.db")

    code = run(["check", "--database-url", database_url])

    captured = capsys.readouterr()
    assert code == 1
    assert "dal-obscura-migrate upgrade" in captured.err


def test_migration_cli_upgrade_then_check_succeeds(tmp_path: Path, capsys) -> None:
    database_url = _database_url(tmp_path / "config.db")

    assert run(["upgrade", "--database-url", database_url]) == 0
    assert run(["check", "--database-url", database_url]) == 0

    captured = capsys.readouterr()
    assert "config-store schema upgraded to head" in captured.out
    assert "config-store schema is current" in captured.out


def test_migration_cli_current_prints_none_before_upgrade(tmp_path: Path, capsys) -> None:
    database_url = _database_url(tmp_path / "config.db")

    assert run(["current", "--database-url", database_url]) == 0

    captured = capsys.readouterr()
    assert captured.out.strip() == "none"


def test_migration_cli_reads_database_url_from_env(tmp_path: Path, monkeypatch) -> None:
    database_url = _database_url(tmp_path / "config.db")
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", database_url)

    assert run(["upgrade"]) == 0

    engine = create_engine_from_url(database_url)
    assert "alembic_version" in inspect(engine).get_table_names()
