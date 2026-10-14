from __future__ import annotations

import importlib

from sqlalchemy import inspect

cli = importlib.import_module("dal_obscura.interfaces.cli.read")


def test_data_plane_startup_checks_schema_without_migrating(
    engine_factory, tmp_path, monkeypatch
) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'config.db'}"
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", database_url)
    monkeypatch.delenv("DAL_OBSCURA_CELL_ID", raising=False)
    monkeypatch.setenv("DAL_OBSCURA_LOCATION", "grpc://127.0.0.1:8815")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "secret")

    try:
        cli.main()
    except Exception as exc:
        assert "dal-obscura-migrate upgrade" in str(exc)
    else:
        raise AssertionError("data plane started without migrated schema")

    engine = engine_factory(database_url)
    assert inspect(engine).get_table_names() == []


def test_data_plane_help_does_not_require_runtime_environment(monkeypatch, capsys) -> None:
    monkeypatch.setattr(cli.sys, "argv", ["dal-obscura", "--help"])

    cli.main()

    output = capsys.readouterr().out
    assert "governed Arrow Flight data plane" in output
    assert "DAL_OBSCURA_DATABASE_URL" in output
