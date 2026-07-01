from __future__ import annotations

import importlib

from sqlalchemy import inspect

from dal_obscura.common.config_store.db import create_engine_from_url

cli = importlib.import_module("dal_obscura.data_plane.interfaces.cli.main")


def test_data_plane_startup_checks_schema_without_migrating(tmp_path, monkeypatch) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'config.db'}"
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", database_url)
    monkeypatch.setenv("DAL_OBSCURA_CELL_ID", "00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv("DAL_OBSCURA_LOCATION", "grpc://127.0.0.1:8815")
    monkeypatch.setenv("DAL_OBSCURA_TICKET_SECRET", "secret")

    try:
        cli.main()
    except Exception as exc:
        assert "dal-obscura-migrate upgrade" in str(exc)
    else:
        raise AssertionError("data plane started without migrated schema")

    engine = create_engine_from_url(database_url)
    assert inspect(engine).get_table_names() == []
