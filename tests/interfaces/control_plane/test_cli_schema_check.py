from __future__ import annotations

from sqlalchemy import inspect

from dal_obscura.common.config_store.db import create_engine_from_url
from dal_obscura.control_plane.interfaces import cli


def test_control_plane_startup_checks_schema_without_migrating(tmp_path, monkeypatch) -> None:
    database_url = f"sqlite+pysqlite:///{tmp_path / 'config.db'}"
    monkeypatch.setenv("DAL_OBSCURA_DATABASE_URL", database_url)
    monkeypatch.setenv("DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN", "admin")

    def fail_run(*args, **kwargs):
        raise AssertionError("uvicorn.run should not be reached when schema is missing")

    monkeypatch.setattr(cli.uvicorn, "run", fail_run)

    try:
        cli.main()
    except Exception as exc:
        assert "dal-obscura-migrate upgrade" in str(exc)
    else:
        raise AssertionError("control plane started without migrated schema")

    engine = create_engine_from_url(database_url)
    assert inspect(engine).get_table_names() == []
