from __future__ import annotations

import pytest

from dal_obscura.common.config_store import db


def test_missing_postgres_driver_error_points_to_postgres_extra(monkeypatch) -> None:
    def raise_missing_driver(*args, **kwargs):
        raise ModuleNotFoundError("No module named 'psycopg'")

    monkeypatch.setattr(db, "create_engine", raise_missing_driver)

    with pytest.raises(RuntimeError, match=r"dal-obscura\[postgres\]"):
        db.create_engine_from_url("postgresql+psycopg://user:pass@localhost/db")
