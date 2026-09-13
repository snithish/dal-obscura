from __future__ import annotations

from pathlib import Path


def test_operator_docs_describe_factory_free_plugin_lock_generation() -> None:
    docs = Path("docs/operators.md").read_text()

    assert "scripts/build_plugin_lock.py" in docs
    assert "--plugin catalog:iceberg.rest" in docs
    assert "--plugin catalog:manifest" in docs
    assert "--plugin table_format:parquet.dataset" in docs
    assert "importing plugin factories" in docs
    assert "refuses to overwrite" in docs
