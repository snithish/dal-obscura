from __future__ import annotations

from importlib import metadata
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from dal_obscura_plugin_api import PluginKind

from dal_obscura.common.plugin_api.registry import PluginAdmissionError
from scripts.build_plugin_lock import build_document, parse_selection, write_lock


def test_parse_selection_requires_an_admitted_plugin_kind() -> None:
    assert parse_selection("catalog:iceberg.sql") == ("catalog", "iceberg.sql")
    assert parse_selection("table_format:parquet.dataset") == (
        "table_format",
        "parquet.dataset",
    )
    with pytest.raises(PluginAdmissionError):
        parse_selection("format:parquet.dataset")


def test_build_document_uses_static_descriptor_and_sorts_rows(monkeypatch) -> None:
    distribution = SimpleNamespace(name="fixture-dist", version="1.2.3")
    entries: list[tuple[PluginKind, metadata.EntryPoint]] = [
        (
            "catalog",
            metadata.EntryPoint(name="zeta", value="pkg:zeta", group="dal_obscura.catalogs.v1"),
        ),
        (
            "catalog",
            metadata.EntryPoint(name="alpha", value="pkg:alpha", group="dal_obscura.catalogs.v1"),
        ),
    ]
    for _kind, entry in entries:
        object.__setattr__(entry, "dist", distribution)
    monkeypatch.setattr(
        "scripts.build_plugin_lock.load_static_plugin_descriptor",
        lambda entry: SimpleNamespace(
            kind="catalog",
            plugin_id=str(entry.name),
            api_version="1",
            config_version=1,
            distribution="fixture-dist",
            version="1.2.3",
        ),
    )
    monkeypatch.setattr(
        "scripts.build_plugin_lock.build_plugin_lock",
        lambda _kind, entry, _descriptor: (
            "fixture-dist",
            "1.2.3",
            "1",
            "a" * 64,
            "b" * 64,
        ),
    )

    document = build_document(
        [("catalog", "zeta"), ("catalog", "alpha")], entry_points=entries
    )

    assert document["version"] == 1
    rows = cast(list[dict[str, object]], document["plugins"])
    assert [row["plugin_id"] for row in rows] == ["alpha", "zeta"]


def test_write_lock_is_atomic_and_refuses_overwrite(tmp_path: Path) -> None:
    output = tmp_path / "plugin-lock.json"
    write_lock(output, {"version": 1, "plugins": []})
    assert output.read_text().endswith("\n")
    with pytest.raises(PluginAdmissionError, match="refusing to overwrite"):
        write_lock(output, {"version": 1, "plugins": []})
