from __future__ import annotations

import json

import pytest

from dal_obscura.common.plugin_api import PluginAdmissionError, load_plugin_lock_file


def _write_lock(path, entries) -> None:
    path.write_text(json.dumps({"version": 1, "plugins": entries}))


def test_load_plugin_lock_file_returns_five_part_allowlist(tmp_path) -> None:
    path = tmp_path / "plugins.json"
    _write_lock(
        path,
        [
            {
                "kind": "catalog",
                "plugin_id": "fixture.catalog",
                "lock": ["fixture", "1.0.0", "1", "a" * 64, "b" * 64],
            }
        ],
    )

    assert load_plugin_lock_file(path) == {
        ("catalog", "fixture.catalog"): ("fixture", "1.0.0", "1", "a" * 64, "b" * 64)
    }


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"version": 2, "plugins": []}, "version"),
        ({"version": 1, "plugins": []}, "bounded"),
        (
            {
                "version": 1,
                "plugins": [{"kind": "catalog", "plugin_id": "fixture.catalog", "lock": ["x"]}],
            },
            "five-part",
        ),
    ],
)
def test_load_plugin_lock_file_rejects_invalid_documents(tmp_path, document, message) -> None:
    path = tmp_path / "plugins.json"
    path.write_text(json.dumps(document))

    with pytest.raises(PluginAdmissionError, match=message):
        load_plugin_lock_file(path)


def test_load_plugin_lock_file_rejects_symlink(tmp_path) -> None:
    target = tmp_path / "target.json"
    _write_lock(target, [])
    link = tmp_path / "plugins.json"
    link.symlink_to(target)

    with pytest.raises(PluginAdmissionError, match="symbolic"):
        load_plugin_lock_file(link)


def test_load_plugin_lock_file_rejects_duplicate_document_keys(tmp_path) -> None:
    path = tmp_path / "plugins.json"
    path.write_text(
        '{"version":1,"version":1,"plugins":[{"kind":"catalog",'
        '"plugin_id":"fixture.catalog","lock":["fixture","1.0.0","1",'
        '"' + "a" * 64 + '","' + "b" * 64 + '"]}]}'
    )

    with pytest.raises(PluginAdmissionError, match="valid JSON"):
        load_plugin_lock_file(path)


@pytest.mark.parametrize(
    ("plugin_id", "lock", "message"),
    [
        ("Fixture.catalog", ["fixture", "1.0.0", "1", "a" * 64, "b" * 64], "identity"),
        ("fixture.catalog", ["fixture", "1.0.0", "1", "A" * 64, "b" * 64], "digests"),
        ("fixture.catalog", ["fixture", "1.0.0", "1", "a" * 63, "b" * 64], "digests"),
    ],
)
def test_load_plugin_lock_file_rejects_malformed_identity_or_digest(
    tmp_path,
    plugin_id,
    lock,
    message,
) -> None:
    path = tmp_path / "plugins.json"
    _write_lock(
        path,
        [{"kind": "catalog", "plugin_id": plugin_id, "lock": lock}],
    )

    with pytest.raises(PluginAdmissionError, match=message):
        load_plugin_lock_file(path)
