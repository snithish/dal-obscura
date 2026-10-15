from __future__ import annotations

import json

import pytest

from dal_obscura.interfaces.cli import (
    control_client,
    operator,
    operator_io,
    plugin_ops,
)


def test_help_lists_operator_commands_without_runtime_environment(capsys):
    assert operator.main(["--help"]) == 0
    output = capsys.readouterr().out
    assert "plugin" in output
    assert "deploy" in output


def test_configure_stores_scoped_reference_without_resolving_secret(tmp_path, monkeypatch):
    descriptor = {
        "kind": "catalog",
        "plugin_id": "acme.delta",
        "distribution": "acme-delta-plugin",
        "version": "1.0",
        "api_version": "3",
        "config_version": 1,
        "config_schema": {
            "fields": [
                {"name": "uri", "type": "uri", "required": True},
                {"name": "token", "type": "secret_reference", "required": True},
            ]
        },
    }
    (tmp_path / "candidate.json").write_text(json.dumps({"descriptors": [descriptor]}))
    answers = iter(["https://catalog.example", "catalog-token", "catalog:warehouse"])
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    assert (
        operator.main(
            [
                "--workspace",
                str(tmp_path),
                "plugin",
                "configure",
                "acme.delta",
                "--name",
                "warehouse",
            ]
        )
        == 0
    )
    saved = json.loads((tmp_path / "catalogs.json").read_text())
    assert saved == {
        "warehouse": {
            "plugin_id": "acme.delta",
            "options": {
                "uri": "https://catalog.example",
                "token": {"secret": "catalog-token", "scope": "catalog:warehouse"},
            },
            "expected_revision": 0,
        }
    }


def test_invalid_configuration_preserves_previous_draft(tmp_path, capsys):
    descriptor = {
        "kind": "catalog",
        "plugin_id": "acme.delta",
        "distribution": "acme-delta-plugin",
        "version": "1.0",
        "api_version": "3",
        "config_version": 1,
        "config_schema": {"fields": [{"name": "token", "type": "secret_reference"}]},
    }
    (tmp_path / "candidate.json").write_text(json.dumps({"descriptors": [descriptor]}))
    previous = '{"warehouse": {}}\n'
    (tmp_path / "catalogs.json").write_text(previous)
    options = tmp_path / "options.json"
    options.write_text('{"token": "plaintext"}')
    assert (
        operator.main(
            [
                "--workspace",
                str(tmp_path),
                "plugin",
                "configure",
                "acme.delta",
                "--name",
                "warehouse",
                "--options",
                str(options),
            ]
        )
        == 1
    )
    assert (tmp_path / "catalogs.json").read_text() == previous
    assert "secret reference" in capsys.readouterr().err


@pytest.mark.parametrize("image", ["dal-obscura:latest", "server@sha256:bad"])
def test_add_rejects_mutable_or_invalid_base_before_docker(tmp_path, image, monkeypatch):
    (tmp_path / "target.json").write_text(json.dumps({"base_image": image}))
    monkeypatch.setattr(operator_io, "run", lambda *a, **kw: pytest.fail("Docker must not run"))
    assert operator.main(["--workspace", str(tmp_path), "plugin", "add", "acme-delta-plugin"]) == 1


def test_failed_package_install_preserves_prepared_candidate(tmp_path, monkeypatch):
    import subprocess

    (tmp_path / "target.json").write_text(
        json.dumps(
            {
                "base_image": "server@sha256:" + "a" * 64,
                "uv_image": "uv@sha256:" + "b" * 64,
            }
        )
    )
    previous = json.dumps({"requirements": {"old-plugin": "old-plugin==1.0"}})
    (tmp_path / "candidate.json").write_text(previous)

    def failed_build(context):
        assert (context / "requirements.txt").read_text().splitlines() == [
            "old-plugin==1.0",
            "acme-delta-plugin",
        ]
        raise subprocess.CalledProcessError(1, ["docker", "build"], stderr="private detail")

    monkeypatch.setattr(plugin_ops, "build_image", failed_build)
    assert operator.main(["--workspace", str(tmp_path), "plugin", "add", "acme-delta-plugin"]) == 1
    assert (tmp_path / "candidate.json").read_text() == previous


def test_control_redirect_never_forwards_authorization():
    from urllib.request import Request

    request = Request(
        "https://control.example/v1/plugins", headers={"Authorization": "Bearer secret"}
    )
    with pytest.raises(ValueError, match="redirects"):
        control_client.NoRedirect().redirect_request(
            request, None, 302, "Found", {}, "https://other.example"
        )


def test_no_arguments_preserves_flight_startup(monkeypatch):
    from dal_obscura.interfaces.cli import read

    started = []
    monkeypatch.setattr(read, "main", lambda: started.append(True))
    assert operator.main([]) == 0
    assert started == [True]


def test_workspace_equals_form_dispatches_operator_command(tmp_path, monkeypatch):
    monkeypatch.setattr(
        operator.sys,
        "argv",
        ["dal-obscura", f"--workspace={tmp_path}", "plugin", "add", "acme-delta-plugin"],
    )
    requested = []
    monkeypatch.setattr(
        plugin_ops, "add_plugin", lambda workspace, package: requested.append((workspace, package))
    )
    assert operator.main() == 0
    assert requested == [(tmp_path, "acme-delta-plugin")]
