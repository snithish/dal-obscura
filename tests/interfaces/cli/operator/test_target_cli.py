"""Target setup writes references; diagnostics never change running services."""

import json

import pytest

from dal_obscura.interfaces.cli import (
    control_client,
    operator,
    operator_io,
)


@pytest.fixture
def target_arguments(tmp_path):
    compose = tmp_path / "compose.yaml"
    compose.write_text("services: {}\n")
    return [
        "--workspace",
        str(tmp_path / "workspace"),
        "target",
        "init",
        "--non-interactive",
        "--base-image",
        "server@sha256:" + "a" * 64,
        "--uv-image",
        "uv@sha256:" + "b" * 64,
        "--compose-file",
        str(compose),
        "--image-repository",
        "registry.example/obscura",
        "--control-url",
        "https://governance.example",
        "--flight-uri",
        "grpc+tls://flight.example:443",
        "--catalog",
        "synthetic",
        "--table",
        "default.deployment_check",
        "--columns",
        '["id"]',
        "--expected-rows",
        '[{"id":1}]',
    ]


def test_target_init_prepares_complete_reference_only_configuration(
    target_arguments, tmp_path, monkeypatch
):
    monkeypatch.setattr(
        operator_io, "run", lambda *a, **kw: pytest.fail("No external calls during initialization")
    )
    assert operator.main(target_arguments) == 0
    target = json.loads((tmp_path / "workspace/target.json").read_text())
    assert target["compose_file"] == str(tmp_path / "compose.yaml")
    assert target["admin_token_env"] == "DAL_OBSCURA_DEPLOY_ADMIN_TOKEN"
    assert target["smoke"] == {
        "uri": "grpc+tls://flight.example:443",
        "catalog": "synthetic",
        "target": "default.deployment_check",
        "columns": ["id"],
        "expected_rows": [{"id": 1}],
        "token_env": "DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN",
    }
    assert (tmp_path / "workspace/target.json").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    ("flag", "value"),
    [
        ("--base-image", "server:latest"),
        ("--control-url", "https://user:sentinel-secret@governance.example"),
        ("--control-url", "http://governance.example"),
        ("--flight-uri", "grpc+tls://user:sentinel-secret@flight.example:443"),
        ("--flight-uri", "grpc+tcp://flight.example:8815"),
        ("--expected-rows", "[]"),
        ("--columns", ""),
    ],
)
def test_target_init_rejects_invalid_values_without_partial_state(
    target_arguments, tmp_path, capsys, flag, value
):
    target_arguments[target_arguments.index(flag) + 1] = value
    assert operator.main(target_arguments) == 1
    assert not (tmp_path / "workspace/target.json").exists()
    assert "sentinel-secret" not in capsys.readouterr().err


def test_target_init_preserves_existing_configuration(target_arguments, tmp_path):
    target = tmp_path / "workspace/target.json"
    target.parent.mkdir()
    target.write_text('{"existing":true}\n')
    assert operator.main(target_arguments) == 1
    assert target.read_text() == '{"existing":true}\n'


def test_target_init_collects_missing_fields_interactively(tmp_path, monkeypatch):
    compose = tmp_path / "compose.yaml"
    compose.write_text("services: {}\n")
    answers = iter(
        [
            "server@sha256:" + "a" * 64,
            "uv@sha256:" + "b" * 64,
            str(compose),
            "registry.example/obscura",
            "https://governance.example",
            "grpc+tls://flight.example:443",
            "synthetic",
            "default.deployment_check",
            '["id"]',
            '[{"id":1}]',
        ]
    )
    monkeypatch.setattr("builtins.input", lambda _: next(answers))
    assert operator.main(["--workspace", str(tmp_path / "workspace"), "target", "init"]) == 0
    assert json.loads((tmp_path / "workspace/target.json").read_text())["smoke"]["columns"] == [
        "id"
    ]


def test_offline_doctor_reports_missing_credentials_without_exposing_values(
    target_arguments, tmp_path, monkeypatch, capsys
):
    assert operator.main(target_arguments) == 0
    capsys.readouterr()
    monkeypatch.setattr(
        operator_io, "run", lambda *a, **kw: pytest.fail("Offline diagnostics must not call Docker")
    )
    monkeypatch.delenv("DAL_OBSCURA_DEPLOY_ADMIN_TOKEN", raising=False)
    monkeypatch.setenv("DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN", "sentinel-secret")
    assert (
        operator.main(["--workspace", str(tmp_path / "workspace"), "doctor", "--offline", "--json"])
        == 1
    )
    result = json.loads(capsys.readouterr().out)
    assert result["ready"] is False
    assert {item["name"]: item["ok"] for item in result["checks"]}["control credential"] is False
    assert "sentinel-secret" not in json.dumps(result)
    assert result["mode"] == "offline"


def test_noninteractive_missing_setting_never_prompts_or_saves(
    target_arguments, tmp_path, monkeypatch
):
    index = target_arguments.index("--table")
    del target_arguments[index : index + 2]
    monkeypatch.setattr("builtins.input", lambda _: pytest.fail("Must not prompt"))
    assert operator.main(target_arguments) == 1
    assert not (tmp_path / "workspace/target.json").exists()


def test_online_doctor_uses_only_read_only_calls(target_arguments, tmp_path, monkeypatch, capsys):
    assert operator.main(target_arguments) == 0
    capsys.readouterr()
    monkeypatch.setenv("DAL_OBSCURA_DEPLOY_ADMIN_TOKEN", "sentinel-secret")
    monkeypatch.setenv("DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN", "sentinel-secret")
    calls = []

    def run(arguments, *, capture=False):
        assert capture
        calls.append(arguments)
        return ""

    def api(target, method, path, body=None):
        assert (method, path, body) == ("GET", "/v1/plugins", None)
        return {"plugins": [], "pairs": [], "states": []}

    monkeypatch.setattr(operator_io, "run", run)
    monkeypatch.setattr(control_client, "api_request", api)
    assert operator.main(["--workspace", str(tmp_path / "workspace"), "doctor", "--json"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["ready"] is True
    assert result["mode"] == "online"
    assert calls[0] == ["docker", "info", "--format", "{{.ServerVersion}}"]
    assert calls[1][-2:] == ["config", "--quiet"]
    assert "sentinel-secret" not in json.dumps(result)


def test_doctor_sanitizes_container_failure_details(
    target_arguments, tmp_path, monkeypatch, capsys
):
    import subprocess

    assert operator.main(target_arguments) == 0
    capsys.readouterr()
    monkeypatch.setenv("DAL_OBSCURA_DEPLOY_ADMIN_TOKEN", "sentinel-secret")
    monkeypatch.setenv("DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN", "sentinel-secret")

    def failed(*args, **kwargs):
        raise subprocess.CalledProcessError(1, ["docker"], stderr="sentinel-secret")

    monkeypatch.setattr(operator_io, "run", failed)
    monkeypatch.setattr(control_client, "api_request", lambda *args: {})
    assert operator.main(["--workspace", str(tmp_path / "workspace"), "doctor", "--json"]) == 1
    result = json.loads(capsys.readouterr().out)
    assert "sentinel-secret" not in json.dumps(result)
    assert {item["name"]: item["ok"] for item in result["checks"]}[
        "container prerequisites"
    ] is False
