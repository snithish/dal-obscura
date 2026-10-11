"""Operator workflows exercise the CLI boundary without changing live services."""

from __future__ import annotations

import json

import pytest

from dal_obscura.interfaces.cli import operator


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
        "api_version": "2",
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
        "api_version": "2",
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
    monkeypatch.setattr(operator, "run", lambda *a, **kw: pytest.fail("Docker must not run"))
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

    monkeypatch.setattr(operator, "build_image", failed_build)
    assert operator.main(["--workspace", str(tmp_path), "plugin", "add", "acme-delta-plugin"]) == 1
    assert (tmp_path / "candidate.json").read_text() == previous


def test_catalog_revision_conflict_blocks_all_writes(monkeypatch):
    calls = []

    def api(target, method, path, body=None):
        calls.append((method, path))
        return [{"name": "existing", "revision": 4, "plugin_id": "acme.delta", "options": {}}]

    monkeypatch.setattr(operator, "api_request", api)
    with pytest.raises(ValueError, match="revision changed"):
        operator.publish_catalogs(
            {},
            {
                "new": {"plugin_id": "acme.delta", "options": {}, "expected_revision": 0},
                "existing": {
                    "plugin_id": "acme.delta",
                    "options": {"x": 1},
                    "expected_revision": 3,
                },
            },
        )
    assert calls == [("GET", "/v1/catalogs")]


def test_catalog_publication_is_idempotent_with_same_options(monkeypatch):
    calls = []
    options = {"root": "s3://bucket/path"}

    def api(target, method, path, body=None):
        calls.append(method)
        return [{"name": "warehouse", "revision": 9, "plugin_id": "acme.delta", "options": options}]

    monkeypatch.setattr(operator, "api_request", api)
    operator.publish_catalogs(
        {},
        {
            "warehouse": {
                "plugin_id": "acme.delta",
                "options": options,
                "expected_revision": 0,
            }
        },
    )
    assert calls == ["GET"]


@pytest.mark.parametrize("bad_worker", ["different_image", "wrong_lock", "stopped"])
def test_every_scaled_worker_must_run_selected_image_and_lock(monkeypatch, bad_worker):
    image = "sha256:" + "a" * 64
    verified = []

    def docker(args, capture=False):
        if "ps" in args:
            return "worker-a\nworker-b"
        if args[1] == "inspect":
            bad = args[-1] == "worker-b"
            return json.dumps(
                [
                    {
                        "Image": "sha256:" + "b" * 64
                        if bad and bad_worker == "different_image"
                        else image,
                        "State": {"Running": not (bad and bad_worker == "stopped")},
                        "Config": {
                            "Env": []
                            if bad and bad_worker == "wrong_lock"
                            else ["DAL_OBSCURA_PLUGIN_LOCK_FILE=/app/plugin-lock.json"]
                        },
                    }
                ]
            )
        if args[1] == "exec":
            verified.append(args[2])
        return ""

    monkeypatch.setattr(operator, "run", docker)
    with pytest.raises(ValueError, match="worker"):
        operator.verify_workers(["docker", "compose"], image, "c" * 64)
    assert verified == ["worker-a"]


def test_control_redirect_never_forwards_authorization():
    from urllib.request import Request

    request = Request(
        "https://control.example/v1/plugins", headers={"Authorization": "Bearer secret"}
    )
    with pytest.raises(ValueError, match="redirects"):
        operator.NoRedirect().redirect_request(
            request, None, 302, "Found", {}, "https://other.example"
        )


def test_prepare_release_embeds_lock_and_never_publishes(tmp_path, monkeypatch):
    from tests.support.operator_release import prepared_operator_workspace

    candidate = prepared_operator_workspace(tmp_path)
    inspected = {"lock": candidate["lock"], "descriptors": candidate["descriptors"]}
    monkeypatch.setattr(operator, "image_id", lambda _: candidate["image"])
    monkeypatch.setattr(operator, "inspect_image", lambda *args: inspected)
    monkeypatch.setattr(operator, "build_image", lambda context: "sha256:" + "f" * 64)
    calls = []
    monkeypatch.setattr(operator, "run", lambda args, **kwargs: calls.append(args) or "")
    assert operator.main(["--workspace", str(tmp_path), "deploy", "--prepare"]) == 0
    release = next((tmp_path / "releases").iterdir())
    manifest = json.loads((release / "release.json").read_text())
    assert manifest["image"] == "sha256:" + "f" * 64
    assert manifest["lock"] == candidate["lock"]
    assert (release / "plugin-lock.json").stat().st_mode & 0o222 == 0
    assert (
        "COPY --chmod=0444 plugin-lock.json /app/plugin-lock.json"
        in (release / "Dockerfile").read_text()
    )
    assert not (tmp_path / "verified.json").exists()
    assert all(args[1] == "compose" and args[-2:] == ["config", "--quiet"] for args in calls)


def test_changed_candidate_artifact_blocks_release_creation(tmp_path, monkeypatch):
    from tests.support.operator_release import prepared_operator_workspace

    candidate = prepared_operator_workspace(tmp_path)
    monkeypatch.setattr(operator, "image_id", lambda _: candidate["image"])
    monkeypatch.setattr(operator, "inspect_image", lambda *args: {"descriptors": [], "lock": {}})
    monkeypatch.setattr(
        operator, "build_image", lambda _: pytest.fail("Must not build changed candidate")
    )
    assert operator.main(["--workspace", str(tmp_path), "deploy", "--prepare"]) == 1
    assert not (tmp_path / "releases").exists()


@pytest.mark.parametrize("outcome", ["success", "worker_failure", "smoke_failure"])
def test_deploy_marks_verified_only_after_workers_and_smoke(tmp_path, monkeypatch, outcome):
    from tests.support.operator_release import prepared_operator_workspace

    candidate = prepared_operator_workspace(tmp_path)
    previous = '{"release_id": "previous"}\n'
    (tmp_path / "verified.json").write_text(previous)
    monkeypatch.setattr(operator, "image_id", lambda _: candidate["image"])
    monkeypatch.setattr(
        operator,
        "inspect_image",
        lambda *args: {
            "lock": candidate["lock"],
            "descriptors": candidate["descriptors"],
        },
    )
    image = "sha256:" + "f" * 64
    monkeypatch.setattr(operator, "build_image", lambda _: image)
    monkeypatch.setenv("DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN", "not-persisted")
    monkeypatch.setattr(operator, "api_request", lambda *args: {})
    published = "registry.example/dal-obscura@sha256:" + "9" * 64
    events = []

    def docker(args, capture=False):
        if args[1] == "push":
            events.append("publish")
        if args[1:3] == ["image", "inspect"]:
            return json.dumps([published])
        if "up" in args:
            events.append("start")
        return ""

    def workers(*args):
        events.append("workers")
        if outcome == "worker_failure":
            raise ValueError("Worker failed")

    def smoke(*args):
        events.append("smoke")
        if outcome == "smoke_failure":
            raise ValueError("Smoke failed")

    monkeypatch.setattr(operator, "run", docker)
    monkeypatch.setattr(operator, "verify_workers", workers)
    monkeypatch.setattr(operator, "smoke_read", smoke)
    result = operator.main(["--workspace", str(tmp_path), "deploy", "--yes"])
    if outcome == "success":
        assert result == 0
        verified = json.loads((tmp_path / "verified.json").read_text())
        assert verified["image"] == image
        assert verified["published_image"] == published
        assert events == ["publish", "start", "workers", "smoke"]
        assert "not-persisted" not in (tmp_path / "verified.json").read_text()
    else:
        assert result == 1
        assert (tmp_path / "verified.json").read_text() == previous
        assert events[-1] == ("workers" if outcome == "worker_failure" else "smoke")


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
        operator, "add_plugin", lambda workspace, package: requested.append((workspace, package))
    )
    assert operator.main() == 0
    assert requested == [(tmp_path, "acme-delta-plugin")]
