from __future__ import annotations

import json
from pathlib import Path

import pytest

from dal_obscura.interfaces.cli import (
    control_client,
    deployment,
    flight_smoke,
    operator,
    operator_io,
    plugin_ops,
)


def test_catalog_revision_conflict_blocks_all_writes(monkeypatch):
    calls = []

    def api(target, method, path, body=None):
        calls.append((method, path))
        return [{"name": "existing", "revision": 4, "plugin_id": "acme.delta", "options": {}}]

    monkeypatch.setattr(control_client, "api_request", api)
    with pytest.raises(ValueError, match="revision changed"):
        deployment.publish_catalogs(
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

    monkeypatch.setattr(control_client, "api_request", api)
    deployment.publish_catalogs(
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


def test_catalog_publication_sends_one_atomic_change_set(monkeypatch):
    calls = []
    drafts = {
        name: {
            "plugin_id": "acme.delta",
            "options": {"root": f"s3://bucket/{name}"},
            "expected_revision": 0,
        }
        for name in ("warehouse-b", "warehouse-a")
    }

    def api(target, method, path, body=None):
        calls.append((method, path, body))
        return []

    monkeypatch.setattr(control_client, "api_request", api)
    deployment.publish_catalogs({}, drafts)
    assert calls == [
        ("GET", "/v1/catalogs", None),
        ("POST", "/v1/catalogs/publish", {"catalogs": drafts}),
    ]


def test_oversized_catalog_release_fails_before_container_work(tmp_path, monkeypatch):
    (tmp_path / "catalogs.json").write_text(json.dumps({str(index): {} for index in range(101)}))
    monkeypatch.setattr(
        operator_io, "run", lambda *args, **kwargs: pytest.fail("Container work must not run")
    )
    with pytest.raises(ValueError, match="at most 100 catalogs"):
        deployment.prepare_release(tmp_path, {})


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

    monkeypatch.setattr(operator_io, "run", docker)
    with pytest.raises(ValueError, match="worker"):
        deployment.verify_workers(["docker", "compose"], image, "c" * 64)
    assert verified == ["worker-a"]


def test_prepare_release_embeds_lock_and_never_publishes(tmp_path, monkeypatch):
    candidate = prepared_operator_workspace(tmp_path)
    inspected = {"lock": candidate["lock"], "descriptors": candidate["descriptors"]}
    monkeypatch.setattr(plugin_ops, "image_id", lambda _: candidate["image"])
    monkeypatch.setattr(plugin_ops, "inspect_image", lambda *args: inspected)
    monkeypatch.setattr(plugin_ops, "build_image", lambda context: "sha256:" + "f" * 64)
    calls = []
    monkeypatch.setattr(operator_io, "run", lambda args, **kwargs: calls.append(args) or "")
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
    candidate = prepared_operator_workspace(tmp_path)
    monkeypatch.setattr(plugin_ops, "image_id", lambda _: candidate["image"])
    monkeypatch.setattr(plugin_ops, "inspect_image", lambda *args: {"descriptors": [], "lock": {}})
    monkeypatch.setattr(
        plugin_ops, "build_image", lambda _: pytest.fail("Must not build changed candidate")
    )
    assert operator.main(["--workspace", str(tmp_path), "deploy", "--prepare"]) == 1
    assert not (tmp_path / "releases").exists()


@pytest.mark.parametrize("outcome", ["success", "worker_failure", "smoke_failure"])
def test_deploy_marks_verified_only_after_workers_and_smoke(tmp_path, monkeypatch, outcome):
    candidate = prepared_operator_workspace(tmp_path)
    previous = '{"release_id": "previous"}\n'
    (tmp_path / "verified.json").write_text(previous)
    monkeypatch.setattr(plugin_ops, "image_id", lambda _: candidate["image"])
    monkeypatch.setattr(
        plugin_ops,
        "inspect_image",
        lambda *args: {
            "lock": candidate["lock"],
            "descriptors": candidate["descriptors"],
        },
    )
    image = "sha256:" + "f" * 64
    monkeypatch.setattr(plugin_ops, "build_image", lambda _: image)
    monkeypatch.setenv("DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN", "not-persisted")
    monkeypatch.setattr(control_client, "api_request", lambda *args: {})
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

    monkeypatch.setattr(operator_io, "run", docker)
    monkeypatch.setattr(deployment, "verify_workers", workers)
    monkeypatch.setattr(flight_smoke, "smoke_read", smoke)
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


def prepared_operator_workspace(workspace: Path) -> dict:
    image = "sha256:" + "c" * 64
    descriptor = {
        "kind": "catalog",
        "plugin_id": "acme.delta",
        "api_version": "3",
        "config_version": 1,
        "distribution": "acme-delta-plugin",
        "version": "1.0",
        "config_schema": {},
    }
    candidate = {
        "image": image,
        "base_image": "server@sha256:" + "a" * 64,
        "requirements": {"acme-delta-plugin": "acme-delta-plugin==1.0"},
        "descriptors": [descriptor],
        "lock": {
            "version": 1,
            "plugins": [
                {
                    "kind": "catalog",
                    "plugin_id": "acme.delta",
                    "lock": ["acme-delta-plugin", "1.0", "2", "d" * 64, "e" * 64],
                }
            ],
        },
    }
    target = {
        "base_image": candidate["base_image"],
        "uv_image": "uv@sha256:" + "b" * 64,
        "compose_file": "compose.yaml",
        "control_url": "https://control.example",
        "image_repository": "registry.example/dal-obscura",
        "project_name": "operator-test",
        "smoke": {
            "uri": "grpc+tls://flight.example:443",
            "catalog": "synthetic",
            "target": "default.smoke",
            "columns": ["id"],
            "expected_rows": [{"id": 1}],
        },
    }
    (workspace / "candidate.json").write_text(json.dumps(candidate))
    (workspace / "target.json").write_text(json.dumps(target))
    return candidate
