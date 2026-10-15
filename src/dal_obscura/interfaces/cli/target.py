"""Guided operator target initialization and read-only prerequisite diagnostics."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from dal_obscura.interfaces.cli import (
    control_client,
    deployment,
    flight_smoke,
    operator_io,
    plugin_ops,
)

TARGET_FIELDS = (
    ("base_image", "Worker base image with verified sha256 digest"),
    ("uv_image", "uv build image with verified sha256 digest"),
    ("compose_file", "Existing initialized Compose file"),
    ("image_repository", "Release image repository, without tag"),
    ("control_url", "Control API HTTPS URL"),
    ("flight_uri", "Flight TLS URI"),
    ("catalog", "Synthetic smoke catalog"),
    ("table", "Synthetic smoke table"),
    ("columns", 'Projected column paths as JSON, e.g. ["id"]'),
    ("expected_rows", 'Expected non-secret synthetic rows as JSON, e.g. [{"id":1}]'),
)


def initialize_target(workspace: Path, args: argparse.Namespace) -> None:
    destination = workspace / "target.json"
    if destination.exists():
        raise ValueError("Target already exists; use another workspace or review the existing file")
    values = {}
    for name, label in TARGET_FIELDS:
        value = getattr(args, name)
        if value is None:
            if args.non_interactive:
                raise ValueError(
                    f"Missing --{name.replace('_', '-')}; omit --non-interactive for guided setup"
                )
            value = input(label + ": ").strip()
        values[name] = value
    target = {
        **{
            name: values[name]
            for name in ("base_image", "uv_image", "image_repository", "control_url")
        },
        "compose_file": str(Path(values["compose_file"]).resolve()),
        "admin_token_env": args.admin_token_env,
        "smoke": {
            "uri": values["flight_uri"],
            "catalog": values["catalog"],
            "target": values["table"],
            "columns": json.loads(values["columns"]),
            "expected_rows": json.loads(values["expected_rows"]),
            "token_env": args.flight_token_env,
        },
    }
    if args.env_file:
        target["env_file"] = str(Path(args.env_file).resolve())
    if args.project_name:
        target["project_name"] = args.project_name
    for name in ("ca_file", "cert_file", "key_file"):
        if getattr(args, name):
            target["smoke"][name] = str(Path(getattr(args, name)).resolve())
    validate_target(workspace, target)
    operator_io.write_json(destination, target, replace=False)
    print(f"Initialized target: {destination.resolve()}")
    print("Next: provision the named credential references, then run dal-obscura doctor")


def validate_target(workspace: Path, target: dict[str, Any]) -> None:
    for name in ("base_image", "uv_image"):
        plugin_ops.validate_image_reference(str(target[name]), name)
    plugin_ops.validate_image_repository(str(target["image_repository"]))
    control_client.validate_control_url(str(target["control_url"]))
    flight_smoke.validate_smoke(target["smoke"])
    for value in (
        target.get("admin_token_env", "DAL_OBSCURA_DEPLOY_ADMIN_TOKEN"),
        target["smoke"].get("token_env", "DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN"),
    ):
        if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError("Credential references must be environment variable names")
    for name in ("compose_file", "env_file"):
        value = target.get(name)
        if name == "env_file" and not value:
            continue
        if not isinstance(value, str) or not value:
            raise ValueError(f"{name} must reference an existing file")
        path = Path(value)
        if not path.is_absolute():
            path = workspace / path
        if not path.is_file():
            raise ValueError(f"{name} must reference an existing file")


def diagnose_target(workspace: Path, args: argparse.Namespace) -> int:
    checks: list[dict[str, object]] = []
    try:
        target = plugin_ops.target_settings(workspace)
        validate_target(workspace, target)
        checks.append(
            {"name": "target configuration", "ok": True, "message": "Valid files and references"}
        )
    except OSError, ValueError, KeyError, TypeError:
        target = None
        checks.append(
            {
                "name": "target configuration",
                "ok": False,
                "message": "Run target init or review target.json and referenced files",
            }
        )
    if target is not None:
        for name, reference in (
            ("control credential", target.get("admin_token_env", "DAL_OBSCURA_DEPLOY_ADMIN_TOKEN")),
            (
                "Flight credential",
                target["smoke"].get("token_env", "DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN"),
            ),
        ):
            available = bool(os.environ.get(reference))
            checks.append(
                {
                    "name": name,
                    "ok": available,
                    "message": "Available"
                    if available
                    else f"Provision {reference} through credential tooling",
                }
            )
        if not args.offline:
            _check_online(workspace, target, checks)
    ready = all(check["ok"] for check in checks)
    report = {
        "version": 1,
        "mode": "offline" if args.offline else "online",
        "ready": ready,
        "checks": checks,
    }
    if args.json:
        print(json.dumps(report, allow_nan=False))
    else:
        for check in checks:
            print(f"{'PASS' if check['ok'] else 'FAIL'} {check['name']}: {check['message']}")
    return 0 if ready else 1


def _check_online(workspace: Path, target: dict[str, Any], checks: list[dict[str, object]]) -> None:
    try:
        operator_io.run(["docker", "info", "--format", "{{.ServerVersion}}"], capture=True)
        with tempfile.TemporaryDirectory(prefix="dal-target-check-") as temporary:
            overlay = Path(temporary) / "compose.json"
            operator_io.write_json(overlay, {"services": {}})
            operator_io.run(
                [*deployment.compose_arguments(workspace, target, overlay), "config", "--quiet"],
                capture=True,
            )
        checks.append(
            {
                "name": "container prerequisites",
                "ok": True,
                "message": "Engine and Compose configuration available",
            }
        )
    except OSError, ValueError, subprocess.SubprocessError:
        checks.append(
            {
                "name": "container prerequisites",
                "ok": False,
                "message": "Check container engine and Compose configuration; no services changed",
            }
        )
    if os.environ.get(target.get("admin_token_env", "DAL_OBSCURA_DEPLOY_ADMIN_TOKEN")):
        try:
            control_client.api_request(target, "GET", "/v1/plugins")
            checks.append(
                {
                    "name": "control authentication",
                    "ok": True,
                    "message": "Authenticated plugin inventory available",
                }
            )
        except OSError, ValueError, KeyError:
            checks.append(
                {
                    "name": "control authentication",
                    "ok": False,
                    "message": "Check control URL, TLS, and administrative credential",
                }
            )
