"""Immutable release preparation and ordered Compose deployment verification."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Any

from dal_obscura.application.governance.catalogs.service import MAX_CATALOG_PUBLICATION_SIZE
from dal_obscura.interfaces.cli import (
    control_client,
    flight_smoke,
    operator_io,
    plugin_ops,
)


def compose_arguments(workspace: Path, target: dict[str, Any], override: Path) -> list[str]:
    def local_path(value: str) -> str:
        path = Path(value)
        return str((workspace / path).resolve() if not path.is_absolute() else path)

    arguments = [
        "docker",
        "compose",
        "--file",
        local_path(target["compose_file"]),
        "--file",
        str(override.resolve()),
    ]
    if target.get("env_file"):
        arguments.extend(["--env-file", local_path(target["env_file"])])
    if target.get("project_name"):
        arguments.extend(["--project-name", target["project_name"]])
    return arguments


def prepare_release(workspace: Path, target: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    catalogs_path = workspace / "catalogs.json"
    catalogs = operator_io.read_json(catalogs_path) if catalogs_path.exists() else {}
    if len(catalogs) > MAX_CATALOG_PUBLICATION_SIZE:
        raise ValueError(f"A release supports at most {MAX_CATALOG_PUBLICATION_SIZE} catalogs")
    candidate = operator_io.read_json(workspace / "candidate.json")
    if candidate["base_image"] != target["base_image"]:
        raise ValueError("Base image changed; run plugin add again before deployment")
    image = candidate["image"]
    if not plugin_ops.IMAGE_ID_PATTERN.fullmatch(image) or plugin_ops.image_id(image) != image:
        raise ValueError("Prepared plugin image is unavailable or changed")
    actual = plugin_ops.inspect_image(image, list(candidate["requirements"]))
    if actual != {"lock": candidate["lock"], "descriptors": candidate["descriptors"]}:
        raise ValueError("Prepared plugin metadata changed; run plugin add again")
    verified_path = workspace / "verified.json"
    previous = operator_io.read_json(verified_path) if verified_path.exists() else {}
    plugin_ops.show_admission(candidate, previous)
    repository = str(target["image_repository"])
    plugin_ops.validate_image_repository(repository)
    release_id = uuid.uuid4().hex
    directory = workspace / "releases" / release_id
    directory.mkdir(parents=True)
    operator_io.write_json(directory / "plugin-lock.json", candidate["lock"])
    (directory / "plugin-lock.json").chmod(0o444)
    (directory / "Dockerfile").write_text(
        f"FROM {image}\nCOPY --chmod=0444 plugin-lock.json {plugin_ops.PLUGIN_LOCK_PATH}\n"
        f"ENV DAL_OBSCURA_PLUGIN_LOCK_FILE={plugin_ops.PLUGIN_LOCK_PATH}\n"
    )
    final_image = plugin_ops.build_image(directory)
    # Check the final image layer changed no installed plugin artifacts.
    if plugin_ops.inspect_image(final_image, list(candidate["requirements"])) != actual:
        raise ValueError("Final image artifacts do not match prepared admission")
    descriptors = {
        item["plugin_id"]: item for item in candidate["descriptors"] if item["kind"] == "catalog"
    }
    for draft in catalogs.values():
        if draft["plugin_id"] not in descriptors:
            raise ValueError("Prepared catalog references a plugin outside selected admission")
        plugin_ops.validate_options(descriptors[draft["plugin_id"]], draft["options"])
    release = {
        **candidate,
        "image": final_image,
        "release_id": release_id,
        "lock_sha256": hashlib.sha256((directory / "plugin-lock.json").read_bytes()).hexdigest(),
        "tag": f"{repository}:dal-obscura-{release_id}",
        "catalogs": catalogs,
    }
    operator_io.write_json(directory / "release.json", release)
    print(f"Prepared release {release_id}: {final_image}")
    return directory, release


def publish_catalogs(target: dict[str, Any], catalogs: dict[str, Any]) -> None:
    if not catalogs:
        return
    if len(catalogs) > MAX_CATALOG_PUBLICATION_SIZE:
        raise ValueError(f"A release supports at most {MAX_CATALOG_PUBLICATION_SIZE} catalogs")
    inventory = {
        item["name"]: item for item in control_client.api_request(target, "GET", "/v1/catalogs")
    }
    pending = {}
    # Preflight gives actionable conflicts; the API rechecks all fences in one transaction.
    for name, draft in catalogs.items():
        current = inventory.get(name)
        if (
            current
            and current["plugin_id"] == draft["plugin_id"]
            and current["options"] == draft["options"]
        ):
            continue
        revision = current["revision"] if current else 0
        if draft["expected_revision"] != revision:
            raise ValueError(
                f"Catalog {name} revision changed; reconfigure with --expected-revision {revision}"
            )
        pending[name] = draft
    if not pending:
        return
    control_client.api_request(target, "POST", "/v1/catalogs/publish", {"catalogs": pending})
    for name in sorted(pending):
        print(f"Published catalog {name}")


def verify_workers(compose: list[str], image: str, lock_sha256: str) -> None:
    for service in ("control-plane", "data-plane"):
        containers = operator_io.run(
            [*compose, "ps", "--quiet", service], capture=True
        ).splitlines()
        if not containers:
            raise ValueError(f"No running {service} containers")
        for container in containers:
            state = json.loads(operator_io.run(["docker", "inspect", container], capture=True))[0]
            if state["Image"] != image or not state["State"]["Running"]:
                raise ValueError(f"{service} worker does not run the selected release")
            if (
                f"DAL_OBSCURA_PLUGIN_LOCK_FILE={plugin_ops.PLUGIN_LOCK_PATH}"
                not in state["Config"]["Env"]
            ):
                raise ValueError(f"{service} worker does not use the embedded lock")
            operator_io.run(
                [
                    "docker",
                    "exec",
                    container,
                    "python",
                    "-m",
                    plugin_ops.PLUGIN_IMAGE_MODULE,
                    "verify",
                    service,
                    lock_sha256,
                ]
            )
            print(f"Verified {service} worker {container[:12]}")


def deploy(workspace: Path, args: argparse.Namespace) -> None:
    target = plugin_ops.target_settings(workspace)
    # Missing target settings fail before any publication or runtime changes.
    for key in ("compose_file", "image_repository", "control_url", "smoke"):
        if not target.get(key):
            raise ValueError(f"Deployment target requires {key}")
    flight_smoke.validate_smoke(target["smoke"])
    directory, release = prepare_release(workspace, target)
    override = directory / "compose.json"

    def write_override(reference: str) -> None:
        operator_io.write_json(
            override,
            {
                "services": {
                    service: {
                        "image": reference,
                        "environment": {
                            "DAL_OBSCURA_PLUGIN_LOCK_FILE": plugin_ops.PLUGIN_LOCK_PATH
                        },
                    }
                    for service in ("control-plane", "data-plane")
                }
            },
        )

    write_override(release["image"])
    compose = compose_arguments(workspace, target, override)
    operator_io.run([*compose, "config", "--quiet"])
    if args.prepare:
        print(f"Prepared only: {directory.resolve()}")
        return
    # Read-only authentication/preflight before an operator approves the concrete release.
    control_client.api_request(target, "GET", "/v1/plugins")
    os.environ[target["smoke"].get("token_env", "DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN")]
    if (
        not args.yes
        and input("Publish this admission and deploy both planes? [y/N]: ").strip().lower() != "y"
    ):
        print("Release prepared; deployment cancelled.")
        return
    operator_io.run(["docker", "tag", release["image"], release["tag"]])
    operator_io.run(["docker", "push", release["tag"]])
    identities = json.loads(
        operator_io.run(
            ["docker", "image", "inspect", "--format", "{{json .RepoDigests}}", release["tag"]],
            capture=True,
        )
    )
    prefix = target["image_repository"] + "@sha256:"
    digests = [value for value in identities if value.startswith(prefix)]
    if len(digests) != 1 or not plugin_ops.IMAGE_DIGEST_PATTERN.fullmatch(digests[0]):
        raise ValueError("Published image has no unique immutable repository digest")
    release["published_image"] = digests[0]
    operator_io.write_json(directory / "release.json", release)
    write_override(digests[0])
    # Existing schema only: routine deployment never runs migrations or changes demo services.
    operator_io.run(
        [
            *compose,
            "up",
            "--detach",
            "--no-deps",
            "--wait",
            "--wait-timeout",
            "120",
            "control-plane",
            "data-plane",
        ]
    )
    verify_workers(compose, release["image"], release["lock_sha256"])
    publish_catalogs(target, release["catalogs"])
    flight_smoke.smoke_read(target["smoke"])
    if release["catalogs"]:
        inventory = {
            item["name"]: item for item in control_client.api_request(target, "GET", "/v1/catalogs")
        }
        path = workspace / "catalogs.json"
        drafts = operator_io.read_json(path)
        for name, published in release["catalogs"].items():
            current = inventory[name]
            if (
                drafts.get(name) == published
                and current["options"] == published["options"]
                and current["plugin_id"] == published["plugin_id"]
            ):
                drafts[name]["expected_revision"] = current["revision"]
        operator_io.write_json(path, drafts)
    operator_io.write_json(workspace / "verified.json", release)
    print(f"Release {release['release_id']} published and verified: {digests[0]}")
