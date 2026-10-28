"""Plugin image preparation, descriptor admission and catalog drafts."""

from __future__ import annotations

import argparse
import json
import re
import tempfile
import uuid
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from dal_obscura_plugin_api import PluginDescriptor
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from dal_obscura.application.governance.catalogs.validation import (
    validate_catalog_options,
    validate_descriptor_options,
)
from dal_obscura.interfaces.cli import (
    operator_io,
)
from dal_obscura.kernel.plugins import PluginLock

IMAGE_DIGEST_PATTERN = re.compile(r"[a-z0-9][a-z0-9./:_-]*@sha256:[0-9a-f]{64}\Z")
IMAGE_ID_PATTERN = re.compile(r"sha256:[0-9a-f]{64}\Z")
PLUGIN_IMAGE_MODULE = "dal_obscura.interfaces.cli.plugin_image"
PLUGIN_LOCK_PATH = "/app/plugin-lock.json"


def target_settings(workspace: Path) -> dict[str, Any]:
    target = operator_io.read_json(workspace / "target.json")
    for name in ("base_image", "uv_image"):
        validate_image_reference(str(target.get(name, "")), name)
    return target


def validate_image_reference(value: str, name: str) -> None:
    if not IMAGE_DIGEST_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be an immutable image@sha256:digest reference")


def validate_image_repository(repository: str) -> None:
    if not re.fullmatch(r"[a-z0-9][a-z0-9./:_-]*", repository) or not re.fullmatch(
        r"[a-z0-9][a-z0-9._-]*", repository.rsplit("/", 1)[-1]
    ):
        raise ValueError("image_repository must be a container repository, without tag/digest")


def image_id(reference: str) -> str:
    identity = operator_io.run(
        ["docker", "image", "inspect", "--format", "{{.Id}}", reference], capture=True
    )
    if not IMAGE_ID_PATTERN.fullmatch(identity):
        raise ValueError("Container engine returned an invalid image identity")
    return identity


def inspect_image(image: str, distributions: list[str]) -> dict[str, Any]:
    payload = operator_io.run(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--entrypoint",
            "python",
            image,
            "-m",
            PLUGIN_IMAGE_MODULE,
            "inspect",
            *distributions,
        ],
        capture=True,
    )
    result = json.loads(payload)
    if not isinstance(result, dict) or not result.get("descriptors"):
        raise ValueError("Image inspection returned no plugin descriptors")
    return result


def build_image(context: Path) -> str:
    tag = f"dal-obscura-candidate:{uuid.uuid4().hex}"
    operator_io.run(["docker", "build", "--tag", tag, str(context)])
    return image_id(tag)


def add_plugin(workspace: Path, specification: str) -> None:
    target = target_settings(workspace)
    requirement = Requirement(specification)
    if requirement.url or requirement.marker or requirement.extras:
        raise ValueError(
            "Use a package name with an optional version constraint, without URLs/extras"
        )
    candidate_path = workspace / "candidate.json"
    previous = operator_io.read_json(candidate_path) if candidate_path.exists() else {}
    requirements = dict(previous.get("requirements", {}))
    package = canonicalize_name(requirement.name)
    requirements[package] = str(requirement)
    # Build from the pinned base each time; previously selected packages remain version-pinned.
    with tempfile.TemporaryDirectory() as directory:
        context = Path(directory)
        (context / "requirements.txt").write_text("\n".join(requirements.values()) + "\n")
        (context / "constraints.py").write_text(
            "from importlib.metadata import distributions\n"
            "from pathlib import Path\n"
            "Path('/tmp/base.txt').write_text(''.join("
            "f'{d.name}=={d.version}\\n' for d in distributions()))\n"
        )
        (context / "Dockerfile").write_text(
            f"FROM {target['uv_image']} AS tools\n"
            f"FROM {target['base_image']} AS install\nUSER 0\n"
            "COPY --from=tools /uv /tmp/uv\n"
            "COPY constraints.py requirements.txt /tmp/\n"
            "RUN /app/.venv/bin/python /tmp/constraints.py && "
            "/tmp/uv pip install --python /app/.venv/bin/python --only-binary :all: "
            "--constraint /tmp/base.txt --requirement /tmp/requirements.txt\n"
            f"FROM {target['base_image']}\n"
            "COPY --from=install /app/.venv /app/.venv\n"
        )
        image = build_image(context)
    inspected = inspect_image(image, list(requirements))
    for descriptor in inspected["descriptors"]:
        name = canonicalize_name(descriptor["distribution"])
        requirements[name] = f"{name}=={descriptor['version']}"
    candidate = {
        **inspected,
        "image": image,
        "base_image": target["base_image"],
        "requirements": requirements,
    }
    show_admission(candidate, previous)
    operator_io.write_json(candidate_path, candidate)


def show_admission(candidate: dict[str, Any], previous: dict[str, Any]) -> None:
    old = {
        (item["kind"], item["plugin_id"]): PluginLock.from_json(item["lock"])
        for item in previous.get("lock", {}).get("plugins", [])
    }
    current = [
        ((item["kind"], item["plugin_id"]), PluginLock.from_json(item["lock"]))
        for item in candidate["lock"]["plugins"]
    ]
    for key, lock in current:
        state = "unchanged" if old.get(key) == lock else "changed" if key in old else "added"
        print(
            f"{state}: {key[0]}:{key[1]} — "
            f"{lock.distribution}=={lock.version} (API {lock.api_version})"
        )
        print(f"  descriptor {lock.descriptor_digest}\n  artifact   {lock.artifact_digest}")
    removed = old.keys() - {key for key, _ in current}
    for kind, plugin_id in sorted(removed):
        print(f"removed: {kind}:{plugin_id}")
    for descriptor in candidate["descriptors"]:
        fields = descriptor.get("config_schema", {}).get("fields", [])
        required = [
            f"{field['name']} ({field.get('type', 'string')})"
            for field in fields
            if field.get("required")
        ]
        print(
            f"{descriptor['plugin_id']} required settings: {', '.join(required) or 'none declared'}"
        )


def descriptor_model(payload: dict[str, Any]) -> PluginDescriptor:
    return PluginDescriptor(
        kind=payload["kind"],
        plugin_id=payload["plugin_id"],
        api_version=payload["api_version"],
        config_version=payload["config_version"],
        distribution=payload["distribution"],
        version=payload["version"],
        config_schema=payload.get("config_schema", {}),
        capabilities=frozenset(payload.get("capabilities", [])),
        output_formats=frozenset(payload.get("output_formats", [])),
        handle_versions=frozenset(payload.get("handle_versions", [1])),
    )


def collect_options(payload: dict[str, Any], name: str) -> dict[str, Any]:
    schema = payload.get("config_schema", {})
    fields = schema.get("fields", [])
    if not fields:
        raise ValueError(
            "Plugin declares no form fields; supply --options with non-secret JSON options"
        )
    options = dict(schema.get("defaults", {}))
    for field in fields:
        key = field["name"]
        field_type = field.get("type", "string")
        if field.get("description"):
            print(field["description"])
        if field_type == "secret_reference":
            secret = input(f"{key} secret name (never the secret value): ").strip()
            if secret:
                scope = input(f"{key} scope [catalog:{name}]: ").strip() or f"catalog:{name}"
                options[key] = {"secret": secret, "scope": scope}
            continue
        choices = f" choices={field['options']}" if field.get("options") else ""
        value = input(f"{key} ({field_type}{choices}) [{options.get(key, '')}]: ")
        if value:
            options[key] = (
                json.loads(value) if field_type in {"boolean", "integer", "number"} else value
            )
    return options


def configure_plugin(workspace: Path, args: argparse.Namespace) -> None:
    candidate = operator_io.read_json(workspace / "candidate.json")
    matches = [
        d
        for d in candidate["descriptors"]
        if d["plugin_id"] == args.plugin_id and d["kind"] == "catalog"
    ]
    if len(matches) != 1:
        raise ValueError(
            "Select one prepared catalog plugin ID; formats have no catalog configuration"
        )
    name = args.name or input("Catalog instance name: ").strip()
    if not re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}", name):
        raise ValueError(
            "Catalog name must contain 1-128 letters, digits, dots, underscores or hyphens"
        )
    if args.expected_revision is not None and args.expected_revision < 0:
        raise ValueError("expected_revision must be non-negative")
    options = (
        operator_io.read_json(args.options) if args.options else collect_options(matches[0], name)
    )
    validate_options(matches[0], options)
    path = workspace / "catalogs.json"
    catalogs = operator_io.read_json(path) if path.exists() else {}
    revision = args.expected_revision
    if revision is None:
        revision = catalogs.get(name, {}).get("expected_revision", 0)
    catalogs[name] = {
        "plugin_id": args.plugin_id,
        "options": options,
        "expected_revision": revision,
    }
    operator_io.write_json(path, catalogs)
    print(f"Prepared catalog {name}; secret references only. Publication happens during deploy.")


def validate_options(payload: dict[str, Any], options: dict[str, Any]) -> None:
    validate_descriptor_options(descriptor_model(payload), options)
    # Reuse secret-literal/shape validation; live API still enforces deployment egress.
    hosts = tuple(
        filter(
            None,
            (
                urlsplit(value).hostname
                for value in options.values()
                if isinstance(value, str) and "://" in value
            ),
        )
    )
    validate_catalog_options(options, egress_allowlist=hosts)
