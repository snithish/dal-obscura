"""Operator plugin preparation and deployment through the existing Compose topology."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from dal_obscura_plugin_api import PluginDescriptor
from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from dal_obscura.control.catalog_service import (
    validate_catalog_options,
    validate_descriptor_options,
)
from dal_obscura.sources.plugins import PluginAdmissionError

_IMAGE_DIGEST = re.compile(r"[^\s]+@sha256:[0-9a-f]{64}\Z")
_IMAGE_ID = re.compile(r"sha256:[0-9a-f]{64}\Z")
_INTERNAL = "dal_obscura.interfaces.cli.plugin_image"
_LOCK_PATH = "/app/plugin-lock.json"


def run(arguments: list[str], *, capture: bool = False) -> str:
    """Run argument vectors without a shell; never print captured service credentials."""
    result = subprocess.run(arguments, check=True, text=True, capture_output=capture, timeout=900)
    return result.stdout.strip() if capture else ""


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def write_json(path: Path, value: dict[str, Any]) -> None:
    """Replace local drafts atomically; leave existing state intact on failure."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)


def target_settings(workspace: Path) -> dict[str, Any]:
    target = read_json(workspace / "target.json")
    if not _IMAGE_DIGEST.fullmatch(str(target.get("base_image", ""))):
        raise ValueError("base_image must be an immutable image@sha256:digest reference")
    if not _IMAGE_DIGEST.fullmatch(str(target.get("uv_image", ""))):
        raise ValueError("uv_image must be an immutable image@sha256:digest reference")
    return target


def image_id(reference: str) -> str:
    identity = run(["docker", "image", "inspect", "--format", "{{.Id}}", reference], capture=True)
    if not _IMAGE_ID.fullmatch(identity):
        raise ValueError("Container engine returned an invalid image identity")
    return identity


def inspect_image(image: str, distributions: list[str]) -> dict[str, Any]:
    payload = run(
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
            _INTERNAL,
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
    run(["docker", "build", "--tag", tag, str(context)])
    return image_id(tag)


def add_plugin(workspace: Path, specification: str) -> None:
    target = target_settings(workspace)
    requirement = Requirement(specification)
    if requirement.url or requirement.marker or requirement.extras:
        raise ValueError(
            "Use a package name with an optional version constraint, without URLs/extras"
        )
    candidate_path = workspace / "candidate.json"
    previous = read_json(candidate_path) if candidate_path.exists() else {}
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
    write_json(candidate_path, candidate)
    show_admission(candidate, previous)


def show_admission(candidate: dict[str, Any], previous: dict[str, Any]) -> None:
    old = {
        (item["kind"], item["plugin_id"]): item["lock"]
        for item in previous.get("lock", {}).get("plugins", [])
    }
    current = candidate["lock"]["plugins"]
    for item in current:
        lock = item["lock"]
        key = (item["kind"], item["plugin_id"])
        state = "unchanged" if old.get(key) == lock else "changed" if key in old else "added"
        print(f"{state}: {key[0]}:{key[1]} — {lock[0]}=={lock[1]} (API {lock[2]})")
        print(f"  descriptor {lock[3]}\n  artifact   {lock[4]}")
    removed = set(old) - {(item["kind"], item["plugin_id"]) for item in current}
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
    candidate = read_json(workspace / "candidate.json")
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
    options = read_json(args.options) if args.options else collect_options(matches[0], name)
    validate_options(matches[0], options)
    path = workspace / "catalogs.json"
    catalogs = read_json(path) if path.exists() else {}
    revision = args.expected_revision
    if revision is None:
        revision = catalogs.get(name, {}).get("expected_revision", 0)
    catalogs[name] = {
        "plugin_id": args.plugin_id,
        "options": options,
        "expected_revision": revision,
    }
    write_json(path, catalogs)
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
    candidate = read_json(workspace / "candidate.json")
    if candidate["base_image"] != target["base_image"]:
        raise ValueError("Base image changed; run plugin add again before deployment")
    image = candidate["image"]
    if not _IMAGE_ID.fullmatch(image) or image_id(image) != image:
        raise ValueError("Prepared plugin image is unavailable or changed")
    actual = inspect_image(image, list(candidate["requirements"]))
    if actual != {"lock": candidate["lock"], "descriptors": candidate["descriptors"]}:
        raise ValueError("Prepared plugin metadata changed; run plugin add again")
    verified_path = workspace / "verified.json"
    previous = read_json(verified_path) if verified_path.exists() else {}
    show_admission(candidate, previous)
    repository = str(target["image_repository"])
    if not re.fullmatch(r"[a-z0-9][a-z0-9./:_-]*", repository) or not re.fullmatch(
        r"[a-z0-9][a-z0-9._-]*", repository.rsplit("/", 1)[-1]
    ):
        raise ValueError("image_repository must be a container repository, without tag/digest")
    release_id = uuid.uuid4().hex
    directory = workspace / "releases" / release_id
    directory.mkdir(parents=True)
    write_json(directory / "plugin-lock.json", candidate["lock"])
    (directory / "plugin-lock.json").chmod(0o444)
    (directory / "Dockerfile").write_text(
        f"FROM {image}\nCOPY --chmod=0444 plugin-lock.json {_LOCK_PATH}\n"
        f"ENV DAL_OBSCURA_PLUGIN_LOCK_FILE={_LOCK_PATH}\n"
    )
    final_image = build_image(directory)
    # Check the final image layer changed no installed plugin artifacts.
    if inspect_image(final_image, list(candidate["requirements"])) != actual:
        raise ValueError("Final image artifacts do not match prepared admission")
    catalogs_path = workspace / "catalogs.json"
    catalogs = read_json(catalogs_path) if catalogs_path.exists() else {}
    descriptors = {
        item["plugin_id"]: item for item in candidate["descriptors"] if item["kind"] == "catalog"
    }
    for draft in catalogs.values():
        if draft["plugin_id"] not in descriptors:
            raise ValueError("Prepared catalog references a plugin outside selected admission")
        validate_options(descriptors[draft["plugin_id"]], draft["options"])
    release = {
        **candidate,
        "image": final_image,
        "release_id": release_id,
        "lock_sha256": hashlib.sha256((directory / "plugin-lock.json").read_bytes()).hexdigest(),
        "tag": f"{repository}:dal-obscura-{release_id}",
        "catalogs": catalogs,
    }
    write_json(directory / "release.json", release)
    print(f"Prepared release {release_id}: {final_image}")
    return directory, release


class NoRedirect(HTTPRedirectHandler):
    """Never forward deployment credentials to a redirect destination."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ValueError("Control API redirects are not allowed; configure the final HTTPS URL")


def api_request(
    target: dict[str, Any], method: str, path: str, body: dict[str, Any] | None = None
) -> Any:
    base = str(target["control_url"]).rstrip("/")
    parsed = urlsplit(base)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("control_url must not contain credentials, query or fragment")
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and parsed.hostname in {"localhost", "127.0.0.1", "::1"}
    ):
        raise ValueError("control_url requires HTTPS except on loopback")
    token = os.environ[target.get("admin_token_env", "DAL_OBSCURA_DEPLOY_ADMIN_TOKEN")]
    request = Request(
        base + path,
        method=method,
        data=None if body is None else json.dumps(body, allow_nan=False).encode(),
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with build_opener(NoRedirect()).open(request, timeout=30) as response:
        return json.load(response)


def publish_catalogs(target: dict[str, Any], catalogs: dict[str, Any]) -> None:
    if not catalogs:
        return
    inventory = {item["name"]: item for item in api_request(target, "GET", "/v1/catalogs")}
    pending = []
    # Check every revision before making changes. The API checks again atomically at each write.
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
        pending.append((name, draft))
    for name, draft in pending:
        api_request(target, "PUT", "/v1/catalogs/" + quote(name, safe=""), draft)
        print(f"Published catalog {name}")


def verify_workers(compose: list[str], image: str, lock_sha256: str) -> None:
    for service in ("control-plane", "data-plane"):
        containers = run([*compose, "ps", "--quiet", service], capture=True).splitlines()
        if not containers:
            raise ValueError(f"No running {service} containers")
        for container in containers:
            state = json.loads(run(["docker", "inspect", container], capture=True))[0]
            if state["Image"] != image or not state["State"]["Running"]:
                raise ValueError(f"{service} worker does not run the selected release")
            if f"DAL_OBSCURA_PLUGIN_LOCK_FILE={_LOCK_PATH}" not in state["Config"]["Env"]:
                raise ValueError(f"{service} worker does not use the embedded lock")
            run(
                [
                    "docker",
                    "exec",
                    container,
                    "python",
                    "-m",
                    _INTERNAL,
                    "verify",
                    service,
                    lock_sha256,
                ]
            )
            print(f"Verified {service} worker {container[:12]}")


def validate_smoke(settings: dict[str, Any]) -> None:
    expected = settings["expected_rows"]
    if not isinstance(expected, list) or not expected or len(expected) > 1000:
        raise ValueError("smoke.expected_rows must contain 1-1000 synthetic non-secret rows")
    if not all(isinstance(row, dict) for row in expected):
        raise ValueError("smoke.expected_rows must be JSON objects")
    uri = str(settings["uri"])
    if not uri.startswith("grpc+tls://") and not uri.startswith("grpc+tcp://127.0.0.1:"):
        raise ValueError("Flight smoke requires TLS except on loopback")
    if not settings.get("target") or not isinstance(settings.get("columns"), list):
        raise ValueError("smoke requires a target and a columns list")
    for key in ("ca_file", "cert_file", "key_file"):
        if settings.get(key):
            Path(settings[key]).read_bytes()


def read_smoke_endpoint(
    reader: Any,
    schema: Any,
    rows: list[dict[str, Any]],
    max_rows: int,
    chunks: int,
    deadline: float,
) -> int:
    try:
        while True:
            if time.monotonic() >= deadline:
                raise ValueError("Authenticated Flight smoke deadline exceeded")
            try:
                batch = reader.read_chunk().data
            except StopIteration:
                return chunks
            chunks += 1
            if chunks > 1000:
                raise ValueError("Flight smoke exceeded batch budget")
            if batch is None:
                continue
            if len(rows) + batch.num_rows > max_rows:
                raise ValueError("Flight smoke exceeded bounded expected result")
            if not batch.schema.equals(schema, check_metadata=True):
                raise ValueError("Flight smoke batch schema differs from admitted schema")
            rows.extend(batch.to_pylist())
    finally:
        with suppress(Exception):
            reader.cancel()


def smoke_read(settings: dict[str, Any]) -> None:
    import pyarrow as pa
    import pyarrow.flight as flight

    from dal_obscura.interfaces.flight_contract import encode_plan_command

    validate_smoke(settings)
    expected = settings["expected_rows"]
    token = os.environ[settings.get("token_env", "DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN")]
    options = {
        option: Path(settings[key]).read_bytes()
        for key, option in (
            ("ca_file", "tls_root_certs"),
            ("cert_file", "cert_chain"),
            ("key_file", "private_key"),
        )
        if settings.get(key)
    }
    descriptor = flight.FlightDescriptor.for_command(
        encode_plan_command(
            catalog=settings.get("catalog"),
            target=settings["target"],
            columns=settings["columns"],
            row_filter=settings.get("row_filter"),
            include_typed_paths=True,
        )
    )
    deadline = time.monotonic() + 30

    def call_options() -> flight.FlightCallOptions:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ValueError("Authenticated Flight smoke deadline exceeded")
        return flight.FlightCallOptions(
            timeout=remaining, headers=[(b"authorization", f"Bearer {token}".encode())]
        )

    rows: list[dict[str, Any]] = []
    chunks = 0
    try:
        with flight.FlightClient(settings["uri"], **options) as transport:
            schema = transport.get_schema(descriptor, options=call_options()).schema
            plan = transport.get_flight_info(descriptor, options=call_options())
            if not plan.schema.equals(schema, check_metadata=True) or len(plan.endpoints) > 256:
                raise ValueError("Flight smoke plan/schema differs or exceeds task budget")
            for endpoint in plan.endpoints:
                reader = transport.do_get(endpoint.ticket, options=call_options())
                chunks = read_smoke_endpoint(reader, schema, rows, len(expected), chunks, deadline)
    except (flight.FlightError, pa.ArrowException) as exc:
        raise ValueError(
            "Authenticated Flight smoke failed; release was not marked verified"
        ) from exc

    def encode(row: dict[str, Any]) -> str:
        return json.dumps(row, sort_keys=True, default=str)

    if sorted(map(encode, rows)) != sorted(map(encode, expected)):
        raise ValueError("Flight smoke values differ from synthetic expected rows")
    print(f"Verified authenticated Flight read: {len(rows)} rows")


def deploy(workspace: Path, args: argparse.Namespace) -> None:
    target = target_settings(workspace)
    # Missing target settings fail before any publication or runtime changes.
    for key in ("compose_file", "image_repository", "control_url", "smoke"):
        if not target.get(key):
            raise ValueError(f"Deployment target requires {key}")
    validate_smoke(target["smoke"])
    directory, release = prepare_release(workspace, target)
    override = directory / "compose.json"

    def write_override(reference: str) -> None:
        write_json(
            override,
            {
                "services": {
                    service: {
                        "image": reference,
                        "environment": {"DAL_OBSCURA_PLUGIN_LOCK_FILE": _LOCK_PATH},
                    }
                    for service in ("control-plane", "data-plane")
                }
            },
        )

    write_override(release["image"])
    compose = compose_arguments(workspace, target, override)
    run([*compose, "config", "--quiet"])
    if args.prepare:
        print(f"Prepared only: {directory.resolve()}")
        return
    # Read-only authentication/preflight before an operator approves the concrete release.
    api_request(target, "GET", "/v1/plugins")
    os.environ[target["smoke"].get("token_env", "DAL_OBSCURA_DEPLOY_FLIGHT_TOKEN")]
    if (
        not args.yes
        and input("Publish this admission and deploy both planes? [y/N]: ").strip().lower() != "y"
    ):
        print("Release prepared; deployment cancelled.")
        return
    run(["docker", "tag", release["image"], release["tag"]])
    run(["docker", "push", release["tag"]])
    identities = json.loads(
        run(
            ["docker", "image", "inspect", "--format", "{{json .RepoDigests}}", release["tag"]],
            capture=True,
        )
    )
    prefix = target["image_repository"] + "@sha256:"
    digests = [value for value in identities if value.startswith(prefix)]
    if len(digests) != 1 or not _IMAGE_DIGEST.fullmatch(digests[0]):
        raise ValueError("Published image has no unique immutable repository digest")
    release["published_image"] = digests[0]
    write_json(directory / "release.json", release)
    write_override(digests[0])
    # Existing schema only: routine deployment never runs migrations or changes demo services.
    run(
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
    smoke_read(target["smoke"])
    if release["catalogs"]:
        inventory = {item["name"]: item for item in api_request(target, "GET", "/v1/catalogs")}
        path = workspace / "catalogs.json"
        drafts = read_json(path)
        for name, published in release["catalogs"].items():
            current = inventory[name]
            if (
                drafts.get(name) == published
                and current["options"] == published["options"]
                and current["plugin_id"] == published["plugin_id"]
            ):
                drafts[name]["expected_revision"] = current["revision"]
        write_json(path, drafts)
    write_json(workspace / "verified.json", release)
    print(f"Release {release['release_id']} published and verified: {digests[0]}")


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(
        description="dal-obscura operator commands; no arguments starts Flight"
    )
    cli.add_argument("--workspace", type=Path, default=Path(".dal-obscura-deploy"))
    commands = cli.add_subparsers(dest="command", required=True)
    plugins = commands.add_parser("plugin", help="Prepare plugin admission and catalog settings")
    actions = plugins.add_subparsers(dest="action", required=True)
    add = actions.add_parser("add", help="Install a package in an isolated worker image")
    add.add_argument("package")
    configure = actions.add_parser(
        "configure", help="Prepare catalog options and secret references"
    )
    configure.add_argument("plugin_id")
    configure.add_argument("--name")
    configure.add_argument("--options", type=Path)
    configure.add_argument("--expected-revision", type=int)
    deployment = commands.add_parser(
        "deploy", help="Publish immutable image/lock and verify Compose workers"
    )
    deployment.add_argument(
        "--prepare", action="store_true", help="Build/review release without publishing"
    )
    deployment.add_argument(
        "--yes", action="store_true", help="Approve the printed admission for automation"
    )
    return cli


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    if not arguments:
        from dal_obscura.interfaces.cli.read import main as serve

        serve()
        return 0
    if arguments in (["--help"], ["-h"]):
        from dal_obscura.interfaces.cli.read import _HELP_TEXT

        print(_HELP_TEXT)
        parser().print_help()
        return 0
    args = parser().parse_args(arguments)
    try:
        if args.command == "deploy":
            deploy(args.workspace, args)
        elif args.action == "add":
            add_plugin(args.workspace, args.package)
        else:
            configure_plugin(args.workspace, args)
    except (OSError, ValueError, KeyError, subprocess.SubprocessError, PluginAdmissionError) as exc:
        # Captured subprocess stderr and HTTP bodies may contain secrets; do not echo them.
        if isinstance(exc, subprocess.SubprocessError):
            message = "Container command failed; release was not marked verified"
        elif isinstance(exc, KeyError):
            message = "Missing deployment setting or credential environment variable"
        else:
            message = str(exc)
        print(message, file=sys.stderr)
        return 1
    return 0
