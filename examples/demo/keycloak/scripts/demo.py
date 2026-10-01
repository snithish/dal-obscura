"""Manage the isolated local example with standard-library-only host tooling."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from time import monotonic, sleep

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[2]
DEFAULTS = {
    "UI_PORT": "28821",
    "KEYCLOAK_PORT": "20080",
    "FLIGHT_PORT": "28115",
    "KEYCLOAK_IMAGE": "quay.io/keycloak/keycloak:26.7.4",
    "POSTGRES_IMAGE": "docker.io/library/postgres:17.10-alpine",
}
SECRET_KEYS = (
    "POSTGRES_PASSWORD",
    "KEYCLOAK_DB_PASSWORD",
    "APP_DB_PASSWORD",
    "ICEBERG_DB_PASSWORD",
    "KEYCLOAK_ADMIN_PASSWORD",
    "ADMIN_TOKEN",
    "TICKET_SECRET",
    "OIDC_CLI_CLIENT_SECRET",
    "DEMO_ADMIN_PASSWORD",
    "ASSET_OWNER_PASSWORD",
    "US_ANALYST_PASSWORD",
    "EU_ANALYST_PASSWORD",
    "DATA_STEWARD_PASSWORD",
    "BLOCKED_USER_PASSWORD",
)


def public_urls(values: Mapping[str, str]) -> dict[str, str]:
    return {
        "ui": f"http://localhost:{values['UI_PORT']}",
        "issuer": f"http://localhost:{values['KEYCLOAK_PORT']}/realms/dal-obscura-demo",
        "flight": f"grpc+tcp://localhost:{values['FLIGHT_PORT']}",
    }


def _validate(
    values: Mapping[str, str],
    *,
    defaults: Mapping[str, str] = DEFAULTS,
    secret_keys: Sequence[str] = SECRET_KEYS,
    project_prefix: str = "dal-obscura-local-",
) -> None:
    required = {*defaults, *secret_keys, "PROJECT_NAME"}
    if required - values.keys():
        raise ValueError("Local .env is incomplete; restore it or explicitly run ./demo reset.")
    for key in ("UI_PORT", "KEYCLOAK_PORT", "FLIGHT_PORT"):
        if (
            not values[key].isascii()
            or not values[key].isdigit()
            or not 1024 <= int(values[key]) <= 65535
        ):
            raise ValueError(f"{key} port must be an integer between 1024 and 65535.")
    for key, value in values.items():
        if not re.fullmatch(r"[A-Z][A-Z0-9_]*", key) or not re.fullmatch(
            r"[A-Za-z0-9_./:@+-]+", value
        ):
            raise ValueError(f"Invalid local configuration value for {key}.")
    if len({values[key] for key in defaults if key.endswith("_PORT")}) != 3:
        raise ValueError("UI, Keycloak, and Flight ports must be distinct.")
    if not re.fullmatch(re.escape(project_prefix) + r"[a-z0-9-]+", values["PROJECT_NAME"]):
        raise ValueError(f"PROJECT_NAME must use the {project_prefix} prefix.")
    if any(len(values[key]) < 32 for key in secret_keys):
        raise ValueError("Local generated secrets must contain at least 32 characters.")


def read_config(
    root: Path,
    *,
    defaults: Mapping[str, str] = DEFAULTS,
    secret_keys: Sequence[str] = SECRET_KEYS,
    project_prefix: str = "dal-obscura-local-",
) -> dict[str, str]:
    path = root / ".env"
    if not path.exists():
        raise ValueError("Local example is not initialized. Run ./demo init first.")
    if path.is_symlink():
        raise ValueError("Local .env must be a regular file, not a symbolic link.")
    values = {}
    for line in path.read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        key, sep, value = line.partition("=")
        if not sep or key in values:
            raise ValueError("Local .env is malformed or contains duplicate keys.")
        values[key] = value
    _validate(values, defaults=defaults, secret_keys=secret_keys, project_prefix=project_prefix)
    path.chmod(0o600)
    return values


def initialize_config(
    root: Path,
    overrides: Mapping[str, str],
    *,
    defaults: Mapping[str, str] = DEFAULTS,
    secret_keys: Sequence[str] = SECRET_KEYS,
    project_prefix: str = "dal-obscura-local-",
) -> dict[str, str]:
    if (root / ".env").exists():
        current = read_config(
            root, defaults=defaults, secret_keys=secret_keys, project_prefix=project_prefix
        )
        if any(
            overrides.get(key, value) != value for key, value in current.items() if key in defaults
        ):
            raise ValueError(
                "Local example is already configured; keep its ports/images or reset explicitly."
            )
        return current
    values = {key: overrides.get(key, value) for key, value in defaults.items()}
    values["PROJECT_NAME"] = (
        project_prefix + hashlib.sha256(str(root.resolve()).encode()).hexdigest()[:10]
    )
    values.update({key: secrets.token_hex(24) for key in secret_keys})
    _validate(values, defaults=defaults, secret_keys=secret_keys, project_prefix=project_prefix)
    path = root / ".env"
    # Exclusive creation avoids replacing secrets if another initializer races us.
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as output:
        output.write("# Generated local credentials; preserved until explicit reset.\n")
        output.write("\n".join(f"{key}={value}" for key, value in values.items()) + "\n")
    return values


@contextmanager
def operation_lock(root: Path) -> Iterator[None]:
    with (root / ".demo.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("Wait for another demo command to finish before retrying.") from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def redact(text: str, values: Mapping[str, str]) -> str:
    for key in values:
        if not key.endswith(("_PASSWORD", "_SECRET", "_TOKEN")):
            continue
        if values.get(key):
            text = text.replace(values[key], "[redacted]")
    return text


def check_ports(values: Mapping[str, str], owned: set[str], *, timeout: float = 0) -> None:
    deadline = monotonic() + timeout
    for key in ("UI_PORT", "KEYCLOAK_PORT", "FLIGHT_PORT"):
        if key not in values or key in owned:
            continue
        while True:
            try:
                with socket.socket() as probe:
                    # Match server bind behavior: closed TCP connections in
                    # TIME_WAIT are reusable, while active listeners still fail.
                    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    probe.bind(("127.0.0.1", int(values[key])))
                break
            except OSError as exc:
                # VM-based runtimes can briefly retain forwarding sockets after
                # Compose down has returned. Wait for release, with a deadline.
                if monotonic() < deadline:
                    sleep(0.1)
                    continue
                raise ValueError(
                    f"{key} port {values[key]} is occupied. "
                    "Stop its owner or choose a free port before init."
                ) from exc


def _run(command: Sequence[str], values: Mapping[str, str], *, timeout: int = 300) -> str:
    environment = {**os.environ, **values}
    try:
        result = subprocess.run(
            command, cwd=ROOT, env=environment, capture_output=True, text=True, timeout=timeout
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"{command[0]} exceeded its {timeout}s deadline; inspect ./demo logs."
        ) from exc
    if result.returncode:
        evidence = redact((result.stdout + result.stderr)[-6000:], values)
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(command[:3])}\n{evidence}"
        )
    return redact(result.stdout, values)


def _compose(
    values: Mapping[str, str],
    *args: str,
    timeout: int = 300,
    env_file: Path | None = None,
    root: Path | None = None,
) -> str:
    root = root or ROOT
    return _run(
        [
            "docker",
            "compose",
            "--project-name",
            values["PROJECT_NAME"],
            "--env-file",
            str(env_file or root / ".env"),
            "-f",
            str(root / "compose.yaml"),
            *args,
        ],
        values,
        timeout=timeout,
    )


def _preflight(values: Mapping[str, str], *, root: Path | None = None) -> None:
    if shutil.which("docker") is None:
        raise ValueError(
            "Docker/Podman CLI is missing. Install Docker Desktop or Podman with Compose v2."
        )
    try:
        _run(["docker", "info"], values, timeout=20)
    except RuntimeError as exc:
        raise ValueError(
            "Container runtime is unavailable. Start Docker Desktop or run podman machine start."
        ) from exc
    version = _run(["docker", "compose", "version", "--short"], values, timeout=20).strip()
    if not version.lstrip("v").startswith("2."):
        raise ValueError("This example requires Docker Compose v2, including when using Podman.")
    _compose(values, "config", "--quiet", timeout=20, root=root)
    owned = set()
    raw = _compose(values, "ps", "--format", "json", timeout=20, root=root).strip()
    if raw:
        records = (
            json.loads(raw)
            if raw.startswith("[")
            else [json.loads(line) for line in raw.splitlines()]
        )
        service_ports = {
            "ui": "UI_PORT",
            "edge": "UI_PORT",
            "keycloak": "KEYCLOAK_PORT",
            "data-plane": "FLIGHT_PORT",
        }
        owned = {
            service_ports[row["Service"]]
            for row in records
            if row.get("State") == "running" and row.get("Service") in service_ports
        }
    check_ports(values, owned, timeout=5)


def _start(values: Mapping[str, str], *, initialize: bool) -> None:
    print("Starting PostgreSQL and Keycloak...", flush=True)
    _compose(values, "up", "-d", "--wait", "--wait-timeout", "180", "postgres", "keycloak")
    print("Checking schema and fixture...", flush=True)
    if initialize:
        _compose(values, "run", "--rm", "--no-deps", "migrate")
        _compose(values, "run", "--rm", "--no-deps", "seed")
    else:
        _compose(values, "run", "--rm", "--no-deps", "migrate", "dal-obscura-migrate", "check")
    print("Starting control plane and ensuring initial configuration...", flush=True)
    _compose(values, "up", "-d", "--wait", "--wait-timeout", "90", "control-plane")
    if initialize:
        _compose(values, "run", "--rm", "--no-deps", "provision")
    print("Starting Flight and governance UI...", flush=True)
    _compose(values, "up", "-d", "--wait", "--wait-timeout", "90", "data-plane", "ui")
    urls = public_urls(values)
    print(
        f"Ready. UI: {urls['ui']}  Flight: {urls['flight']}\n"
        "Run ./demo credentials to show local passwords; ./demo check to verify."
    )


def _check(values: Mapping[str, str], *, reads_only: bool) -> None:
    print(_compose(values, "run", "--rm", "--no-deps", "check"), end="")
    if reads_only:
        return
    check_browser(values, root=ROOT, repo=REPO, ui_url=public_urls(values)["ui"])


def check_browser(
    values: Mapping[str, str],
    *,
    root: Path,
    repo: Path,
    ui_url: str,
    extra_env: Mapping[str, str] | None = None,
) -> None:
    if shutil.which("pnpm") is None:
        raise ValueError(
            "Browser check requires Node 24 and pnpm. "
            "Read checks passed; install UI tools and retry ./demo check."
        )
    ui = repo / "apps/governance-ui"
    if not (ui / "node_modules").exists():
        print("Installing locked UI test dependencies...", flush=True)
        _run(["pnpm", "--dir", str(ui), "install", "--frozen-lockfile"], values, timeout=600)
    print("Ensuring Chromium and verifying real SSO...", flush=True)
    _run(
        ["pnpm", "--dir", str(ui), "exec", "playwright", "install", "chromium"], values, timeout=600
    )
    env = {
        **values,
        "DAL_OBSCURA_E2E_LIVE_OIDC": "1",
        "DAL_OBSCURA_E2E_BASE_URL": ui_url,
        "DAL_OBSCURA_DEMO_ENV_FILE": str(root / ".env"),
        **(extra_env or {}),
    }
    print(
        _run(
            [
                "pnpm",
                "--dir",
                str(ui),
                "exec",
                "playwright",
                "test",
                "e2e/live-oidc-demo.spec.ts",
                "--workers=1",
            ],
            env,
        ),
        end="",
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Initialize, run, and verify the local Keycloak example."
    )
    sub = parser.add_subparsers(dest="action", required=True)
    for action in ("init", "up", "down", "reset", "credentials"):
        sub.add_parser(action)
    check = sub.add_parser("check")
    check.add_argument("--reads-only", action="store_true")
    logs = sub.add_parser("logs")
    logs.add_argument("service", nargs="?")
    args = parser.parse_args(argv)
    values: dict[str, str] = {}
    try:
        with operation_lock(ROOT):
            if args.action == "reset":
                try:
                    values = read_config(ROOT)
                except ValueError:
                    values = {**DEFAULTS, **dict.fromkeys(SECRET_KEYS, "0" * 48)}
                    values["PROJECT_NAME"] = (
                        "dal-obscura-local-"
                        + hashlib.sha256(str(ROOT.resolve()).encode()).hexdigest()[:10]
                    )
                _compose(
                    values, "down", "--volumes", "--remove-orphans", env_file=Path("/dev/null")
                )
                (ROOT / ".env").unlink(missing_ok=True)
                print("Local example reset.")
                return 0
            values = (
                initialize_config(ROOT, os.environ) if args.action == "init" else read_config(ROOT)
            )
            if args.action == "credentials":
                for user, key in (
                    ("demo-admin", "DEMO_ADMIN_PASSWORD"),
                    ("asset-owner", "ASSET_OWNER_PASSWORD"),
                    ("us-analyst", "US_ANALYST_PASSWORD"),
                    ("eu-analyst", "EU_ANALYST_PASSWORD"),
                    ("data-steward", "DATA_STEWARD_PASSWORD"),
                    ("blocked-user", "BLOCKED_USER_PASSWORD"),
                ):
                    print(f"{user}: {values[key]}")
                print(f"Keycloak admin: {values['KEYCLOAK_ADMIN_PASSWORD']}")
                return 0
            if args.action in {"init", "up"}:
                _preflight(values)
                if args.action == "init":
                    print("Building service and UI from this checkout...", flush=True)
                    _run(
                        [
                            "docker",
                            "build",
                            "-t",
                            f"{values['PROJECT_NAME']}-service:local",
                            str(REPO),
                        ],
                        values,
                        timeout=1200,
                    )
                    _run(
                        [
                            "docker",
                            "build",
                            "-t",
                            f"{values['PROJECT_NAME']}-ui:local",
                            "-f",
                            str(REPO / "ui/Dockerfile"),
                            str(REPO),
                        ],
                        values,
                        timeout=1200,
                    )
                _start(values, initialize=args.action == "init")
            elif args.action == "down":
                _compose(values, "down")
                print("Stopped; local state preserved.")
            elif args.action == "logs":
                services = [args.service] if args.service else []
                print(_compose(values, "logs", "--no-color", "--tail=100", *services), end="")
            else:
                _check(values, reads_only=args.reads_only)
    except (ValueError, RuntimeError, OSError) as exc:
        print(redact(str(exc), values), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
