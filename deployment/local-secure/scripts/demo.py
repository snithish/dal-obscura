"""Run the self-contained HTTPS/OIDC and Flight mTLS local example."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import ssl
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parents[1]
sys.path.insert(0, str(REPO))

from examples.demo.keycloak.scripts import demo as common  # noqa: E402

DEFAULTS = {
    **common.DEFAULTS,
    "UI_PORT": "28443",
    "KEYCLOAK_PORT": "24443",
    "FLIGHT_PORT": "28815",
    "EDGE_IMAGE": "docker.io/library/caddy:2.11.1-alpine",
}
SECRET_KEYS = (*common.SECRET_KEYS, "MIGRATION_DB_PASSWORD", "DATA_DB_PASSWORD")
PREFIX = "dal-obscura-secure-local-"
TLS_FILES = tuple(
    f"{name}.{suffix}"
    for name in ("ca", "ui", "keycloak", "flight", "client", "untrusted-client")
    for suffix in ("crt", "key")
)


def initialize(root: Path, overrides: Mapping[str, str]) -> dict[str, str]:
    return common.initialize_config(
        root, overrides, defaults=DEFAULTS, secret_keys=SECRET_KEYS, project_prefix=PREFIX
    )


def read_config(root: Path) -> dict[str, str]:
    return common.read_config(
        root, defaults=DEFAULTS, secret_keys=SECRET_KEYS, project_prefix=PREFIX
    )


def public_urls(values: Mapping[str, str]) -> dict[str, str]:
    return {
        "ui": f"https://localhost:{values['UI_PORT']}",
        "issuer": f"https://localhost:{values['KEYCLOAK_PORT']}/realms/dal-obscura-demo",
        "flight": f"grpc+tls://localhost:{values['FLIGHT_PORT']}",
    }


def ensure_tls(root: Path) -> None:
    target = root / ".tls"
    if target.exists():
        validate_tls(root)
        return
    if shutil.which("openssl") is None:
        raise ValueError("Install OpenSSL before initializing this secure example.")
    # Publish the complete identity set with one rename. Interrupted generation
    # never exposes a partial CA or silently replaces an existing identity.
    with tempfile.TemporaryDirectory(prefix=".tls-build-", dir=root) as temporary:
        stage = Path(temporary)

        def run(*args: str) -> None:
            common._run(["openssl", *args], {}, timeout=30)

        run(
            "req",
            "-x509",
            "-newkey",
            "rsa:3072",
            "-nodes",
            "-sha256",
            "-days",
            "365",
            "-subj",
            "/CN=dal-obscura secure-local CA",
            "-keyout",
            str(stage / "ca.key"),
            "-out",
            str(stage / "ca.crt"),
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        )
        for name, dns in (
            ("ui", "localhost,edge"),
            ("keycloak", "localhost,keycloak"),
            ("flight", "localhost,data-plane"),
            ("client", ""),
            ("untrusted-client", ""),
        ):
            key, certificate = stage / f"{name}.key", stage / f"{name}.crt"
            csr, extension = stage / f"{name}.csr", stage / f"{name}.ext"
            run(
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-sha256",
                "-subj",
                f"/CN={name}",
                "-keyout",
                str(key),
                "-out",
                str(csr),
            )
            text = (
                "basicConstraints=critical,CA:FALSE\n"
                "keyUsage=critical,digitalSignature,keyEncipherment\n"
            )
            text += "extendedKeyUsage=" + ("serverAuth" if dns else "clientAuth") + "\n"
            if dns:
                text += (
                    "subjectAltName="
                    + ",".join(f"DNS:{item}" for item in dns.split(","))
                    + ",IP:127.0.0.1\n"
                )
            extension.write_text(text)
            signing = (
                ["-signkey", str(key)]
                if name == "untrusted-client"
                else ["-CA", str(stage / "ca.crt"), "-CAkey", str(stage / "ca.key")]
            )
            run(
                "x509",
                "-req",
                "-in",
                str(csr),
                *signing,
                "-set_serial",
                str(int.from_bytes(os.urandom(16), "big")),
                "-days",
                "365",
                "-sha256",
                "-extfile",
                str(extension),
                "-out",
                str(certificate),
            )
            csr.unlink()
            extension.unlink()
        for path in stage.iterdir():
            path.chmod(0o600 if path.suffix == ".key" else 0o644)
        stage.chmod(0o700)
        stage.rename(target)


def validate_tls(root: Path) -> None:
    directory = root / ".tls"
    if directory.is_symlink() or not all((directory / name).is_file() for name in TLS_FILES):
        raise ValueError("Local TLS material is incomplete; restore it or explicitly ./demo reset.")
    if directory.stat().st_mode & 0o077:
        raise ValueError("Local .tls directory must have mode 700.")
    for name in TLS_FILES:
        path = directory / name
        if path.is_symlink() or (name.endswith(".key") and path.stat().st_mode & 0o077):
            raise ValueError(f"TLS key must be owner-only and not a symlink: {name}")
        if name.endswith(".crt"):
            try:
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                context.load_cert_chain(path, directory / name.replace(".crt", ".key"))
            except ssl.SSLError as exc:
                raise ValueError(f"TLS key and certificate do not match: {name}") from exc
            common._run(
                ["openssl", "x509", "-in", str(path), "-noout", "-checkend", "86400"],
                {},
                timeout=10,
            )
    for name in ("ui", "keycloak", "flight", "client"):
        purpose = "sslclient" if name == "client" else "sslserver"
        common._run(
            [
                "openssl",
                "verify",
                "-purpose",
                purpose,
                "-CAfile",
                str(directory / "ca.crt"),
                str(directory / f"{name}.crt"),
            ],
            {},
            timeout=10,
        )


def compose(values: Mapping[str, str], *args: str, **kwargs) -> str:
    return common._compose(values, *args, root=ROOT, **kwargs)


def start(values: Mapping[str, str], *, initialize: bool) -> None:
    print("Installing private service TLS material...", flush=True)
    compose(values, "run", "--rm", "--no-deps", "tls-install")
    print("Starting PostgreSQL and HTTPS Keycloak...", flush=True)
    compose(values, "up", "-d", "--wait", "--wait-timeout", "180", "postgres", "keycloak")
    if initialize:
        print("Migrating, granting database permissions, and seeding Iceberg...", flush=True)
        compose(values, "run", "--rm", "--no-deps", "migrate")
        compose(values, "run", "--rm", "--no-deps", "postgres-grants")
        compose(values, "run", "--rm", "--no-deps", "seed")
    else:
        compose(values, "run", "--rm", "--no-deps", "migrate", "dal-obscura-migrate", "check")
    print("Starting production-profile control plane...", flush=True)
    compose(values, "up", "-d", "--wait", "--wait-timeout", "90", "control-plane")
    if initialize:
        compose(values, "run", "--rm", "--no-deps", "provision")
    print("Starting Flight mTLS and browser HTTPS...", flush=True)
    compose(values, "up", "-d", "--wait", "--wait-timeout", "90", "data-plane", "ui", "edge")
    doctor(values)
    print(f"Ready. UI: {public_urls(values)['ui']}  Flight: {public_urls(values)['flight']}")
    print("Run ./demo credentials for passwords; ./demo check for TLS, reads, and SSO.")


def doctor(values: Mapping[str, str]) -> None:
    validate_tls(ROOT)
    compose(values, "config", "--quiet")
    context = ssl.create_default_context(cafile=str(ROOT / ".tls/ca.crt"))
    for url in (
        public_urls(values)["ui"] + "/",
        public_urls(values)["issuer"] + "/.well-known/openid-configuration",
    ):
        with urlopen(url, context=context, timeout=10) as response:
            assert response.status == 200
    print(
        "HTTPS certificate chains and hostnames verified; bootstrap disabled; Flight requires mTLS."
    )
    raw = compose(values, "ps", "--format", "json").strip()
    rows = (
        json.loads(raw) if raw.startswith("[") else [json.loads(line) for line in raw.splitlines()]
    )
    if len(rows) != 6 or any(row.get("Health") != "healthy" for row in rows):
        raise ValueError("Expected six healthy services; run ./demo up and inspect ./demo logs.")
    print("Six healthy services: " + ", ".join(row["Service"] for row in rows))


def check(values: Mapping[str, str], *, reads_only: bool) -> None:
    doctor(values)
    print(compose(values, "run", "--rm", "--no-deps", "check"), end="")
    if reads_only:
        return
    # Host probes verify CA chains and hostnames first. Chromium receives
    # only the two expected browser endpoint public keys, scoped to this run.
    pins = []
    for name in ("ui", "keycloak"):
        public_key = common._run(
            [
                "openssl",
                "x509",
                "-in",
                str(ROOT / f".tls/{name}.crt"),
                "-pubkey",
                "-noout",
            ],
            {},
        )
        der = subprocess.run(
            [
                "openssl",
                "pkey",
                "-pubin",
                "-outform",
                "DER",
            ],
            input=public_key.encode(),
            capture_output=True,
            check=True,
            timeout=10,
        ).stdout
        pins.append(base64.b64encode(hashlib.sha256(der).digest()).decode())
    common.check_browser(
        values,
        root=ROOT,
        repo=REPO,
        ui_url=public_urls(values)["ui"],
        extra_env={
            "NODE_EXTRA_CA_CERTS": str(ROOT / ".tls/ca.crt"),
            "DAL_OBSCURA_DEMO_TLS_SPKI": ",".join(pins),
        },
    )


def credentials(values: Mapping[str, str]) -> None:
    for user, key in (
        ("demo-admin", "DEMO_ADMIN_PASSWORD"),
        ("asset-owner", "ASSET_OWNER_PASSWORD"),
        ("us-analyst", "US_ANALYST_PASSWORD"),
        ("eu-analyst", "EU_ANALYST_PASSWORD"),
        ("data-steward", "DATA_STEWARD_PASSWORD"),
        ("blocked-user", "BLOCKED_USER_PASSWORD"),
        ("Keycloak admin", "KEYCLOAK_ADMIN_PASSWORD"),
    ):
        print(f"{user}: {values[key]}")


def remove_identity(root: Path) -> None:
    identity = root / ".tls"
    if identity.is_symlink():
        identity.unlink()
    elif identity.exists():
        shutil.rmtree(identity)
    (root / ".env").unlink(missing_ok=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    for action in ("init", "up", "down", "reset", "credentials", "doctor", "config"):
        sub.add_parser(action)
    sub.add_parser("check").add_argument("--reads-only", action="store_true")
    sub.add_parser("logs").add_argument("service", nargs="?")
    args = parser.parse_args(argv)
    values: dict[str, str] = {}
    try:
        with common.operation_lock(ROOT):
            if args.action == "reset":
                try:
                    values = read_config(ROOT)
                except ValueError:
                    values = {
                        **DEFAULTS,
                        **dict.fromkeys(SECRET_KEYS, "0" * 48),
                        "PROJECT_NAME": PREFIX
                        + hashlib.sha256(str(ROOT.resolve()).encode()).hexdigest()[:10],
                    }
                compose(values, "down", "--volumes", "--remove-orphans", env_file=Path("/dev/null"))
                remove_identity(ROOT)
                print("Secure local example reset.")
                return 0
            values = initialize(ROOT, os.environ) if args.action == "init" else read_config(ROOT)
            if args.action == "credentials":
                credentials(values)
                return 0
            if args.action in {"init", "up"}:
                ensure_tls(ROOT) if args.action == "init" else validate_tls(ROOT)
                common._preflight(values, root=ROOT)
                if args.action == "init":
                    print("Building product service and UI images...", flush=True)
                    common._run(
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
                    common._run(
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
                    common._run(
                        [
                            "docker",
                            "build",
                            "--build-arg",
                            f"EDGE_IMAGE={values['EDGE_IMAGE']}",
                            "-t",
                            f"{values['PROJECT_NAME']}-edge:local",
                            "-f",
                            str(ROOT / "Dockerfile.edge"),
                            str(ROOT),
                        ],
                        values,
                        timeout=600,
                    )
                start(values, initialize=args.action == "init")
            elif args.action == "down":
                compose(values, "down")
                print("Stopped; databases, warehouse, credentials, and CA preserved.")
            elif args.action == "config":
                validate_tls(ROOT)
                compose(values, "config", "--quiet")
                print("Secure local Compose configuration valid.")
            elif args.action == "doctor":
                doctor(values)
            elif args.action == "logs":
                print(
                    compose(
                        values,
                        "logs",
                        "--no-color",
                        "--tail=100",
                        *([args.service] if args.service else []),
                    ),
                    end="",
                )
            else:
                check(values, reads_only=args.reads_only)
    except (ValueError, RuntimeError, OSError, AssertionError) as exc:
        print(common.redact(str(exc), values), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
