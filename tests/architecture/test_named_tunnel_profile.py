import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[2]
PROFILE = ROOT / "deployment" / "named-tunnel"


def test_named_tunnel_profile_keeps_connector_private_and_origin_bound() -> None:
    compose = (PROFILE / "compose.yaml").read_text(encoding="utf-8")
    env = (PROFILE / ".env.example").read_text(encoding="utf-8")
    caddy = (PROFILE / "Caddyfile").read_text(encoding="utf-8")
    readme = (PROFILE / "README.md").read_text(encoding="utf-8")

    assert "cloudflared:" in compose
    assert "--token-file" in compose
    assert 'profiles: ["connector"]' in compose
    assert "image: ${DAL_OBSCURA_EDGE_IMAGE:?set DAL_OBSCURA_EDGE_IMAGE}" in compose
    assert "condition: service_healthy" in compose
    assert "  ui:\n    # The named origin" in compose
    assert "    ports: []" in compose
    assert (
        "DAL_OBSCURA_NAMED_HOST: ${DAL_OBSCURA_NAMED_HOST:?set DAL_OBSCURA_NAMED_HOST}" in compose
    )
    assert "ports:\n      - " not in compose
    assert "named_origin_cert" in compose and "named_origin_key" in compose
    assert "named_origin_ca" in compose
    assert "DAL_OBSCURA_CLOUDFLARED_IMAGE=cloudflare/cloudflared@sha256:" in env
    assert "DAL_OBSCURA_CLOUDFLARE_ACCESS_TEAM_NAME=" in env
    assert "DAL_OBSCURA_CLOUDFLARE_ACCESS_AUD_TAG=" in env
    assert "DAL_OBSCURA_CONTROL_PLANE_TRUSTED_PROXY_PEERS=REPLACE_WITH_UI_PROXY_IP_OR_CIDR" in env
    assert "DAL_OBSCURA_CONTROL_PLANE_BOOTSTRAP_ENABLED=false" in env
    assert "DAL_OBSCURA_UI_TLS_CERT_SOURCE=../local-secure/secrets/ui.crt" in env
    assert "DAL_OBSCURA_UI_TLS_KEY_SOURCE=../local-secure/secrets/ui.key" in env
    assert "DAL_OBSCURA_NAMED_ORIGIN_CA_SOURCE=../named-tunnel/secrets/origin-ca.crt" in env
    assert "DAL_OBSCURA_SECRET_PROVIDER_CONFIG=" in env
    assert "https://{$DAL_OBSCURA_NAMED_HOST}:8443" in caddy
    assert "Create the DNS record, tunnel, Access application" in readme
    assert "never creates or modifies" in readme


def test_named_tunnel_runner_fails_closed_and_redacts_edge_claims() -> None:
    runner = PROFILE / "run"
    source = runner.read_text(encoding="utf-8")
    result = subprocess.run(["sh", "-n", str(runner)], check=False, capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
    assert "OIDC callback must equal https://$host/auth/callback" in source
    assert "Bootstrap login must be disabled" in source
    assert "cloudflare_access=not-verified" in source
    assert "cloudflared.token" in source
    assert "compose --profile connector up -d --wait cloudflared" in source
    assert "compose up -d --wait" in source
    assert "compose --profile connector down" in source
    assert "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN" not in source
    assert "require_owner_only_secret" in source
    assert "Secret file must be owner-only" in source
    assert "validate_origin_certificate" in source
    assert "Origin certificate SAN does not match" in source
    assert "Origin certificate is not trusted" in source
    assert "*\\?*|*\\**" in source


def test_named_tunnel_runner_rejects_weak_secrets_and_wrong_origin_san(tmp_path: Path) -> None:
    named = tmp_path / "named-tunnel"
    local = tmp_path / "local-secure"
    (named / "secrets").mkdir(parents=True)
    (local / "secrets").mkdir(parents=True)
    runner = named / "run"
    runner.write_text((PROFILE / "run").read_text(encoding="utf-8"), encoding="utf-8")
    runner.chmod(0o755)
    host = "obscura.example.test"
    env_file = named / ".env"
    env_file.write_text(
        "\n".join(
            [
                f"DAL_OBSCURA_NAMED_HOST={host}",
                "DAL_OBSCURA_CLOUDFLARE_ACCESS_TEAM_NAME=team",
                "DAL_OBSCURA_CLOUDFLARE_ACCESS_AUD_TAG=aud-tag",
                "DAL_OBSCURA_CONTROL_PLANE_TRUSTED_PROXY_PEERS=10.0.0.8/32",
                f"DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI=https://{host}/auth/callback",
                f"DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_POST_LOGOUT_REDIRECT_URI=https://{host}",
                f"DAL_OBSCURA_CONTROL_PLANE_CORS_ORIGINS=https://{host}",
                "DAL_OBSCURA_CONTROL_PLANE_BOOTSTRAP_ENABLED=false",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    def certificate(prefix: Path, name: str, cert_host: str = host) -> tuple[Path, Path]:
        key = prefix / f"{name}.key"
        cert = prefix / f"{name}.crt"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                f"/CN={cert_host}",
                "-addext",
                f"subjectAltName=DNS:{cert_host}",
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=True,
            capture_output=True,
        )
        key.chmod(0o600)
        return cert, key

    origin_cert, origin_key = certificate(named / "secrets", "origin")
    (named / "secrets" / "origin-ca.crt").write_bytes(origin_cert.read_bytes())
    (named / "secrets" / "cloudflared.token").write_text("token\n", encoding="utf-8")
    (named / "secrets" / "cloudflared.token").chmod(0o600)
    for name in ("flight.crt", "client-ca.crt"):
        (local / "secrets" / name).write_text("certificate\n", encoding="utf-8")
    flight_key = local / "secrets" / "flight.key"
    flight_key.write_text("private\n", encoding="utf-8")
    flight_key.chmod(0o600)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker = fake_bin / "docker"
    docker.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    docker.chmod(0o755)
    env = {**os.environ, "PATH": f"{fake_bin}:{os.environ.get('PATH', '')}"}

    valid = subprocess.run(
        ["sh", str(runner), "config"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert valid.returncode == 0, valid.stderr
    original_env = env_file.read_text(encoding="utf-8")
    for invalid_host in ("192.0.2.1", "[::1]", "*.example.test", "bad_host.example"):
        env_file.write_text(original_env.replace(host, invalid_host), encoding="utf-8")
        invalid = subprocess.run(
            ["sh", str(runner), "config"],
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )
        assert invalid.returncode == 2
        assert "stable DNS hostname" in invalid.stderr
    env_file.write_text(original_env, encoding="utf-8")

    origin_key.chmod(0o644)
    weak = subprocess.run(
        ["sh", str(runner), "config"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert weak.returncode == 2
    assert "owner-only" in weak.stderr
    origin_key.chmod(0o600)

    wrong_cert, _wrong_key = certificate(tmp_path, "wrong", "other.example.test")
    origin_cert.write_bytes(wrong_cert.read_bytes())
    wrong_san = subprocess.run(
        ["sh", str(runner), "config"],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert wrong_san.returncode == 2
    assert "Origin certificate SAN does not match" in wrong_san.stderr


def test_deployment_secret_directories_are_ignored() -> None:
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")

    assert "deployment/local-secure/secrets/*" in gitignore
    assert "deployment/named-tunnel/secrets/*" in gitignore
    assert "deployment/production/secrets/*" in gitignore
