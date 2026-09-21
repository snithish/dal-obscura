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
    assert "image: ${DAL_OBSCURA_EDGE_IMAGE:?set DAL_OBSCURA_EDGE_IMAGE}" in compose
    assert "condition: service_healthy" in compose
    assert "  ui:\n    # The named origin" in compose
    assert "    ports: []" in compose
    assert (
        "DAL_OBSCURA_NAMED_HOST: ${DAL_OBSCURA_NAMED_HOST:?set DAL_OBSCURA_NAMED_HOST}" in compose
    )
    assert "ports:\n      - " not in compose
    assert "named_origin_cert" in compose and "named_origin_key" in compose
    assert "DAL_OBSCURA_CLOUDFLARED_IMAGE=cloudflare/cloudflared@sha256:" in env
    assert "DAL_OBSCURA_CONTROL_PLANE_BOOTSTRAP_ENABLED=false" in env
    assert "DAL_OBSCURA_UI_TLS_CERT_SOURCE=../local-secure/secrets/ui.crt" in env
    assert "DAL_OBSCURA_UI_TLS_KEY_SOURCE=../local-secure/secrets/ui.key" in env
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
    assert "DAL_OBSCURA_CONTROL_PLANE_ADMIN_TOKEN" not in source


def test_deployment_secret_directories_are_ignored() -> None:
    gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8")

    assert "deployment/local-secure/secrets/*" in gitignore
    assert "deployment/named-tunnel/secrets/*" in gitignore
    assert "deployment/production/secrets/*" in gitignore
