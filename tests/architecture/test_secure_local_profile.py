from pathlib import Path

ROOT = Path(__file__).parents[2]
PROFILE = ROOT / "deployment" / "local-secure"


def test_secure_local_profile_reuses_production_security_contract() -> None:
    compose = (PROFILE / "compose.yaml").read_text(encoding="utf-8")
    env = (PROFILE / ".env.example").read_text(encoding="utf-8")
    readme = (PROFILE / "README.md").read_text(encoding="utf-8")
    runner = (PROFILE / "run").read_text(encoding="utf-8")

    assert "../production/compose.yaml" in runner
    assert '"127.0.0.1:8815:8815"' in compose
    assert '"127.0.0.1:8443:8443"' in compose
    assert "../local-secure/Caddyfile:/etc/caddy/Caddyfile:ro" in compose
    assert "DAL_OBSCURA_CONTROL_PLANE_BOOTSTRAP_ENABLED=false" in env
    assert "DAL_OBSCURA_TLS_VERIFY_CLIENT=true" in env
    assert "DAL_OBSCURA_EDGE_IMAGE=caddy@sha256:" in env
    assert (
        "DAL_OBSCURA_CONTROL_PLANE_UI_OIDC_REDIRECT_URI=https://localhost:8443/auth/callback"
        in env
    )
    assert "openssl" in runner
    assert "REPLACE_WITH placeholders" in runner
    assert "Missing readable TLS material" in runner
    assert "same production services and security settings" in readme
