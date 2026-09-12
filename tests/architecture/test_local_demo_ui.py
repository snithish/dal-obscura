from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_keycloak_demo_builds_the_governance_ui_and_proxies_api_same_origin() -> None:
    compose = (REPOSITORY_ROOT / "examples/demo/keycloak/compose.yaml").read_text()
    dockerfile = (REPOSITORY_ROOT / "ui/Dockerfile").read_text()
    nginx = (REPOSITORY_ROOT / "ui/nginx.conf").read_text()
    package = (REPOSITORY_ROOT / "apps/governance-ui/package.json").read_text()
    dockerignore = (REPOSITORY_ROOT / ".dockerignore").read_text()

    assert "control-plane-ui:" in compose
    assert "dockerfile: ui/Dockerfile" in compose
    assert "pnpm run build" in dockerfile
    assert '"packageManager": "pnpm@' in package
    assert "corepack install" in dockerfile
    assert "COPY --from=build /app/dist" in dockerfile
    assert "nginxinc/nginx-unprivileged:1.27-alpine" in dockerfile
    assert "location /v1/" in nginx
    assert "proxy_pass http://control-plane:8820" in nginx
    assert "Content-Security-Policy" in nginx
    assert "location /assets/" in nginx
    assert "try_files $uri =404" in nginx
    assert "expires max" in nginx
    assert "location = /index.html" in nginx
    assert "expires -1" in nginx
    assert "node_modules" in dockerignore
    assert "apps/governance-ui/dist" in dockerignore
    assert ".pnpm-store" in dockerignore
