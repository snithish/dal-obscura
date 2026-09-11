from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_keycloak_demo_builds_the_governance_ui_and_proxies_api_same_origin() -> None:
    compose = (REPOSITORY_ROOT / "examples/demo/keycloak/compose.yaml").read_text()
    dockerfile = (REPOSITORY_ROOT / "ui/Dockerfile").read_text()
    nginx = (REPOSITORY_ROOT / "ui/nginx.conf").read_text()

    assert "control-plane-ui:" in compose
    assert "dockerfile: ui/Dockerfile" in compose
    assert "pnpm run build" in dockerfile
    assert "COPY --from=build /app/dist" in dockerfile
    assert "location /v1/" in nginx
    assert "proxy_pass http://control-plane:8820" in nginx
    assert "Content-Security-Policy" in nginx
