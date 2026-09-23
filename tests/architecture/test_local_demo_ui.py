from pathlib import Path

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def test_keycloak_demo_builds_the_governance_ui_and_proxies_api_same_origin() -> None:
    compose = (REPOSITORY_ROOT / "examples/demo/keycloak/compose.yaml").read_text()
    dockerfile = (REPOSITORY_ROOT / "ui/Dockerfile").read_text()
    nginx = (REPOSITORY_ROOT / "ui/nginx.conf").read_text()
    resolver_script = (REPOSITORY_ROOT / "ui/resolve-upstream-dns.sh").read_text()
    package = (REPOSITORY_ROOT / "apps/governance-ui/package.json").read_text()
    dockerignore = (REPOSITORY_ROOT / ".dockerignore").read_text()
    runner = (REPOSITORY_ROOT / "examples/demo/keycloak/run").read_text()

    assert "control-plane-ui:" in compose
    assert "name: dal-obscura-keycloak-demo" in compose
    assert "127.0.0.1:20080:8080" in compose
    assert "127.0.0.1:25432:5432" in compose
    assert "127.0.0.1:28820:8820" in compose
    assert "127.0.0.1:${DAL_OBSCURA_DEMO_UI_PORT:-28821}:8080" in compose
    assert "127.0.0.1:28115:8815" in compose
    assert "dockerfile: ui/Dockerfile" in compose
    assert "pnpm run build" in dockerfile
    assert '"packageManager": "pnpm@' in package
    assert "corepack install" in dockerfile
    assert "COPY --from=build /app/dist" in dockerfile
    assert "nginxinc/nginx-unprivileged:1.27-alpine" in dockerfile
    assert "location /v1/" in nginx
    assert "resolver __OBSCURA_NGINX_RESOLVER__ valid=10s ipv6=off;" in nginx
    assert nginx.count("proxy_pass $control_plane_upstream;") == 2
    assert "ui/nginx.conf /etc/nginx/obscura/default.conf.template" in dockerfile
    assert (
        "ui/resolve-upstream-dns.sh /docker-entrypoint.d/40-resolve-upstream-dns.sh" in dockerfile
    )
    assert '"nameserver"' in resolver_script
    assert "__OBSCURA_NGINX_RESOLVER__" in resolver_script
    assert "proxy_set_header X-Forwarded-For $remote_addr;" in nginx
    assert nginx.count("proxy_set_header X-Forwarded-Host $http_host;") == 2
    assert "proxy_add_x_forwarded_for" not in nginx
    assert "Content-Security-Policy" in nginx
    assert "location /assets/" in nginx
    assert "try_files $uri =404" in nginx
    assert "expires max" in nginx
    assert "location = /index.html" in nginx
    assert "expires -1" in nginx
    assert "node_modules" in dockerignore
    assert "**/node_modules" in dockerignore
    assert "apps/governance-ui/dist" in dockerignore
    assert ".pnpm-store" in dockerignore
    assert "DAL_OBSCURA_DEMO_PROJECT_NAME:-dal-obscura-keycloak-demo" in runner
    assert 'docker compose --project-name "$project_name"' in runner
    assert "compose up -d --build --wait" in runner
