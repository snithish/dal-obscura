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
    caddyfile = (REPOSITORY_ROOT / "examples/demo/keycloak/Caddyfile").read_text()

    assert "control-plane-ui:" in compose
    assert "name: dal-obscura-keycloak-demo" in compose
    assert "127.0.0.1:${DAL_OBSCURA_DEMO_CADDY_HTTP_PORT:-80}:80" in compose
    assert "127.0.0.1:${DAL_OBSCURA_DEMO_CADDY_HTTPS_PORT:-443}:443" in compose
    assert "caddy-data:/data" in compose
    assert "keycloak.localhost" in caddyfile
    assert "governance.localhost" in caddyfile
    assert "api.localhost" in caddyfile
    assert caddyfile.count("tls internal") == 3
    assert "reverse_proxy keycloak:8080" in caddyfile
    assert "reverse_proxy control-plane-ui:8080" in caddyfile
    assert "reverse_proxy control-plane:8820" in caddyfile
    assert "127.0.0.1:8081:8080" not in compose
    assert "DAL_OBSCURA_DEMO_UI_PORT" not in compose
    assert "127.0.0.1:8820:8820" not in compose
    assert "127.0.0.1:5432:5432" not in compose
    podman_pf = (REPOSITORY_ROOT / "examples/demo/keycloak/pf-anchor.conf").read_text()
    assert "rdr pass on lo0 inet proto tcp" in podman_pf
    assert "127.0.0.1 port 80 -> 127.0.0.1 port 18080" in podman_pf
    assert "127.0.0.1 port 443 -> 127.0.0.1 port 18443" in podman_pf
    assert "podman_loopback_ports.sh" in runner
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
