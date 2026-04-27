# Auth Compose Examples Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build one fully runnable Docker Compose example for each supported authentication mechanism, including shared helpers, real services, real clients, and per-example documentation.

**Architecture:** Add `examples/auth/` with shared Docker and Python helper scripts under `_shared/`, then six independent example directories with their own `compose.yaml` and `README.md`. Each example creates a real Iceberg fixture, starts the real `dal-obscura` server, authenticates with the selected provider, and validates a real Arrow Flight read.

**Tech Stack:** Docker Compose, Python 3.12, `uv`, PyArrow Flight, PyIceberg SQL catalog, SQLite, Keycloak, PyJWT, `cryptography`, YAML.

---

## Scope Check

This plan intentionally covers the full examples suite in one pass because all examples share the same fixture/client helpers and the same success criteria. The examples are independent at runtime, but implementing the shared helpers first avoids duplicated setup logic and keeps each compose directory small.

## File Structure

- Create: `examples/auth/README.md`
- Create: `examples/auth/_shared/Dockerfile`
- Create: `examples/auth/_shared/scripts/build_fixture.py`
- Create: `examples/auth/_shared/scripts/flight_client.py`
- Create: `examples/auth/_shared/scripts/generate_certs.py`
- Create: `examples/auth/_shared/scripts/trusted_gateway.py`
- Create: `examples/auth/_shared/scripts/wait_for_flight.py`
- Create: `examples/auth/_shared/keycloak/realm.json`
- Create: `examples/auth/shared-jwt/compose.yaml`
- Create: `examples/auth/shared-jwt/README.md`
- Create: `examples/auth/keycloak-oidc/compose.yaml`
- Create: `examples/auth/keycloak-oidc/README.md`
- Create: `examples/auth/api-key/compose.yaml`
- Create: `examples/auth/api-key/README.md`
- Create: `examples/auth/mtls/compose.yaml`
- Create: `examples/auth/mtls/README.md`
- Create: `examples/auth/trusted-headers/compose.yaml`
- Create: `examples/auth/trusted-headers/README.md`
- Create: `examples/auth/composite-provider/compose.yaml`
- Create: `examples/auth/composite-provider/README.md`
- Create: `tests/examples/test_auth_compose_examples.py`
- Modify: `.gitignore` only if generated example runtime files are accidentally unignored.

## Shared Constants

Use these values across examples so policies, clients, and configs align:

```text
Runtime directory: /workspace/runtime
Flight port: 8815
Catalog name: example_catalog
Target: default.users
Authorized principal: example-user
Ticket secret env var: DAL_OBSCURA_TICKET_SECRET
JWT secret env var: DAL_OBSCURA_JWT_SECRET
API key env var: DAL_OBSCURA_API_KEY
Trusted proxy secret env var: DAL_OBSCURA_PROXY_SECRET
```

## Task 1: Shared Docker Image And Compose Contract

**Files:**
- Create: `examples/auth/_shared/Dockerfile`
- Create: `tests/examples/test_auth_compose_examples.py`

- [ ] **Step 1: Write failing tests for example directory structure**

Create `tests/examples/test_auth_compose_examples.py` with:

```python
from __future__ import annotations

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[2]
AUTH_EXAMPLES = REPO_ROOT / "examples" / "auth"
EXAMPLE_NAMES = {
    "shared-jwt",
    "keycloak-oidc",
    "api-key",
    "mtls",
    "trusted-headers",
    "composite-provider",
}


def test_each_auth_example_has_compose_and_readme():
    for name in EXAMPLE_NAMES:
        example_dir = AUTH_EXAMPLES / name
        assert (example_dir / "compose.yaml").is_file()
        assert (example_dir / "README.md").is_file()


def test_each_compose_has_setup_server_and_client_services():
    for name in EXAMPLE_NAMES:
        compose = yaml.safe_load((AUTH_EXAMPLES / name / "compose.yaml").read_text())
        services = compose["services"]
        assert "setup" in services
        assert "dal-obscura" in services
        assert "client" in services
        assert services["client"]["depends_on"]["dal-obscura"]["condition"] == "service_started"


def test_shared_dockerfile_builds_from_repo_context():
    dockerfile = AUTH_EXAMPLES / "_shared" / "Dockerfile"
    content = dockerfile.read_text()
    assert "FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim" in content
    assert "uv sync --extra server" in content
    assert "COPY src ./src" in content
```

- [ ] **Step 2: Run structure tests and verify they fail**

Run:

```bash
uv run pytest tests/examples/test_auth_compose_examples.py -q
```

Expected: FAIL because `examples/auth/` does not exist.

- [ ] **Step 3: Create the shared Dockerfile**

Create `examples/auth/_shared/Dockerfile`:

```dockerfile
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /workspace

ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY examples/auth/_shared ./examples/auth/_shared

RUN uv sync --extra server
```

- [ ] **Step 4: Create initial example directories with minimal compose files**

Create each example directory listed in `EXAMPLE_NAMES`. For now, each `compose.yaml`
should contain the common services with a client that exits after printing the example
name. Use this exact service shape for `examples/auth/shared-jwt/compose.yaml`; repeat
with only `EXAMPLE_AUTH_MODE` changed for the other examples:

```yaml
name: dal-obscura-auth-shared-jwt
services:
  setup:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    environment:
      EXAMPLE_AUTH_MODE: shared-jwt
    volumes:
      - runtime:/workspace/runtime
    command: ["uv", "run", "python", "examples/auth/_shared/scripts/build_fixture.py"]
  dal-obscura:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      setup:
        condition: service_completed_successfully
    volumes:
      - runtime:/workspace/runtime
    command: ["uv", "run", "dal-obscura", "--app-config", "/workspace/runtime/app.yaml"]
  client:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      dal-obscura:
        condition: service_started
    environment:
      EXAMPLE_AUTH_MODE: shared-jwt
      FLIGHT_URI: grpc+tcp://dal-obscura:8815
    volumes:
      - runtime:/workspace/runtime
    command: ["uv", "run", "python", "examples/auth/_shared/scripts/flight_client.py"]
volumes:
  runtime:
```

- [ ] **Step 5: Create initial README files**

Create `examples/auth/README.md` and one `README.md` in each example directory. Each
file must include the command:

```bash
docker compose up --build --abort-on-container-exit --exit-code-from client
```

- [ ] **Step 6: Run tests and verify they pass**

Run:

```bash
uv run pytest tests/examples/test_auth_compose_examples.py -q
```

Expected: PASS.

- [ ] **Step 7: Commit**

Run:

```bash
git add examples/auth tests/examples/test_auth_compose_examples.py
git commit -m "test(examples): define auth compose example contract"
```

## Task 2: Shared Fixture Builder

**Files:**
- Create: `examples/auth/_shared/scripts/build_fixture.py`
- Modify: `tests/examples/test_auth_compose_examples.py`

- [ ] **Step 1: Add failing fixture generation tests**

Append to `tests/examples/test_auth_compose_examples.py`:

```python
import json
import os
import subprocess
import sys


def _run_fixture_builder(tmp_path: Path, mode: str) -> dict[str, object]:
    env = os.environ.copy()
    env.update(
        {
            "EXAMPLE_AUTH_MODE": mode,
            "RUNTIME_DIR": str(tmp_path),
            "DAL_OBSCURA_TICKET_SECRET": "ticket-secret-32-characters-long",
            "DAL_OBSCURA_JWT_SECRET": "jwt-secret-32-characters-long",
            "DAL_OBSCURA_API_KEY": "example-api-key",
            "DAL_OBSCURA_PROXY_SECRET": "example-proxy-secret",
        }
    )
    completed = subprocess.run(
        [sys.executable, "examples/auth/_shared/scripts/build_fixture.py"],
        cwd=REPO_ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)


def test_fixture_builder_writes_shared_jwt_app_config(tmp_path):
    metadata = _run_fixture_builder(tmp_path, "shared-jwt")
    app = yaml.safe_load((tmp_path / "app.yaml").read_text())
    policy = yaml.safe_load((tmp_path / "policies.yaml").read_text())

    assert metadata["catalog"] == "example_catalog"
    assert metadata["target"] == "default.users"
    assert app["auth"]["module"].endswith("identity_default.DefaultIdentityAdapter")
    assert app["auth"]["args"]["jwt_secret"]["key"] == "DAL_OBSCURA_JWT_SECRET"
    assert policy["catalogs"]["example_catalog"]["targets"]["default.users"]["rules"][0][
        "principals"
    ] == ["example-user"]


def test_fixture_builder_writes_api_key_app_config(tmp_path):
    _run_fixture_builder(tmp_path, "api-key")
    app = yaml.safe_load((tmp_path / "app.yaml").read_text())

    assert app["auth"]["module"].endswith("identity_api_key.ApiKeyIdentityProvider")
    assert app["auth"]["args"]["keys"][0]["id"] == "example-user"
    assert app["auth"]["args"]["keys"][0]["secret"]["key"] == "DAL_OBSCURA_API_KEY"


def test_fixture_builder_writes_composite_provider_config(tmp_path):
    _run_fixture_builder(tmp_path, "composite-provider")
    app = yaml.safe_load((tmp_path / "app.yaml").read_text())

    providers = app["auth"]["providers"]
    assert providers[0]["module"].endswith("identity_api_key.ApiKeyIdentityProvider")
    assert providers[1]["module"].endswith("identity_default.DefaultIdentityAdapter")
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
uv run pytest tests/examples/test_auth_compose_examples.py -q
```

Expected: FAIL because `build_fixture.py` is missing or not writing configs.

- [ ] **Step 3: Implement the fixture builder**

Create `examples/auth/_shared/scripts/build_fixture.py` with these responsibilities:

```python
from __future__ import annotations

import json
import os
from pathlib import Path

import pyarrow as pa
import yaml
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


RUNTIME_DIR = Path(os.environ.get("RUNTIME_DIR", "/workspace/runtime"))
CATALOG = "example_catalog"
TARGET = "default.users"
PRINCIPAL = "example-user"
PORT = 8815


def main() -> None:
    mode = os.environ["EXAMPLE_AUTH_MODE"]
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    _create_table()
    _write_catalog_config()
    _write_policy_config()
    _write_app_config(mode)
    print(json.dumps({"catalog": CATALOG, "target": TARGET, "principal": PRINCIPAL}))


def _create_table() -> None:
    warehouse = RUNTIME_DIR / "warehouse"
    warehouse.mkdir(parents=True, exist_ok=True)
    catalog = load_catalog(
        CATALOG,
        type="sql",
        uri=f"sqlite:///{RUNTIME_DIR / f'{CATALOG}.db'}",
        warehouse=str(warehouse),
    )
    try:
        catalog.create_namespace("default")
    except Exception:
        pass
    table = catalog.create_table(
        TARGET,
        schema=Schema(
            NestedField(field_id=1, name="id", field_type=LongType(), required=True),
            NestedField(field_id=2, name="email", field_type=StringType(), required=False),
            NestedField(field_id=3, name="region", field_type=StringType(), required=False),
        ),
        properties={"format-version": "2"},
    )
    table.append(
        pa.table(
            {
                "id": [1, 2, 3],
                "email": [
                    "alice@example.com",
                    "bob@example.com",
                    "carol@example.com",
                ],
                "region": ["us", "eu", "us"],
            },
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("email", pa.string()),
                    pa.field("region", pa.string()),
                ]
            ),
        )
    )
```

Continue the file with helper functions that write:

```python
def _write_catalog_config() -> None:
    _write_yaml(
        RUNTIME_DIR / "catalogs.yaml",
        {
            "catalogs": {
                CATALOG: {
                    "module": "dal_obscura.infrastructure.adapters.catalog_registry.IcebergCatalog",
                    "options": {
                        "type": "sql",
                        "uri": f"sqlite:///{RUNTIME_DIR / f'{CATALOG}.db'}",
                        "warehouse": str(RUNTIME_DIR / "warehouse"),
                    },
                }
            }
        },
    )


def _write_policy_config() -> None:
    _write_yaml(
        RUNTIME_DIR / "policies.yaml",
        {
            "version": 1,
            "catalogs": {
                CATALOG: {
                    "targets": {
                        TARGET: {
                            "rules": [
                                {
                                    "principals": [PRINCIPAL],
                                    "columns": ["id", "email", "region"],
                                    "row_filter": "region = 'us'",
                                }
                            ]
                        }
                    }
                }
            },
        },
    )
```

Implement `_auth_config(mode)` with exact branches for `shared-jwt`, `keycloak-oidc`,
`api-key`, `mtls`, `trusted-headers`, and `composite-provider`. Use these module paths:

```python
DEFAULT_JWT = "dal_obscura.infrastructure.adapters.identity_default.DefaultIdentityAdapter"
OIDC = "dal_obscura.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
API_KEY = "dal_obscura.infrastructure.adapters.identity_api_key.ApiKeyIdentityProvider"
MTLS = "dal_obscura.infrastructure.adapters.identity_mtls.MtlsIdentityProvider"
TRUSTED = "dal_obscura.infrastructure.adapters.identity_trusted_headers.TrustedHeaderIdentityProvider"
```

For `keycloak-oidc`, write:

```python
{
    "module": OIDC,
    "args": {
        "issuer": "http://keycloak:8080/realms/dal-obscura",
        "audience": "dal-obscura",
        "subject_claim": "preferred_username",
    },
}
```

For `mtls`, write `transport.tls` with secret refs `SERVER_CERT`, `SERVER_KEY`, and
`CLIENT_CA`, and write:

```python
{
    "module": MTLS,
    "args": {
        "identities": [
            {
                "peer_identity": "example-client",
                "id": PRINCIPAL,
                "groups": ["compose-example"],
            }
        ]
    },
}
```

For `trusted-headers`, write:

```python
{
    "module": TRUSTED,
    "args": {"shared_secret": {"key": "DAL_OBSCURA_PROXY_SECRET"}},
}
```

For `composite-provider`, write API key first and shared JWT second.

- [ ] **Step 4: Run fixture tests and verify they pass**

Run:

```bash
uv run pytest tests/examples/test_auth_compose_examples.py -q
```

Expected: PASS for structure and fixture config tests.

- [ ] **Step 5: Commit**

Run:

```bash
git add examples/auth/_shared/scripts/build_fixture.py tests/examples/test_auth_compose_examples.py
git commit -m "feat(examples): generate shared auth fixture configs"
```

## Task 3: Shared Flight Client And Wait Helpers

**Files:**
- Create: `examples/auth/_shared/scripts/flight_client.py`
- Create: `examples/auth/_shared/scripts/wait_for_flight.py`
- Modify: `tests/examples/test_auth_compose_examples.py`

- [ ] **Step 1: Add failing client helper tests**

Append:

```python
def test_flight_client_defines_auth_headers_for_header_modes():
    content = (AUTH_EXAMPLES / "_shared" / "scripts" / "flight_client.py").read_text()
    assert "def _call_options" in content
    assert "shared-jwt" in content
    assert "api-key" in content
    assert "keycloak-oidc" in content
    assert "composite-provider" in content


def test_wait_helper_uses_flight_get_schema_probe():
    content = (AUTH_EXAMPLES / "_shared" / "scripts" / "wait_for_flight.py").read_text()
    assert "flight.connect" in content
    assert "get_schema" in content
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
uv run pytest tests/examples/test_auth_compose_examples.py -q
```

Expected: FAIL because client scripts are missing.

- [ ] **Step 3: Implement `wait_for_flight.py`**

Create a script that reads `FLIGHT_URI`, retries for 60 seconds, and probes with
`client.get_schema(...)`. Treat unauthorized responses as readiness because auth is
tested by the real client:

```python
from __future__ import annotations

import json
import os
import time

import pyarrow.flight as flight


def main() -> None:
    uri = os.environ["FLIGHT_URI"]
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps({"catalog": "example_catalog", "target": "default.users", "columns": ["id"]})
    )
    deadline = time.monotonic() + 60
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client = flight.connect(uri)
            client.get_schema(descriptor)
            return
        except flight.FlightUnauthorizedError:
            return
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    raise SystemExit(f"Flight server was not ready: {last_error}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Implement `flight_client.py`**

Create a script that reads `EXAMPLE_AUTH_MODE`, `FLIGHT_URI`, and runtime metadata,
builds auth headers or TLS options for the selected mode, runs `get_flight_info` and
`do_get`, and validates a non-empty result:

```python
from __future__ import annotations

import json
import os
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import jwt
import pyarrow.flight as flight


RUNTIME_DIR = Path(os.environ.get("RUNTIME_DIR", "/workspace/runtime"))
CATALOG = "example_catalog"
TARGET = "default.users"
PRINCIPAL = "example-user"


def main() -> None:
    mode = os.environ["EXAMPLE_AUTH_MODE"]
    uri = os.environ["FLIGHT_URI"]
    client = _client(uri, mode)
    options = _call_options(mode)
    payload = {"catalog": CATALOG, "target": TARGET, "columns": ["id", "email", "region"]}
    descriptor = flight.FlightDescriptor.for_command(json.dumps(payload))
    info = client.get_flight_info(descriptor, options=options)
    table = client.do_get(info.endpoints[0].ticket, options=options).read_all()
    if table.num_rows == 0:
        raise SystemExit("Expected at least one authorized row")
    print(f"{mode}: authenticated as {PRINCIPAL} and read {table.num_rows} rows")
```

Implement helpers:

```python
def _client(uri: str, mode: str) -> flight.FlightClient:
    if mode != "mtls":
        return flight.connect(uri)
    return flight.connect(
        uri,
        tls_root_certs=(RUNTIME_DIR / "certs" / "ca.crt").read_bytes(),
        cert_chain=(RUNTIME_DIR / "certs" / "client.crt").read_text(),
        private_key=(RUNTIME_DIR / "certs" / "client.key").read_text(),
        override_hostname="dal-obscura",
    )


def _call_options(mode: str) -> flight.FlightCallOptions:
    headers: list[tuple[bytes, bytes]] = []
    if mode == "shared-jwt":
        headers.append((b"authorization", f"Bearer {_shared_jwt()}".encode()))
    elif mode == "keycloak-oidc":
        headers.append((b"authorization", f"Bearer {_keycloak_token()}".encode()))
    elif mode == "api-key":
        headers.append((b"x-api-key", os.environ["DAL_OBSCURA_API_KEY"].encode()))
    elif mode == "composite-provider":
        credential = os.environ.get("COMPOSITE_CREDENTIAL", "api-key")
        if credential == "api-key":
            headers.append((b"x-api-key", os.environ["DAL_OBSCURA_API_KEY"].encode()))
        else:
            headers.append((b"authorization", f"Bearer {_shared_jwt()}".encode()))
    return flight.FlightCallOptions(headers=headers)
```

Use `_shared_jwt()` to sign `{"sub": PRINCIPAL}` with `DAL_OBSCURA_JWT_SECRET`. Use
`_keycloak_token()` to POST to:

```text
http://keycloak:8080/realms/dal-obscura/protocol/openid-connect/token
```

with form fields:

```text
grant_type=password
client_id=dal-obscura-client
client_secret=dal-obscura-client-secret
username=example-user
password=example-password
```

- [ ] **Step 5: Run tests**

Run:

```bash
uv run pytest tests/examples/test_auth_compose_examples.py -q
```

Expected: PASS.

- [ ] **Step 6: Commit**

Run:

```bash
git add examples/auth/_shared/scripts/flight_client.py examples/auth/_shared/scripts/wait_for_flight.py tests/examples/test_auth_compose_examples.py
git commit -m "feat(examples): add shared flight client helpers"
```

## Task 4: Shared JWT, API Key, And Composite Compose Examples

**Files:**
- Modify: `examples/auth/shared-jwt/compose.yaml`
- Modify: `examples/auth/api-key/compose.yaml`
- Modify: `examples/auth/composite-provider/compose.yaml`

- [ ] **Step 1: Add compose assertions for environment wiring**

Extend `test_each_compose_has_setup_server_and_client_services` with assertions that:

```python
if name == "shared-jwt":
    assert services["client"]["environment"]["DAL_OBSCURA_JWT_SECRET"]
if name == "api-key":
    assert services["client"]["environment"]["DAL_OBSCURA_API_KEY"]
if name == "composite-provider":
    assert services["client"]["environment"]["COMPOSITE_CREDENTIAL"] == "api-key"
```

- [ ] **Step 2: Update the three compose files**

For `shared-jwt`, set setup, server, and client env vars:

```yaml
DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
DAL_OBSCURA_JWT_SECRET: jwt-secret-32-characters-long
```

For `api-key`, set:

```yaml
DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
DAL_OBSCURA_API_KEY: example-api-key
```

For `composite-provider`, set:

```yaml
DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
DAL_OBSCURA_JWT_SECRET: jwt-secret-32-characters-long
DAL_OBSCURA_API_KEY: example-api-key
COMPOSITE_CREDENTIAL: api-key
```

Make the client command run:

```yaml
command:
  - sh
  - -c
  - |
    uv run python examples/auth/_shared/scripts/wait_for_flight.py
    uv run python examples/auth/_shared/scripts/flight_client.py
```

- [ ] **Step 3: Run compose tests**

Run:

```bash
uv run pytest tests/examples/test_auth_compose_examples.py -q
```

Expected: PASS.

- [ ] **Step 4: Run the three compose examples**

Run:

```bash
docker compose -f examples/auth/shared-jwt/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/api-key/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/composite-provider/compose.yaml up --build --abort-on-container-exit --exit-code-from client
```

Expected: each command exits `0` and prints an authenticated read success line.

- [ ] **Step 5: Commit**

Run:

```bash
git add examples/auth/shared-jwt/compose.yaml examples/auth/api-key/compose.yaml examples/auth/composite-provider/compose.yaml tests/examples/test_auth_compose_examples.py
git commit -m "feat(examples): add jwt api key and composite compose flows"
```

## Task 5: Real Keycloak OIDC Example

**Files:**
- Create: `examples/auth/_shared/keycloak/realm.json`
- Modify: `examples/auth/keycloak-oidc/compose.yaml`
- Modify: `tests/examples/test_auth_compose_examples.py`

- [ ] **Step 1: Add failing Keycloak assertions**

Append:

```python
def test_keycloak_example_uses_real_keycloak_with_realm_import():
    compose = yaml.safe_load((AUTH_EXAMPLES / "keycloak-oidc" / "compose.yaml").read_text())
    services = compose["services"]
    assert "keycloak" in services
    assert "quay.io/keycloak/keycloak" in services["keycloak"]["image"]
    assert "start-dev" in services["keycloak"]["command"]
    assert (AUTH_EXAMPLES / "_shared" / "keycloak" / "realm.json").is_file()
```

- [ ] **Step 2: Create Keycloak realm import**

Create `examples/auth/_shared/keycloak/realm.json` with:

```json
{
  "realm": "dal-obscura",
  "enabled": true,
  "clients": [
    {
      "clientId": "dal-obscura-client",
      "enabled": true,
      "publicClient": false,
      "secret": "dal-obscura-client-secret",
      "directAccessGrantsEnabled": true,
      "protocol": "openid-connect",
      "defaultClientScopes": ["web-origins", "acr", "profile", "roles"]
    }
  ],
  "users": [
    {
      "username": "example-user",
      "enabled": true,
      "emailVerified": true,
      "credentials": [
        {
          "type": "password",
          "value": "example-password",
          "temporary": false
        }
      ]
    }
  ]
}
```

- [ ] **Step 3: Update Keycloak compose**

Add a `keycloak` service:

```yaml
keycloak:
  image: quay.io/keycloak/keycloak:26.0
  environment:
    KC_BOOTSTRAP_ADMIN_USERNAME: admin
    KC_BOOTSTRAP_ADMIN_PASSWORD: admin
  command: ["start-dev", "--import-realm", "--http-port=8080", "--hostname-strict=false"]
  volumes:
    - ../_shared/keycloak/realm.json:/opt/keycloak/data/import/realm.json:ro
```

Make `dal-obscura` depend on `keycloak` with `condition: service_started`, and keep the
client waiting by retrying token acquisition in `flight_client.py`.

- [ ] **Step 4: Run tests**

Run:

```bash
uv run pytest tests/examples/test_auth_compose_examples.py -q
```

Expected: PASS.

- [ ] **Step 5: Run Keycloak compose**

Run:

```bash
docker compose -f examples/auth/keycloak-oidc/compose.yaml up --build --abort-on-container-exit --exit-code-from client
```

Expected: client obtains a real Keycloak token and reads rows successfully.

- [ ] **Step 6: Commit**

Run:

```bash
git add examples/auth/_shared/keycloak/realm.json examples/auth/keycloak-oidc/compose.yaml tests/examples/test_auth_compose_examples.py
git commit -m "feat(examples): add keycloak oidc compose flow"
```

## Task 6: mTLS Compose Example

**Files:**
- Create: `examples/auth/_shared/scripts/generate_certs.py`
- Modify: `examples/auth/mtls/compose.yaml`
- Modify: `tests/examples/test_auth_compose_examples.py`

- [ ] **Step 1: Add failing mTLS assertions**

Append:

```python
def test_mtls_example_generates_certs_and_uses_grpc_tls():
    compose = yaml.safe_load((AUTH_EXAMPLES / "mtls" / "compose.yaml").read_text())
    assert "grpc+tls://dal-obscura:8815" in compose["services"]["client"]["environment"]["FLIGHT_URI"]
    assert (AUTH_EXAMPLES / "_shared" / "scripts" / "generate_certs.py").is_file()
```

- [ ] **Step 2: Implement certificate generator**

Create `examples/auth/_shared/scripts/generate_certs.py` using `cryptography`. It must
write:

```text
/workspace/runtime/certs/ca.crt
/workspace/runtime/certs/server.crt
/workspace/runtime/certs/server.key
/workspace/runtime/certs/client.crt
/workspace/runtime/certs/client.key
```

The server certificate must include DNS SAN `dal-obscura`. The client certificate
subject common name must be `example-client`.

- [ ] **Step 3: Update mTLS compose**

Make setup run:

```yaml
command:
  - sh
  - -c
  - |
    uv run python examples/auth/_shared/scripts/generate_certs.py
    uv run python examples/auth/_shared/scripts/build_fixture.py
```

Set the server environment:

```yaml
DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
```

Override the `dal-obscura` service command so `EnvSecretProvider` receives PEM contents,
not file paths:

```yaml
command:
  - sh
  - -c
  - |
    export SERVER_CERT="$(cat /workspace/runtime/certs/server.crt)"
    export SERVER_KEY="$(cat /workspace/runtime/certs/server.key)"
    export CLIENT_CA="$(cat /workspace/runtime/certs/ca.crt)"
    uv run dal-obscura --app-config /workspace/runtime/app.yaml
```

- [ ] **Step 4: Run tests**

Run:

```bash
uv run pytest tests/examples/test_auth_compose_examples.py -q
```

Expected: PASS.

- [ ] **Step 5: Run mTLS compose**

Run:

```bash
docker compose -f examples/auth/mtls/compose.yaml up --build --abort-on-container-exit --exit-code-from client
```

Expected: client connects with a certificate and reads rows successfully.

- [ ] **Step 6: Commit**

Run:

```bash
git add examples/auth/_shared/scripts/generate_certs.py examples/auth/mtls/compose.yaml tests/examples/test_auth_compose_examples.py
git commit -m "feat(examples): add mtls compose flow"
```

## Task 7: Trusted Header Gateway Example

**Files:**
- Create: `examples/auth/_shared/scripts/trusted_gateway.py`
- Modify: `examples/auth/trusted-headers/compose.yaml`
- Modify: `tests/examples/test_auth_compose_examples.py`

- [ ] **Step 1: Add failing trusted-header assertions**

Append:

```python
def test_trusted_headers_example_routes_through_gateway():
    compose = yaml.safe_load((AUTH_EXAMPLES / "trusted-headers" / "compose.yaml").read_text())
    services = compose["services"]
    assert "gateway" in services
    assert services["client"]["environment"]["FLIGHT_URI"] == "grpc+tcp://gateway:8815"
    assert (AUTH_EXAMPLES / "_shared" / "scripts" / "trusted_gateway.py").is_file()
```

- [ ] **Step 2: Implement `trusted_gateway.py`**

Create a PyArrow Flight server that forwards `get_schema`, `get_flight_info`, and
`do_get` to the backend with injected headers:

```python
from __future__ import annotations

import os

import pyarrow.flight as flight


class TrustedHeaderGateway(flight.FlightServerBase):
    def __init__(self, location: str, backend_uri: str) -> None:
        super().__init__(location)
        self._backend = flight.connect(backend_uri)

    def get_schema(self, context, descriptor):
        return self._backend.get_schema(descriptor, options=_trusted_options())

    def get_flight_info(self, context, descriptor):
        return self._backend.get_flight_info(descriptor, options=_trusted_options())

    def do_get(self, context, ticket):
        table = self._backend.do_get(ticket, options=_trusted_options()).read_all()
        return flight.RecordBatchStream(table)


def _trusted_options() -> flight.FlightCallOptions:
    return flight.FlightCallOptions(
        headers=[
            (b"x-dal-obscura-proxy-secret", os.environ["DAL_OBSCURA_PROXY_SECRET"].encode()),
            (b"x-auth-request-user", b"example-user"),
            (b"x-auth-request-groups", b"compose-example"),
        ]
    )
```

Start it with:

```python
server = TrustedHeaderGateway("grpc://0.0.0.0:8815", os.environ["BACKEND_FLIGHT_URI"])
server.serve()
```

- [ ] **Step 3: Update trusted headers compose**

Add `gateway` between client and backend. Backend remains private inside the Compose
network. Client uses `FLIGHT_URI=grpc+tcp://gateway:8815` and sends no auth headers.

- [ ] **Step 4: Run tests**

Run:

```bash
uv run pytest tests/examples/test_auth_compose_examples.py -q
```

Expected: PASS.

- [ ] **Step 5: Run trusted headers compose**

Run:

```bash
docker compose -f examples/auth/trusted-headers/compose.yaml up --build --abort-on-container-exit --exit-code-from client
```

Expected: client reads through the gateway successfully.

- [ ] **Step 6: Commit**

Run:

```bash
git add examples/auth/_shared/scripts/trusted_gateway.py examples/auth/trusted-headers/compose.yaml tests/examples/test_auth_compose_examples.py
git commit -m "feat(examples): add trusted header gateway compose flow"
```

## Task 8: Documentation For Every Example

**Files:**
- Modify: `examples/auth/README.md`
- Modify: `examples/auth/shared-jwt/README.md`
- Modify: `examples/auth/keycloak-oidc/README.md`
- Modify: `examples/auth/api-key/README.md`
- Modify: `examples/auth/mtls/README.md`
- Modify: `examples/auth/trusted-headers/README.md`
- Modify: `examples/auth/composite-provider/README.md`
- Modify: `README.md`

- [ ] **Step 1: Add README content tests**

Append:

```python
def test_each_example_readme_documents_run_command_and_caveats():
    for name in EXAMPLE_NAMES:
        content = (AUTH_EXAMPLES / name / "README.md").read_text()
        assert "docker compose up --build --abort-on-container-exit --exit-code-from client" in content
        assert "What This Demonstrates" in content
        assert "Caveats" in content
```

- [ ] **Step 2: Update each example README**

Each README must include these sections:

- H1 with the concrete example name, for example `# Shared JWT Authentication`
- `## What This Demonstrates`
- `## Services`
- `## Run`
- The exact run command:

```bash
docker compose up --build --abort-on-container-exit --exit-code-from client
```

- `## Success Criteria`
- `## Caveats`

Use these caveats:

- `shared-jwt`: the example uses a development HS256 secret and should be replaced by
  managed key material in production.
- `keycloak-oidc`: the example uses Keycloak dev mode, imported local users, and direct
  password grant only to keep the compose flow self-contained.
- `api-key`: API keys are static bearer credentials, so production deployments need
  rotation, scoped issuance, and secret storage.
- `mtls`: the certificate authority and leaf certificates are generated for local compose
  use only.
- `trusted-headers`: this mode is only valid when `dal-obscura` is unreachable except
  through the trusted gateway.
- `composite-provider`: provider ordering matters; the first provider that authenticates
  the request wins.

- [ ] **Step 3: Add top-level README link**

Add a short section to root `README.md`:

```markdown
## Authentication Examples

Runnable Docker Compose examples for shared JWT, Keycloak/OIDC, API key, mTLS,
trusted headers, and composite provider authentication live in `examples/auth/`.
Each example starts a real `dal-obscura` service and validates a real Arrow Flight read.
```

- [ ] **Step 4: Run documentation tests**

Run:

```bash
uv run pytest tests/examples/test_auth_compose_examples.py -q
```

Expected: PASS.

- [ ] **Step 5: Commit**

Run:

```bash
git add README.md examples/auth tests/examples/test_auth_compose_examples.py
git commit -m "docs(examples): document auth compose examples"
```

## Task 9: Full Verification

**Files:**
- Modify only files needed to fix verification failures.

- [ ] **Step 1: Run Python tests**

Run:

```bash
uv run pytest
```

Expected: PASS.

- [ ] **Step 2: Run lint**

Run:

```bash
uv run ruff check .
```

Expected: PASS.

- [ ] **Step 3: Run type check**

Run:

```bash
uv run ty check
```

Expected: PASS.

- [ ] **Step 4: Run JVM verification**

Run:

```bash
mvn -f connectors/jvm/pom.xml verify
```

Expected: PASS.

- [ ] **Step 5: Run every compose example**

Run:

```bash
docker compose -f examples/auth/shared-jwt/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/keycloak-oidc/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/api-key/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/mtls/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/trusted-headers/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/composite-provider/compose.yaml up --build --abort-on-container-exit --exit-code-from client
```

Expected: every command exits `0`.

- [ ] **Step 6: Clean up Compose resources**

Run:

```bash
docker compose -f examples/auth/shared-jwt/compose.yaml down --volumes
docker compose -f examples/auth/keycloak-oidc/compose.yaml down --volumes
docker compose -f examples/auth/api-key/compose.yaml down --volumes
docker compose -f examples/auth/mtls/compose.yaml down --volumes
docker compose -f examples/auth/trusted-headers/compose.yaml down --volumes
docker compose -f examples/auth/composite-provider/compose.yaml down --volumes
```

Expected: all example containers and volumes are removed.

- [ ] **Step 7: Commit final fixes if needed**

If verification required any changes, commit them:

Run:

```bash
git status --short
```

If verification required changes under `examples/auth/`, `tests/examples/`, or
`README.md`, add those exact files and commit them:

```bash
git add examples/auth tests/examples README.md
git commit -m "fix(examples): stabilize auth compose verification"
```

If `git status --short` prints no changed files, do not create a commit.
