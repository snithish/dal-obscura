# Auth Compose Examples Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build one fully runnable Docker Compose example for each supported authentication mechanism, including a separate SPIFFE/SPIRE mTLS example, shared helpers, real services, real clients, and per-example documentation.

**Architecture:** Add `examples/auth/` with shared Docker and Python helper scripts under `_shared/`, then seven independent example directories with their own `compose.yaml` and `README.md`. Each example creates a real Iceberg fixture, starts the real `dal-obscura` server, authenticates with the selected provider, and validates a real Arrow Flight read. Per user direction, do not add automated tests for these examples; verification is by running the Compose flows.

**Tech Stack:** Docker Compose, Python 3.12, `uv`, PyArrow Flight, PyIceberg SQL catalog, SQLite, Keycloak, SPIRE, SPIFFE X.509-SVIDs, PyJWT, `cryptography`, YAML.

---

## Scope Check

This plan intentionally covers the full examples suite in one pass because all examples share the same Docker image, fixture builder, wait helper, and Flight client. The SPIFFE/SPIRE example is a separate directory because it has extra infrastructure services and operational caveats beyond local-PKI mTLS.

## File Structure

- Create: `examples/auth/README.md`
- Create: `examples/auth/_shared/Dockerfile`
- Create: `examples/auth/_shared/keycloak/realm.json`
- Create: `examples/auth/_shared/scripts/build_fixture.py`
- Create: `examples/auth/_shared/scripts/fetch_spiffe_svid.sh`
- Create: `examples/auth/_shared/scripts/flight_client.py`
- Create: `examples/auth/_shared/scripts/generate_certs.py`
- Create: `examples/auth/_shared/scripts/register_spiffe_entries.sh`
- Create: `examples/auth/_shared/scripts/trusted_gateway.py`
- Create: `examples/auth/_shared/scripts/wait_for_flight.py`
- Create: `examples/auth/_shared/spire/agent.conf`
- Create: `examples/auth/_shared/spire/server.conf`
- Create: `examples/auth/shared-jwt/compose.yaml`
- Create: `examples/auth/shared-jwt/README.md`
- Create: `examples/auth/keycloak-oidc/compose.yaml`
- Create: `examples/auth/keycloak-oidc/README.md`
- Create: `examples/auth/api-key/compose.yaml`
- Create: `examples/auth/api-key/README.md`
- Create: `examples/auth/mtls/compose.yaml`
- Create: `examples/auth/mtls/README.md`
- Create: `examples/auth/mtls-spiffe/compose.yaml`
- Create: `examples/auth/mtls-spiffe/README.md`
- Create: `examples/auth/trusted-headers/compose.yaml`
- Create: `examples/auth/trusted-headers/README.md`
- Create: `examples/auth/composite-provider/compose.yaml`
- Create: `examples/auth/composite-provider/README.md`
- Modify: `README.md`

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
SPIFFE trust domain: example.org
SPIFFE server ID: spiffe://example.org/ns/default/sa/dal-obscura-server
SPIFFE client ID: spiffe://example.org/ns/default/sa/dal-obscura-client
SPIRE agent socket: /run/spire/agent/public/api.sock
```

## Task 1: Shared Docker Image And Directory Scaffold

**Files:**
- Create: `examples/auth/_shared/Dockerfile`
- Create: `examples/auth/README.md`
- Create: seven example directories listed in File Structure

- [ ] **Step 1: Create directories**

Run:

```bash
mkdir -p examples/auth/_shared/scripts
mkdir -p examples/auth/_shared/keycloak
mkdir -p examples/auth/_shared/spire
mkdir -p examples/auth/shared-jwt
mkdir -p examples/auth/keycloak-oidc
mkdir -p examples/auth/api-key
mkdir -p examples/auth/mtls
mkdir -p examples/auth/mtls-spiffe
mkdir -p examples/auth/trusted-headers
mkdir -p examples/auth/composite-provider
```

Expected: all directories exist.

- [ ] **Step 2: Create shared Dockerfile**

Create `examples/auth/_shared/Dockerfile`:

```dockerfile
FROM ghcr.io/spiffe/spire-agent:1.14.5 AS spire-agent

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /workspace

ENV PYTHONUNBUFFERED=1
ENV UV_LINK_MODE=copy
ENV UV_NO_SYNC=1

RUN groupadd --gid 1001 spiffe-server \
    && useradd --uid 1001 --gid 1001 --home-dir /home/spiffe-server --create-home spiffe-server \
    && groupadd --gid 1002 spiffe-client \
    && useradd --uid 1002 --gid 1002 --home-dir /home/spiffe-client --create-home spiffe-client

COPY --from=spire-agent /opt/spire/bin/spire-agent /usr/local/bin/spire-agent
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY examples/auth/_shared ./examples/auth/_shared

RUN uv sync --extra server
```

The SPIRE agent binary is copied into the application image so `dal-obscura` and the
client containers can fetch their own X.509-SVIDs from the Workload API.

- [ ] **Step 3: Create top-level examples README**

Create `examples/auth/README.md`:

```markdown
# Authentication Examples

These examples run `dal-obscura` with real authentication mechanisms inside Docker
Compose. Each directory has its own `compose.yaml` and can be run independently.

## Examples

- `shared-jwt`: shared-secret HS256 bearer JWT.
- `keycloak-oidc`: real Keycloak OIDC issuer and JWKS validation.
- `api-key`: static service API key.
- `mtls`: local certificate-authority mTLS.
- `mtls-spiffe`: real SPIRE server and agent issuing SPIFFE X.509-SVIDs.
- `trusted-headers`: real Flight gateway injecting trusted identity headers.
- `composite-provider`: one server accepting API key and shared JWT credentials.

## Run Pattern

Run from an example directory:

```bash
docker compose up --build --abort-on-container-exit --exit-code-from client
```

Clean up from that directory:

```bash
docker compose down --volumes
```
```

- [ ] **Step 4: Commit scaffold**

Run:

```bash
git add examples/auth
git commit -m "docs(examples): scaffold auth compose examples"
```

Expected: commit succeeds.

## Task 2: Shared Fixture Builder

**Files:**
- Create: `examples/auth/_shared/scripts/build_fixture.py`

- [ ] **Step 1: Create fixture builder**

Create `examples/auth/_shared/scripts/build_fixture.py`:

```python
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

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

DEFAULT_JWT = "dal_obscura.data_plane.infrastructure.adapters.identity_default.DefaultIdentityAdapter"
OIDC = "dal_obscura.data_plane.infrastructure.adapters.identity_oidc_jwks.OidcJwksIdentityProvider"
API_KEY = "dal_obscura.data_plane.infrastructure.adapters.identity_api_key.ApiKeyIdentityProvider"
MTLS = "dal_obscura.data_plane.infrastructure.adapters.identity_mtls.MtlsIdentityProvider"
TRUSTED = "dal_obscura.data_plane.infrastructure.adapters.identity_trusted_headers.TrustedHeaderIdentityProvider"

SPIFFE_CLIENT_ID = "spiffe://example.org/ns/default/sa/dal-obscura-client"


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
                "email": ["alice@example.com", "bob@example.com", "carol@example.com"],
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


def _write_catalog_config() -> None:
    _write_yaml(
        RUNTIME_DIR / "catalogs.yaml",
        {
            "catalogs": {
                CATALOG: {
                    "module": "dal_obscura.data_plane.infrastructure.adapters.catalog_registry.IcebergCatalog",
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


def _write_app_config(mode: str) -> None:
    app: dict[str, Any] = {
        "location": f"grpc://0.0.0.0:{PORT}",
        "catalog_file": str(RUNTIME_DIR / "catalogs.yaml"),
        "policy_file": str(RUNTIME_DIR / "policies.yaml"),
        "secret_provider": {
            "module": "dal_obscura.data_plane.infrastructure.adapters.secret_providers.EnvSecretProvider",
            "args": {},
        },
        "ticket": {
            "ttl_seconds": 900,
            "max_tickets": 64,
            "secret": {"key": "DAL_OBSCURA_TICKET_SECRET"},
        },
        "auth": _auth_config(mode),
        "logging": {"level": "INFO", "json": True},
    }
    if mode in {"mtls", "mtls-spiffe"}:
        app["location"] = f"grpc+tls://0.0.0.0:{PORT}"
        app["transport"] = {
            "tls": {
                "cert": {"key": "SERVER_CERT"},
                "key": {"key": "SERVER_KEY"},
                "client_ca": {"key": "CLIENT_CA"},
                "verify_client": True,
            }
        }
    _write_yaml(RUNTIME_DIR / "app.yaml", app)


def _auth_config(mode: str) -> dict[str, Any]:
    if mode == "shared-jwt":
        return {"module": DEFAULT_JWT, "args": {"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}}}
    if mode == "keycloak-oidc":
        return {
            "module": OIDC,
            "args": {
                "issuer": "http://keycloak:8080/realms/dal-obscura",
                "audience": "dal-obscura",
                "subject_claim": "preferred_username",
            },
        }
    if mode == "api-key":
        return {
            "module": API_KEY,
            "args": {
                "keys": [
                    {
                        "id": PRINCIPAL,
                        "secret": {"key": "DAL_OBSCURA_API_KEY"},
                        "groups": ["compose-example"],
                    }
                ]
            },
        }
    if mode == "mtls":
        return {
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
    if mode == "mtls-spiffe":
        return {
            "module": MTLS,
            "args": {
                "identities": [
                    {
                        "peer_identity": SPIFFE_CLIENT_ID,
                        "id": PRINCIPAL,
                        "groups": ["compose-example", "spiffe"],
                        "attributes": {"spiffe_id": SPIFFE_CLIENT_ID},
                    }
                ]
            },
        }
    if mode == "trusted-headers":
        return {"module": TRUSTED, "args": {"shared_secret": {"key": "DAL_OBSCURA_PROXY_SECRET"}}}
    if mode == "composite-provider":
        return {
            "providers": [
                {
                    "module": API_KEY,
                    "args": {
                        "keys": [
                            {
                                "id": PRINCIPAL,
                                "secret": {"key": "DAL_OBSCURA_API_KEY"},
                                "groups": ["compose-example"],
                            }
                        ]
                    },
                },
                {"module": DEFAULT_JWT, "args": {"jwt_secret": {"key": "DAL_OBSCURA_JWT_SECRET"}}},
            ]
        }
    raise SystemExit(f"Unsupported EXAMPLE_AUTH_MODE: {mode}")


def _write_yaml(path: Path, value: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(value, sort_keys=False))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run fixture builder once**

Run:

```bash
tmpdir="$(mktemp -d)"
EXAMPLE_AUTH_MODE=shared-jwt \
RUNTIME_DIR="$tmpdir" \
DAL_OBSCURA_TICKET_SECRET=ticket-secret-32-characters-long \
DAL_OBSCURA_JWT_SECRET=jwt-secret-32-characters-long \
uv run python examples/auth/_shared/scripts/build_fixture.py
find "$tmpdir" -maxdepth 1 -type f | sort
rm -rf "$tmpdir"
```

Expected: output includes `app.yaml`, `catalogs.yaml`, `policies.yaml`, and
`example_catalog.db`.

- [ ] **Step 3: Commit fixture builder**

Run:

```bash
git add examples/auth/_shared/scripts/build_fixture.py
git commit -m "feat(examples): add auth fixture builder"
```

Expected: commit succeeds.

## Task 3: Shared Client, Wait Helper, And Local Certificates

**Files:**
- Create: `examples/auth/_shared/scripts/flight_client.py`
- Create: `examples/auth/_shared/scripts/wait_for_flight.py`
- Create: `examples/auth/_shared/scripts/generate_certs.py`

- [ ] **Step 1: Create wait helper**

Create `examples/auth/_shared/scripts/wait_for_flight.py`:

```python
from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pyarrow.flight as flight


RUNTIME_DIR = Path(os.environ.get("RUNTIME_DIR", "/workspace/runtime"))


def main() -> None:
    uri = os.environ["FLIGHT_URI"]
    mode = os.environ["EXAMPLE_AUTH_MODE"]
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps({"catalog": "example_catalog", "target": "default.users", "columns": ["id"]})
    )
    deadline = time.monotonic() + 90
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client = _client(uri, mode)
            client.get_schema(descriptor, options=_options(mode))
            return
        except flight.FlightUnauthorizedError:
            return
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    raise SystemExit(f"Flight server was not ready: {last_error}")


def _client(uri: str, mode: str) -> flight.FlightClient:
    if mode == "mtls":
        return flight.connect(
            uri,
            tls_root_certs=(RUNTIME_DIR / "certs" / "ca.crt").read_bytes(),
            cert_chain=(RUNTIME_DIR / "certs" / "client.crt").read_text(),
            private_key=(RUNTIME_DIR / "certs" / "client.key").read_text(),
            override_hostname="dal-obscura",
        )
    if mode == "mtls-spiffe":
        cert_dir = Path(os.environ.get("SPIFFE_SVID_DIR", "/tmp/spiffe-svid"))
        return flight.connect(
            uri,
            tls_root_certs=(cert_dir / "bundle.0.pem").read_bytes(),
            cert_chain=(cert_dir / "svid.0.pem").read_text(),
            private_key=(cert_dir / "svid.0.key").read_text(),
            override_hostname="dal-obscura",
        )
    return flight.connect(uri)


def _options(mode: str) -> flight.FlightCallOptions:
    if mode in {"mtls", "mtls-spiffe"}:
        return flight.FlightCallOptions()
    return flight.FlightCallOptions(headers=[])


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Create Flight client**

Create `examples/auth/_shared/scripts/flight_client.py`:

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
    if mode == "composite-provider":
        _run_read(uri, mode, credential="api-key")
        _run_read(uri, mode, credential="jwt")
        return
    _run_read(uri, mode, credential=None)


def _run_read(uri: str, mode: str, credential: str | None) -> None:
    client = _client(uri, mode)
    options = _call_options(mode, credential)
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps({"catalog": CATALOG, "target": TARGET, "columns": ["id", "email", "region"]})
    )
    info = client.get_flight_info(descriptor, options=options)
    table = client.do_get(info.endpoints[0].ticket, options=options).read_all()
    if table.num_rows == 0:
        raise SystemExit("Expected at least one authorized row")
    if set(table.column_names) != {"id", "email", "region"}:
        raise SystemExit(f"Unexpected columns: {table.column_names}")
    suffix = f" with {credential}" if credential else ""
    print(f"{mode}{suffix}: authenticated as {PRINCIPAL} and read {table.num_rows} rows")


def _client(uri: str, mode: str) -> flight.FlightClient:
    if mode == "mtls":
        return flight.connect(
            uri,
            tls_root_certs=(RUNTIME_DIR / "certs" / "ca.crt").read_bytes(),
            cert_chain=(RUNTIME_DIR / "certs" / "client.crt").read_text(),
            private_key=(RUNTIME_DIR / "certs" / "client.key").read_text(),
            override_hostname="dal-obscura",
        )
    if mode == "mtls-spiffe":
        cert_dir = Path(os.environ.get("SPIFFE_SVID_DIR", "/tmp/spiffe-svid"))
        return flight.connect(
            uri,
            tls_root_certs=(cert_dir / "bundle.0.pem").read_bytes(),
            cert_chain=(cert_dir / "svid.0.pem").read_text(),
            private_key=(cert_dir / "svid.0.key").read_text(),
            override_hostname="dal-obscura",
        )
    return flight.connect(uri)


def _call_options(mode: str, credential: str | None) -> flight.FlightCallOptions:
    headers: list[tuple[bytes, bytes]] = []
    if mode == "shared-jwt":
        headers.append((b"authorization", f"Bearer {_shared_jwt()}".encode()))
    elif mode == "keycloak-oidc":
        headers.append((b"authorization", f"Bearer {_keycloak_token()}".encode()))
    elif mode == "api-key":
        headers.append((b"x-api-key", os.environ["DAL_OBSCURA_API_KEY"].encode()))
    elif mode == "composite-provider":
        if credential == "api-key":
            headers.append((b"x-api-key", os.environ["DAL_OBSCURA_API_KEY"].encode()))
        elif credential == "jwt":
            headers.append((b"authorization", f"Bearer {_shared_jwt()}".encode()))
        else:
            raise SystemExit("Composite example requires api-key or jwt credential")
    return flight.FlightCallOptions(headers=headers)


def _shared_jwt() -> str:
    return jwt.encode({"sub": PRINCIPAL}, os.environ["DAL_OBSCURA_JWT_SECRET"], algorithm="HS256")


def _keycloak_token() -> str:
    body = urlencode(
        {
            "grant_type": "password",
            "client_id": "dal-obscura-client",
            "client_secret": "dal-obscura-client-secret",
            "username": "example-user",
            "password": "example-password",
        }
    ).encode()
    url = "http://keycloak:8080/realms/dal-obscura/protocol/openid-connect/token"
    last_error: Exception | None = None
    for _ in range(60):
        try:
            request = Request(
                url,
                data=body,
                headers={"content-type": "application/x-www-form-urlencoded"},
                method="POST",
            )
            with urlopen(request, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
            return str(payload["access_token"])
        except Exception as exc:
            last_error = exc
            import time

            time.sleep(1)
    raise SystemExit(f"Unable to obtain Keycloak token: {last_error}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Create local certificate generator**

Create `examples/auth/_shared/scripts/generate_certs.py`:

```python
from __future__ import annotations

import datetime as dt
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


RUNTIME_DIR = Path("/workspace/runtime")
CERT_DIR = RUNTIME_DIR / "certs"


def main() -> None:
    CERT_DIR.mkdir(parents=True, exist_ok=True)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "dal-obscura-example-ca")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1))
        .not_valid_after(dt.datetime.now(dt.UTC) + dt.timedelta(days=2))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    _write_key(CERT_DIR / "ca.key", ca_key)
    _write_cert(CERT_DIR / "ca.crt", ca_cert)
    _issue_leaf("dal-obscura", CERT_DIR / "server.key", CERT_DIR / "server.crt", ca_key, ca_cert)
    _issue_leaf("example-client", CERT_DIR / "client.key", CERT_DIR / "client.crt", ca_key, ca_cert)


def _issue_leaf(
    common_name: str,
    key_path: Path,
    cert_path: Path,
    ca_key: rsa.RSAPrivateKey,
    ca_cert: x509.Certificate,
) -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(ca_cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(dt.datetime.now(dt.UTC) - dt.timedelta(minutes=1))
        .not_valid_after(dt.datetime.now(dt.UTC) + dt.timedelta(days=2))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName(common_name)]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    _write_key(key_path, key)
    _write_cert(cert_path, cert)


def _write_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _write_cert(path: Path, cert: x509.Certificate) -> None:
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Make shell helpers executable after they exist**

Run this later after Task 5 creates shell helpers:

```bash
chmod +x examples/auth/_shared/scripts/fetch_spiffe_svid.sh
chmod +x examples/auth/_shared/scripts/register_spiffe_entries.sh
```

Expected: shell helper scripts are executable.

- [ ] **Step 5: Commit client helpers**

Run:

```bash
git add examples/auth/_shared/scripts/flight_client.py examples/auth/_shared/scripts/wait_for_flight.py examples/auth/_shared/scripts/generate_certs.py
git commit -m "feat(examples): add shared auth client helpers"
```

Expected: commit succeeds.

## Task 4: JWT, API Key, Composite, And Local mTLS Compose Examples

**Files:**
- Create: `examples/auth/shared-jwt/compose.yaml`
- Create: `examples/auth/api-key/compose.yaml`
- Create: `examples/auth/composite-provider/compose.yaml`
- Create: `examples/auth/mtls/compose.yaml`

- [ ] **Step 1: Create shared JWT compose**

Create `examples/auth/shared-jwt/compose.yaml`:

```yaml
name: dal-obscura-auth-shared-jwt
services:
  setup:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    environment:
      EXAMPLE_AUTH_MODE: shared-jwt
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
      DAL_OBSCURA_JWT_SECRET: jwt-secret-32-characters-long
    volumes:
      - runtime:/workspace/runtime
    command: ["/workspace/.venv/bin/python", "examples/auth/_shared/scripts/build_fixture.py"]
  dal-obscura:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      setup:
        condition: service_completed_successfully
    environment:
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
      DAL_OBSCURA_JWT_SECRET: jwt-secret-32-characters-long
    volumes:
      - runtime:/workspace/runtime
    command: ["/workspace/.venv/bin/dal-obscura", "--app-config", "/workspace/runtime/app.yaml"]
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
      DAL_OBSCURA_JWT_SECRET: jwt-secret-32-characters-long
    volumes:
      - runtime:/workspace/runtime
    command:
      - sh
      - -c
      - |
        /workspace/.venv/bin/python examples/auth/_shared/scripts/wait_for_flight.py
        /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py
volumes:
  runtime:
```

- [ ] **Step 2: Create API key compose**

Create `examples/auth/api-key/compose.yaml`:

```yaml
name: dal-obscura-auth-api-key
services:
  setup:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    environment:
      EXAMPLE_AUTH_MODE: api-key
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
      DAL_OBSCURA_API_KEY: example-api-key
    volumes:
      - runtime:/workspace/runtime
    command: ["/workspace/.venv/bin/python", "examples/auth/_shared/scripts/build_fixture.py"]
  dal-obscura:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      setup:
        condition: service_completed_successfully
    environment:
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
      DAL_OBSCURA_API_KEY: example-api-key
    volumes:
      - runtime:/workspace/runtime
    command: ["/workspace/.venv/bin/dal-obscura", "--app-config", "/workspace/runtime/app.yaml"]
  client:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      dal-obscura:
        condition: service_started
    environment:
      EXAMPLE_AUTH_MODE: api-key
      FLIGHT_URI: grpc+tcp://dal-obscura:8815
      DAL_OBSCURA_API_KEY: example-api-key
    volumes:
      - runtime:/workspace/runtime
    command:
      - sh
      - -c
      - |
        /workspace/.venv/bin/python examples/auth/_shared/scripts/wait_for_flight.py
        /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py
volumes:
  runtime:
```

- [ ] **Step 3: Create composite provider compose**

Create `examples/auth/composite-provider/compose.yaml`:

```yaml
name: dal-obscura-auth-composite-provider
services:
  setup:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    environment:
      EXAMPLE_AUTH_MODE: composite-provider
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
      DAL_OBSCURA_JWT_SECRET: jwt-secret-32-characters-long
      DAL_OBSCURA_API_KEY: example-api-key
    volumes:
      - runtime:/workspace/runtime
    command: ["/workspace/.venv/bin/python", "examples/auth/_shared/scripts/build_fixture.py"]
  dal-obscura:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      setup:
        condition: service_completed_successfully
    environment:
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
      DAL_OBSCURA_JWT_SECRET: jwt-secret-32-characters-long
      DAL_OBSCURA_API_KEY: example-api-key
    volumes:
      - runtime:/workspace/runtime
    command: ["/workspace/.venv/bin/dal-obscura", "--app-config", "/workspace/runtime/app.yaml"]
  client:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      dal-obscura:
        condition: service_started
    environment:
      EXAMPLE_AUTH_MODE: composite-provider
      FLIGHT_URI: grpc+tcp://dal-obscura:8815
      DAL_OBSCURA_JWT_SECRET: jwt-secret-32-characters-long
      DAL_OBSCURA_API_KEY: example-api-key
    volumes:
      - runtime:/workspace/runtime
    command:
      - sh
      - -c
      - |
        /workspace/.venv/bin/python examples/auth/_shared/scripts/wait_for_flight.py
        /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py
volumes:
  runtime:
```

- [ ] **Step 4: Create local-PKI mTLS compose**

Create `examples/auth/mtls/compose.yaml`:

```yaml
name: dal-obscura-auth-mtls
services:
  setup:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    environment:
      EXAMPLE_AUTH_MODE: mtls
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
    volumes:
      - runtime:/workspace/runtime
    command:
      - sh
      - -c
      - |
        /workspace/.venv/bin/python examples/auth/_shared/scripts/generate_certs.py
        /workspace/.venv/bin/python examples/auth/_shared/scripts/build_fixture.py
  dal-obscura:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      setup:
        condition: service_completed_successfully
    environment:
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
    volumes:
      - runtime:/workspace/runtime
    command:
      - sh
      - -c
      - |
        export SERVER_CERT="$$(cat /workspace/runtime/certs/server.crt)"
        export SERVER_KEY="$$(cat /workspace/runtime/certs/server.key)"
        export CLIENT_CA="$$(cat /workspace/runtime/certs/ca.crt)"
        /workspace/.venv/bin/dal-obscura --app-config /workspace/runtime/app.yaml
  client:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      dal-obscura:
        condition: service_started
    environment:
      EXAMPLE_AUTH_MODE: mtls
      FLIGHT_URI: grpc+tls://dal-obscura:8815
    volumes:
      - runtime:/workspace/runtime
    command:
      - sh
      - -c
      - |
        /workspace/.venv/bin/python examples/auth/_shared/scripts/wait_for_flight.py
        /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py
volumes:
  runtime:
```

- [ ] **Step 5: Run local examples**

Run:

```bash
docker compose -f examples/auth/shared-jwt/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/api-key/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/composite-provider/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/mtls/compose.yaml up --build --abort-on-container-exit --exit-code-from client
```

Expected: every command exits `0` and prints authenticated read success.

- [ ] **Step 6: Clean up local examples**

Run:

```bash
docker compose -f examples/auth/shared-jwt/compose.yaml down --volumes
docker compose -f examples/auth/api-key/compose.yaml down --volumes
docker compose -f examples/auth/composite-provider/compose.yaml down --volumes
docker compose -f examples/auth/mtls/compose.yaml down --volumes
```

Expected: containers and volumes are removed.

- [ ] **Step 7: Commit local examples**

Run:

```bash
git add examples/auth/shared-jwt/compose.yaml examples/auth/api-key/compose.yaml examples/auth/composite-provider/compose.yaml examples/auth/mtls/compose.yaml
git commit -m "feat(examples): add local auth compose flows"
```

Expected: commit succeeds.

## Task 5: Real Keycloak OIDC Example

**Files:**
- Create: `examples/auth/_shared/keycloak/realm.json`
- Create: `examples/auth/keycloak-oidc/compose.yaml`

- [ ] **Step 1: Create Keycloak realm import**

Create `examples/auth/_shared/keycloak/realm.json`:

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

- [ ] **Step 2: Create Keycloak compose**

Create `examples/auth/keycloak-oidc/compose.yaml`:

```yaml
name: dal-obscura-auth-keycloak-oidc
services:
  keycloak:
    image: quay.io/keycloak/keycloak:26.0
    environment:
      KC_BOOTSTRAP_ADMIN_USERNAME: admin
      KC_BOOTSTRAP_ADMIN_PASSWORD: admin
    command: ["start-dev", "--import-realm", "--http-port=8080", "--hostname-strict=false"]
    volumes:
      - ../_shared/keycloak/realm.json:/opt/keycloak/data/import/realm.json:ro
  setup:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    environment:
      EXAMPLE_AUTH_MODE: keycloak-oidc
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
    volumes:
      - runtime:/workspace/runtime
    command: ["/workspace/.venv/bin/python", "examples/auth/_shared/scripts/build_fixture.py"]
  dal-obscura:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      setup:
        condition: service_completed_successfully
      keycloak:
        condition: service_started
    environment:
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
    volumes:
      - runtime:/workspace/runtime
    command: ["/workspace/.venv/bin/dal-obscura", "--app-config", "/workspace/runtime/app.yaml"]
  client:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      dal-obscura:
        condition: service_started
      keycloak:
        condition: service_started
    environment:
      EXAMPLE_AUTH_MODE: keycloak-oidc
      FLIGHT_URI: grpc+tcp://dal-obscura:8815
    volumes:
      - runtime:/workspace/runtime
    command:
      - sh
      - -c
      - |
        /workspace/.venv/bin/python examples/auth/_shared/scripts/wait_for_flight.py
        /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py
volumes:
  runtime:
```

- [ ] **Step 3: Run Keycloak example**

Run:

```bash
docker compose -f examples/auth/keycloak-oidc/compose.yaml up --build --abort-on-container-exit --exit-code-from client
```

Expected: client obtains a real Keycloak token and reads rows successfully.

- [ ] **Step 4: Clean up Keycloak example**

Run:

```bash
docker compose -f examples/auth/keycloak-oidc/compose.yaml down --volumes
```

Expected: containers and volumes are removed.

- [ ] **Step 5: Commit Keycloak example**

Run:

```bash
git add examples/auth/_shared/keycloak/realm.json examples/auth/keycloak-oidc/compose.yaml
git commit -m "feat(examples): add keycloak oidc compose flow"
```

Expected: commit succeeds.

## Task 6: Real SPIFFE/SPIRE mTLS Example

**Files:**
- Create: `examples/auth/_shared/spire/server.conf`
- Create: `examples/auth/_shared/spire/agent.conf`
- Create: `examples/auth/_shared/scripts/register_spiffe_entries.sh`
- Create: `examples/auth/_shared/scripts/fetch_spiffe_svid.sh`
- Create: `examples/auth/mtls-spiffe/compose.yaml`

- [ ] **Step 1: Create SPIRE server config**

Create `examples/auth/_shared/spire/server.conf`:

```hcl
server {
    bind_address = "0.0.0.0"
    bind_port = "8081"
    socket_path = "/run/spire/server/private/api.sock"
    trust_domain = "example.org"
    data_dir = "/run/spire/server/data"
    log_level = "INFO"
}

plugins {
    DataStore "sql" {
        plugin_data {
            database_type = "sqlite3"
            connection_string = "/run/spire/server/data/datastore.sqlite3"
        }
    }

    NodeAttestor "join_token" {
        plugin_data {
        }
    }

    KeyManager "memory" {
        plugin_data = {}
    }
}
```

- [ ] **Step 2: Create SPIRE agent config**

Create `examples/auth/_shared/spire/agent.conf`:

```hcl
agent {
    data_dir = "/run/spire/agent/data"
    log_level = "INFO"
    server_address = "spire-server"
    server_port = "8081"
    socket_path = "/run/spire/agent/public/api.sock"
    trust_domain = "example.org"
    insecure_bootstrap = true
}

plugins {
    NodeAttestor "join_token" {
        plugin_data {
        }
    }

    KeyManager "disk" {
        plugin_data {
            directory = "/run/spire/agent/data"
        }
    }

    WorkloadAttestor "unix" {
        plugin_data {
        }
    }
}
```

`insecure_bootstrap = true` is acceptable only because this is a local Compose example.
The README must call this out as a development-only shortcut.

- [ ] **Step 3: Create SPIFFE registration helper**

Create `examples/auth/_shared/scripts/register_spiffe_entries.sh`:

```sh
#!/bin/sh
set -eu

SERVER_SOCKET="/run/spire/server/private/api.sock"
HOST_ID="spiffe://example.org/host"
SERVER_ID="spiffe://example.org/ns/default/sa/dal-obscura-server"
CLIENT_ID="spiffe://example.org/ns/default/sa/dal-obscura-client"
JOIN_TOKEN_FILE="/run/spire/shared/join-token"

mkdir -p "$(dirname "$JOIN_TOKEN_FILE")"

i=0
while [ "$i" -lt 60 ]; do
    if spire-server healthcheck -socketPath "$SERVER_SOCKET"; then
        break
    fi
    i=$((i + 1))
    sleep 1
done

spire-server token generate \
    -socketPath "$SERVER_SOCKET" \
    -spiffeID "$HOST_ID" \
    -ttl 3600 \
    | awk '/Token:/ {print $2}' > "$JOIN_TOKEN_FILE"

spire-server entry create \
    -socketPath "$SERVER_SOCKET" \
    -parentID "$HOST_ID" \
    -spiffeID "$SERVER_ID" \
    -selector unix:uid:1001 \
    -dns dal-obscura

spire-server entry create \
    -socketPath "$SERVER_SOCKET" \
    -parentID "$HOST_ID" \
    -spiffeID "$CLIENT_ID" \
    -selector unix:uid:1002

cat "$JOIN_TOKEN_FILE"
```

- [ ] **Step 4: Create SVID fetch helper**

Create `examples/auth/_shared/scripts/fetch_spiffe_svid.sh`:

```sh
#!/bin/sh
set -eu

SOCKET="${SPIFFE_ENDPOINT_SOCKET:-/run/spire/agent/public/api.sock}"
SOCKET="${SOCKET#unix://}"
OUT_DIR="${SPIFFE_SVID_DIR:-/tmp/spiffe-svid}"

mkdir -p "$OUT_DIR"
rm -f "$OUT_DIR"/*

i=0
while [ "$i" -lt 60 ]; do
    if spire-agent api fetch x509 -socketPath "$SOCKET" -write "$OUT_DIR"; then
        test -s "$OUT_DIR/svid.0.pem"
        test -s "$OUT_DIR/svid.0.key"
        test -s "$OUT_DIR/bundle.0.pem"
        exit 0
    fi
    i=$((i + 1))
    sleep 1
done

echo "Unable to fetch SPIFFE X.509-SVID from $SOCKET" >&2
exit 1
```

- [ ] **Step 5: Mark SPIFFE helpers executable**

Run:

```bash
chmod +x examples/auth/_shared/scripts/register_spiffe_entries.sh
chmod +x examples/auth/_shared/scripts/fetch_spiffe_svid.sh
```

Expected: both scripts have executable mode.

- [ ] **Step 6: Create SPIFFE mTLS compose**

Create `examples/auth/mtls-spiffe/compose.yaml`:

```yaml
name: dal-obscura-auth-mtls-spiffe
services:
  spire-server:
    image: ghcr.io/spiffe/spire-server:1.14.5
    volumes:
      - ../_shared/spire/server.conf:/opt/spire/conf/server/server.conf:ro
      - spire-server:/run/spire/server
    command: ["-config", "/opt/spire/conf/server/server.conf"]
  spire-register:
    image: ghcr.io/spiffe/spire-server:1.14.5
    depends_on:
      spire-server:
        condition: service_started
    volumes:
      - ../_shared/scripts/register_spiffe_entries.sh:/register_spiffe_entries.sh:ro
      - spire-server:/run/spire/server
      - spire-shared:/run/spire/shared
    entrypoint: ["/bin/sh", "/register_spiffe_entries.sh"]
  spire-agent:
    image: ghcr.io/spiffe/spire-agent:1.14.5
    depends_on:
      spire-register:
        condition: service_completed_successfully
    volumes:
      - ../_shared/spire/agent.conf:/opt/spire/conf/agent/agent.conf:ro
      - spire-agent:/run/spire/agent
      - spire-shared:/run/spire/shared
    entrypoint:
      - sh
      - -c
      - |
        spire-agent run -config /opt/spire/conf/agent/agent.conf -joinToken "$$(cat /run/spire/shared/join-token)"
  setup:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    environment:
      EXAMPLE_AUTH_MODE: mtls-spiffe
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
    volumes:
      - runtime:/workspace/runtime
    command: ["/workspace/.venv/bin/python", "examples/auth/_shared/scripts/build_fixture.py"]
  dal-obscura:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    user: "1001:1001"
    depends_on:
      setup:
        condition: service_completed_successfully
      spire-agent:
        condition: service_started
    environment:
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
      SPIFFE_ENDPOINT_SOCKET: /run/spire/agent/public/api.sock
      SPIFFE_SVID_DIR: /tmp/server-svid
      UV_CACHE_DIR: /tmp/uv-cache
    volumes:
      - runtime:/workspace/runtime:ro
      - spire-agent:/run/spire/agent
    command:
      - sh
      - -c
      - |
        examples/auth/_shared/scripts/fetch_spiffe_svid.sh
        export SERVER_CERT="$$(cat /tmp/server-svid/svid.0.pem)"
        export SERVER_KEY="$$(cat /tmp/server-svid/svid.0.key)"
        export CLIENT_CA="$$(cat /tmp/server-svid/bundle.0.pem)"
        /workspace/.venv/bin/dal-obscura --app-config /workspace/runtime/app.yaml
  client:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    user: "1002:1002"
    depends_on:
      dal-obscura:
        condition: service_started
      spire-agent:
        condition: service_started
    environment:
      EXAMPLE_AUTH_MODE: mtls-spiffe
      FLIGHT_URI: grpc+tls://dal-obscura:8815
      SPIFFE_ENDPOINT_SOCKET: /run/spire/agent/public/api.sock
      SPIFFE_SVID_DIR: /tmp/client-svid
      UV_CACHE_DIR: /tmp/uv-cache
    volumes:
      - runtime:/workspace/runtime:ro
      - spire-agent:/run/spire/agent
    command:
      - sh
      - -c
      - |
        examples/auth/_shared/scripts/fetch_spiffe_svid.sh
        /workspace/.venv/bin/python examples/auth/_shared/scripts/wait_for_flight.py
        /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py
volumes:
  runtime:
  spire-server:
  spire-agent:
  spire-shared:
```

- [ ] **Step 7: Run SPIFFE mTLS example**

Run:

```bash
docker compose -f examples/auth/mtls-spiffe/compose.yaml up --build --abort-on-container-exit --exit-code-from client
```

Expected: SPIRE starts, registers the server and client entries, both workloads fetch
X.509-SVIDs, the client authenticates with the SPIFFE ID, and the read succeeds.

- [ ] **Step 8: Clean up SPIFFE mTLS example**

Run:

```bash
docker compose -f examples/auth/mtls-spiffe/compose.yaml down --volumes
```

Expected: containers and volumes are removed.

- [ ] **Step 9: Commit SPIFFE mTLS example**

Run:

```bash
git add examples/auth/_shared/spire examples/auth/_shared/scripts/register_spiffe_entries.sh examples/auth/_shared/scripts/fetch_spiffe_svid.sh examples/auth/mtls-spiffe/compose.yaml
git commit -m "feat(examples): add spiffe mtls compose flow"
```

Expected: commit succeeds.

## Task 7: Trusted Header Gateway Example

**Files:**
- Create: `examples/auth/_shared/scripts/trusted_gateway.py`
- Create: `examples/auth/trusted-headers/compose.yaml`

- [ ] **Step 1: Create trusted gateway**

Create `examples/auth/_shared/scripts/trusted_gateway.py`:

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


def main() -> None:
    server = TrustedHeaderGateway("grpc://0.0.0.0:8815", os.environ["BACKEND_FLIGHT_URI"])
    server.serve()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Create trusted headers compose**

Create `examples/auth/trusted-headers/compose.yaml`:

```yaml
name: dal-obscura-auth-trusted-headers
services:
  setup:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    environment:
      EXAMPLE_AUTH_MODE: trusted-headers
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
      DAL_OBSCURA_PROXY_SECRET: example-proxy-secret
    volumes:
      - runtime:/workspace/runtime
    command: ["/workspace/.venv/bin/python", "examples/auth/_shared/scripts/build_fixture.py"]
  dal-obscura:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      setup:
        condition: service_completed_successfully
    environment:
      DAL_OBSCURA_TICKET_SECRET: ticket-secret-32-characters-long
      DAL_OBSCURA_PROXY_SECRET: example-proxy-secret
    volumes:
      - runtime:/workspace/runtime
    command: ["/workspace/.venv/bin/dal-obscura", "--app-config", "/workspace/runtime/app.yaml"]
  gateway:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      dal-obscura:
        condition: service_started
    environment:
      BACKEND_FLIGHT_URI: grpc+tcp://dal-obscura:8815
      DAL_OBSCURA_PROXY_SECRET: example-proxy-secret
    command: ["/workspace/.venv/bin/python", "examples/auth/_shared/scripts/trusted_gateway.py"]
  client:
    build:
      context: ../../..
      dockerfile: examples/auth/_shared/Dockerfile
    depends_on:
      gateway:
        condition: service_started
    environment:
      EXAMPLE_AUTH_MODE: trusted-headers
      FLIGHT_URI: grpc+tcp://gateway:8815
    volumes:
      - runtime:/workspace/runtime
    command:
      - sh
      - -c
      - |
        /workspace/.venv/bin/python examples/auth/_shared/scripts/wait_for_flight.py
        /workspace/.venv/bin/python examples/auth/_shared/scripts/flight_client.py
volumes:
  runtime:
```

- [ ] **Step 3: Run trusted headers example**

Run:

```bash
docker compose -f examples/auth/trusted-headers/compose.yaml up --build --abort-on-container-exit --exit-code-from client
```

Expected: client reads through the gateway successfully without sending credentials to
the gateway.

- [ ] **Step 4: Clean up trusted headers example**

Run:

```bash
docker compose -f examples/auth/trusted-headers/compose.yaml down --volumes
```

Expected: containers and volumes are removed.

- [ ] **Step 5: Commit trusted headers example**

Run:

```bash
git add examples/auth/_shared/scripts/trusted_gateway.py examples/auth/trusted-headers/compose.yaml
git commit -m "feat(examples): add trusted header gateway compose flow"
```

Expected: commit succeeds.

## Task 8: Documentation For Every Example

**Files:**
- Create: `examples/auth/shared-jwt/README.md`
- Create: `examples/auth/keycloak-oidc/README.md`
- Create: `examples/auth/api-key/README.md`
- Create: `examples/auth/mtls/README.md`
- Create: `examples/auth/mtls-spiffe/README.md`
- Create: `examples/auth/trusted-headers/README.md`
- Create: `examples/auth/composite-provider/README.md`
- Modify: `README.md`

- [ ] **Step 1: Add README sections**

Each example README must include:

- H1 with the concrete example name.
- `## What This Demonstrates`
- `## Services`
- `## Run`
- `## Success Criteria`
- `## Caveats`

The run command in each README must be:

```bash
docker compose up --build --abort-on-container-exit --exit-code-from client
```

The cleanup command in each README must be:

```bash
docker compose down --volumes
```

- [ ] **Step 2: Document mechanism caveats**

Use these caveats:

- `shared-jwt`: the example uses a development HS256 secret and should be replaced by
  managed key material in production.
- `keycloak-oidc`: the example uses Keycloak dev mode, imported local users, and direct
  password grant only to keep the compose flow self-contained.
- `api-key`: API keys are static bearer credentials, so production deployments need
  rotation, scoped issuance, and secret storage.
- `mtls`: the certificate authority and leaf certificates are generated for local compose
  use only.
- `mtls-spiffe`: the example uses real SPIRE components, local join-token attestation,
  and `insecure_bootstrap = true` for developer-only startup.
- `trusted-headers`: this mode is only valid when `dal-obscura` is unreachable except
  through the trusted gateway.
- `composite-provider`: provider ordering matters; the first provider that authenticates
  the request wins.

- [ ] **Step 3: Add root README link**

Add this section to the root `README.md`:

```markdown
## Authentication Examples

Runnable Docker Compose examples for shared JWT, Keycloak/OIDC, API key, local-PKI
mTLS, SPIFFE/SPIRE mTLS, trusted headers, and composite provider authentication live in
`examples/auth/`. Each example starts a real `dal-obscura` service and validates a real
Arrow Flight read.
```

- [ ] **Step 4: Commit documentation**

Run:

```bash
git add README.md examples/auth
git commit -m "docs(examples): document auth compose examples"
```

Expected: commit succeeds.

## Task 9: Full Manual Verification

**Files:**
- Modify only files needed to fix verification failures.

- [ ] **Step 1: Run lint on example helpers**

Run:

```bash
uv run ruff check examples/auth/_shared/scripts
```

Expected: PASS.

- [ ] **Step 2: Run every compose example**

Run:

```bash
docker compose -f examples/auth/shared-jwt/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/keycloak-oidc/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/api-key/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/mtls/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/mtls-spiffe/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/trusted-headers/compose.yaml up --build --abort-on-container-exit --exit-code-from client
docker compose -f examples/auth/composite-provider/compose.yaml up --build --abort-on-container-exit --exit-code-from client
```

Expected: every command exits `0`.

- [ ] **Step 3: Clean up Compose resources**

Run:

```bash
docker compose -f examples/auth/shared-jwt/compose.yaml down --volumes
docker compose -f examples/auth/keycloak-oidc/compose.yaml down --volumes
docker compose -f examples/auth/api-key/compose.yaml down --volumes
docker compose -f examples/auth/mtls/compose.yaml down --volumes
docker compose -f examples/auth/mtls-spiffe/compose.yaml down --volumes
docker compose -f examples/auth/trusted-headers/compose.yaml down --volumes
docker compose -f examples/auth/composite-provider/compose.yaml down --volumes
```

Expected: all example containers and volumes are removed.

- [ ] **Step 4: Commit verification fixes when files changed**

Run:

```bash
git status --short
```

If verification changed files under `examples/auth/` or `README.md`, run:

```bash
git add README.md examples/auth
git commit -m "fix(examples): stabilize auth compose verification"
```

If `git status --short` prints no changed files, do not create a commit.
