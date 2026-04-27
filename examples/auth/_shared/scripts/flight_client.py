from __future__ import annotations

import json
import os
import time
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
            time.sleep(1)
    raise SystemExit(f"Unable to obtain Keycloak token: {last_error}")


if __name__ == "__main__":
    main()
