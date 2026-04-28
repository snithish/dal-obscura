from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import jwt
import pyarrow.flight as flight

RUNTIME_DIR = Path(os.environ.get("RUNTIME_DIR", "/workspace/runtime"))
DEFAULT_CATALOG = "example_catalog"
DEFAULT_TARGET = "default.users"
DEFAULT_COLUMNS = ["id", "email", "region"]
PRINCIPAL = "example-user"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run an authenticated Flight read in an auth example client container."
    )
    parser.add_argument("--catalog", default=DEFAULT_CATALOG)
    parser.add_argument("--target", default=DEFAULT_TARGET)
    parser.add_argument("--columns", default="id,email,region")
    parser.add_argument("--credential", default=None)
    parser.add_argument("--row-filter", default=None)
    parser.add_argument("--expect-rows", type=int, default=None)
    args = parser.parse_args()
    perform_read(
        uri=os.environ["FLIGHT_URI"],
        auth_flow=os.environ["AUTH_FLOW"],
        catalog=args.catalog,
        target=args.target,
        columns=[item.strip() for item in args.columns.split(",") if item.strip()],
        credential=args.credential,
        row_filter=args.row_filter,
        expect_rows=args.expect_rows,
    )


def perform_read(
    *,
    uri: str,
    auth_flow: str,
    catalog: str,
    target: str,
    columns: list[str],
    credential: str | None,
    row_filter: str | None,
    expect_rows: int | None,
) -> None:
    client = create_client(uri, auth_flow)
    options = _call_options(auth_flow, credential)
    payload: dict[str, object] = {"catalog": catalog, "target": target, "columns": columns}
    if row_filter:
        payload["row_filter"] = row_filter
    descriptor = flight.FlightDescriptor.for_command(json.dumps(payload))
    info = client.get_flight_info(descriptor, options=options)
    table = client.do_get(info.endpoints[0].ticket, options=options).read_all()
    if table.num_rows == 0:
        raise SystemExit("Expected at least one authorized row")
    if expect_rows is not None and table.num_rows != expect_rows:
        raise SystemExit(f"Expected {expect_rows} rows but read {table.num_rows}")
    if set(table.column_names) != set(columns):
        raise SystemExit(f"Unexpected columns: {table.column_names}")
    suffix = f" with {credential}" if credential else ""
    print(
        f"{auth_flow}{suffix}: authenticated as {PRINCIPAL} and read "
        f"{table.num_rows} rows from {target}"
    )


def create_client(uri: str, auth_flow: str) -> flight.FlightClient:
    if auth_flow == "mtls":
        cert_dir = Path(os.environ.get("CLIENT_CERT_DIR", RUNTIME_DIR / "certs"))
        return flight.connect(
            uri,
            tls_root_certs=(cert_dir / "ca.crt").read_bytes(),
            cert_chain=(cert_dir / "client.crt").read_text(),
            private_key=(cert_dir / "client.key").read_text(),
            override_hostname="dal-obscura",
        )
    if auth_flow == "mtls-spiffe":
        cert_dir = Path(os.environ.get("SPIFFE_SVID_DIR", "/workspace/runtime/client-svid"))
        return flight.connect(
            uri,
            tls_root_certs=(cert_dir / "bundle.0.pem").read_bytes(),
            cert_chain=(cert_dir / "svid.0.pem").read_text(),
            private_key=(cert_dir / "svid.0.key").read_text(),
            override_hostname="dal-obscura",
        )
    return flight.connect(uri)


def _call_options(auth_flow: str, credential: str | None) -> flight.FlightCallOptions:
    headers: list[tuple[bytes, bytes]] = []
    if auth_flow == "shared-jwt":
        headers.append((b"authorization", f"Bearer {_shared_jwt()}".encode()))
    elif auth_flow == "keycloak-oidc":
        headers.append((b"authorization", f"Bearer {_oidc_token()}".encode()))
    elif auth_flow == "api-key":
        headers.append((_api_key_header(), os.environ["DAL_OBSCURA_API_KEY"].encode()))
    elif auth_flow == "composite-provider":
        if credential == "api-key":
            headers.append((_api_key_header(), os.environ["DAL_OBSCURA_API_KEY"].encode()))
        elif credential == "jwt":
            headers.append((b"authorization", f"Bearer {_shared_jwt()}".encode()))
        else:
            raise SystemExit("Composite example requires api-key or jwt credential")
    return flight.FlightCallOptions(headers=headers)


def _shared_jwt() -> str:
    return jwt.encode({"sub": PRINCIPAL}, os.environ["DAL_OBSCURA_JWT_SECRET"], algorithm="HS256")


def _api_key_header() -> bytes:
    return os.environ.get("API_KEY_HEADER", "x-api-key").encode()


def _oidc_token() -> str:
    body = urlencode(
        {
            "grant_type": "password",
            "client_id": os.environ["OIDC_CLIENT_ID"],
            "client_secret": os.environ["OIDC_CLIENT_SECRET"],
            "username": os.environ["OIDC_USERNAME"],
            "password": os.environ["OIDC_PASSWORD"],
        }
    ).encode()
    url = os.environ["OIDC_TOKEN_URL"]
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
    raise SystemExit(f"Unable to obtain OIDC token: {last_error}")


if __name__ == "__main__":
    main()
