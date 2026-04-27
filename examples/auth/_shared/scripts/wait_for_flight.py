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
        except (flight.FlightUnauthenticatedError, flight.FlightUnauthorizedError):
            return
        except flight.FlightError as exc:
            if _is_auth_failure(exc):
                return
            last_error = exc
            time.sleep(1)
        except Exception as exc:
            if _is_auth_failure(exc):
                return
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


def _is_auth_failure(exc: Exception) -> bool:
    message = str(exc).lower()
    return "unauthorized" in message or "unauthenticated" in message


if __name__ == "__main__":
    main()
