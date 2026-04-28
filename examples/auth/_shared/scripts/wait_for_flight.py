from __future__ import annotations

import json
import os
import time

import flight_client
import pyarrow.flight as flight


def main() -> None:
    uri = os.environ["FLIGHT_URI"]
    flow = os.environ["AUTH_FLOW"]
    descriptor = flight.FlightDescriptor.for_command(
        json.dumps(
            {
                "catalog": os.environ.get("READINESS_CATALOG", flight_client.DEFAULT_CATALOG),
                "target": os.environ.get("READINESS_TARGET", flight_client.DEFAULT_TARGET),
                "columns": _columns(),
            }
        )
    )
    deadline = time.monotonic() + 90
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            client = flight_client.create_client(uri, flow)
            client.get_schema(descriptor, options=_options(flow))
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


def _options(flow: str) -> flight.FlightCallOptions:
    if flow in {"mtls", "mtls-spiffe"}:
        return flight.FlightCallOptions()
    return flight.FlightCallOptions(headers=[])


def _columns() -> list[str]:
    raw = os.environ.get("READINESS_COLUMNS", "id")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _is_auth_failure(exc: Exception) -> bool:
    message = str(exc).lower()
    return "unauthorized" in message or "unauthenticated" in message


if __name__ == "__main__":
    main()
