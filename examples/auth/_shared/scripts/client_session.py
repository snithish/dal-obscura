from __future__ import annotations

import os
import time
from pathlib import Path

import flight_client
import wait_for_flight


def main() -> None:
    wait_for_flight.main()
    ready_file = Path(os.environ.get("CLIENT_READY_FILE", "/workspace/runtime/client.ready"))
    ready_file.unlink(missing_ok=True)
    flow = os.environ["AUTH_FLOW"]
    uri = os.environ["FLIGHT_URI"]
    credentials = _startup_credentials(flow)
    for credential in credentials:
        flight_client.perform_read(
            uri=uri,
            auth_flow=flow,
            catalog=os.environ.get("STARTUP_CATALOG", flight_client.DEFAULT_CATALOG),
            target=os.environ.get("STARTUP_TARGET", flight_client.DEFAULT_TARGET),
            columns=_split_csv(
                os.environ.get("STARTUP_COLUMNS", ",".join(flight_client.DEFAULT_COLUMNS))
            ),
            credential=credential,
            row_filter=os.environ.get("STARTUP_ROW_FILTER"),
            expect_rows=_optional_int(os.environ.get("STARTUP_EXPECT_ROWS")),
        )
    ready_file.parent.mkdir(parents=True, exist_ok=True)
    ready_file.touch()
    while True:
        time.sleep(3600)


def _startup_credentials(flow: str) -> list[str | None]:
    raw = os.environ.get("STARTUP_CREDENTIALS", "").strip()
    if raw:
        return [value.strip() or None for value in raw.split(",")]
    if flow == "composite-provider":
        return ["api-key", "jwt"]
    return [None]


def _optional_int(value: str | None) -> int | None:
    if value is None or not value.strip():
        return None
    return int(value)


def _split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


if __name__ == "__main__":
    main()
